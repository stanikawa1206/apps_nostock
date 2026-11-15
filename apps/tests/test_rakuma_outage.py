# -*- coding: utf-8 -*-
"""
ラクマの障害情報の共有（apps/adapters/rakuma_outage.py、mst.control を使う）のテスト。
  - 5xx は全台、429 はその台だけを止める。止めている間は HTTP を送らない。期限後の成功で終了。
  - 複数の台が同時に書いても停止期限は短くならない。期限内の停止は（古い）成功では解除されない。
  - 障害状態の DB の失敗は RakumaStateError（一時的なエラー）で、商品の解析失敗にはしない。
  - 在庫確認（check_remaining）は判定不可、出品（publish_ebay）は候補を手放す／障害中はラクマを候補から外す。

DB を使うテストは、同じ SQL Server に専用DB（test_account_listing_block と同じ nostock_test_blocked）を作り、
本番と同じ定義の mst.control を作って使い、最後に削除する（本番DBには触れない）。ラクマ・eBay には接続しない。

実行: python -m unittest apps.tests.test_rakuma_outage -v
"""

import contextlib
import os
import re
import sys
import threading
import unittest
from unittest import mock

from apps.adapters import rakuma_item_status as ris
from apps.adapters import rakuma_outage as ro
from apps.adapters import rakuma_search as rs
from apps.inventory import inventory_loop as il
from apps.publish import publish_ebay as pe
from apps.tests.test_account_listing_block import DbTestBase

# check_remaining_ebay は import 時に stdout をログへ差し替えるので、元に戻す（test_yahoo_fleamarket と同じ）
_o, _e = sys.stdout, sys.stderr
from apps.inventory import check_remaining_ebay as cr  # noqa: E402
sys.stdout, sys.stderr = _o, _e

# 本番の mst.control と同じ定義（2026-11-15 確認: param_key NVARCHAR(200) 主キー、param_value INT NULL、トリガーなし）
CONTROL_DDL = ("IF OBJECT_ID('mst.control') IS NULL CREATE TABLE mst.control ("
               "param_key NVARCHAR(200) NOT NULL CONSTRAINT PK_control_parameter PRIMARY KEY, param_value INT NULL)")


def http_error(status):
    return ro.RakumaHttpError(url="https://item.fril.jp/x", status_code=status, retry_after=None,
                              server="awselb/2.0", content_type="text/html", body_head="<!DOCTYPE html>")


class FakeResponse:
    def __init__(self, status_code, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


NOT_PAUSED = {"paused_until": None, "kind": None, "open": 0}


# ============================================================
# DB を使わない部分
# ============================================================
class OutageNoDbTest(unittest.TestCase):
    def setUp(self):
        ro._forget_state()
        self.addCleanup(ro._forget_state)
        p = mock.patch("builtins.print")
        p.start()
        self.addCleanup(p.stop)

    def test_page_title(self):
        self.assertEqual(ro.page_title("<html><head><title>\n ただいま\n メンテナンス中 </title>"), "ただいま メンテナンス中")
        self.assertIsNone(ro.page_title("<html><head></head>"))

    def test_state_is_cached(self):
        with mock.patch.object(ro, "_run", return_value=(None, None, 0)) as run:
            for _ in range(5):
                ro.raise_if_paused("https://item.fril.jp/x")
        self.assertEqual(run.call_count, 1)  # RAKUMA_STATE_CACHE_SEC の間は DB を読み直さない

    def test_paused_sends_no_request(self):
        state = {"paused_until": "2026-11-15 03:27:00", "kind": "OUTAGE", "open": 1}
        with mock.patch.object(ro, "_current_state", return_value=state), \
                mock.patch.object(ris.requests, "get") as get_item, \
                mock.patch.object(rs.requests, "get") as get_search:
            with self.assertRaises(ro.RakumaPausedError) as c1:
                ris.fetch_rakuma_item_html("https://item.fril.jp/x")
            with self.assertRaises(rs.RakumaHttpError) as c2:  # scrape_worker の既存の except で受けられる
                rs.fetch_rakuma_search_html("https://fril.jp/s?x=1")
        get_item.assert_not_called()
        get_search.assert_not_called()
        self.assertTrue(c1.exception.transient)
        self.assertTrue(c2.exception.transient)

    def test_record_by_status(self):
        def recorded(status, headers=None):
            with mock.patch.object(ro, "_current_state", return_value=dict(NOT_PAUSED)), \
                    mock.patch.object(ro, "_run", return_value=(None, 100, 50)) as run:
                try:
                    ro.check_response(FakeResponse(status, "<title>t</title>", headers), "https://item.fril.jp/x")
                except ro.RakumaHttpError as e:
                    self.assertEqual(e.title, "t")
                    self.assertIn("title='t'", str(e))
            return run.call_args_list

        for st in (500, 502, 503, 504):
            (call,) = recorded(st)
            self.assertEqual(call.args[1], (ro.OUTAGE_PAUSE_SEC, ro.KEY_OUTAGE_UNTIL, ro.KEY_OUTAGE_START))  # 全台
        with mock.patch.dict(os.environ, {"WORKER_NAME": "x162-43-15-160"}):
            (call,) = recorded(429, {"Retry-After": "120"})
            self.assertEqual(call.args[1], (120, "ラクマ制限_停止期限_x162-43-15-160", None))  # この台だけ
            (call,) = recorded(429)
            self.assertEqual(call.args[1][0], ro.RATE_LIMIT_PAUSE_SEC)  # Retry-After なし
            (call,) = recorded(429, {"Retry-After": "999999"})
            self.assertEqual(call.args[1][0], ro.RATE_LIMIT_PAUSE_MAX_SEC)
        for st in (200, 404, 400, 403, 410):
            self.assertEqual(recorded(st), [], st)  # 障害なしなら成功時も DB を書かない・403 等は障害扱いしない

    def test_db_error_is_state_error_not_http_failure(self):
        # 障害状態の DB の失敗（接続エラー等）は RakumaStateError。一時的なエラーで、商品の解析失敗とは区別する
        with mock.patch.object(ro, "get_sql_server_connection", side_effect=ConnectionError("SQL Server に接続できません")), \
                mock.patch.object(ris.requests, "get") as get:
            with self.assertRaises(ro.RakumaStateError) as cm:
                ris.fetch_rakuma_item_html("https://item.fril.jp/x")
            get.assert_not_called()
        self.assertTrue(cm.exception.transient)
        self.assertIsInstance(cm.exception, ro.RakumaHttpError)  # scrape_worker・check_remaining・publish の既存の扱いに乗る
        self.assertIn("RAKUMA_STATE_DB_ERROR", str(cm.exception))
        # 503 を受けたが記録に失敗した場合も、RakumaStateError（一時的なエラー）
        with mock.patch.object(ro, "_current_state", return_value=dict(NOT_PAUSED)), \
                mock.patch.object(ro, "get_sql_server_connection", side_effect=ConnectionError("x")):
            with self.assertRaises(ro.RakumaStateError):
                ro.check_response(FakeResponse(503, "<html></html>"), "https://item.fril.jp/x")


# ============================================================
# テスト専用DB で SQL を実行
# ============================================================
class OutageDbTest(DbTestBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with contextlib.closing(pe.get_sql_server_connection()) as c:
            cur = c.cursor()
            cur.execute("SELECT DB_NAME()")
            assert cur.fetchone()[0] == "nostock_test_blocked"
            cur.execute(CONTROL_DDL)
            c.commit()

    def setUp(self):
        super().setUp()
        self.sql("DELETE FROM mst.control")
        # 既存のキー（触ってはいけない）
        self.sql("INSERT INTO mst.control VALUES (N'出品枠リセット月', 202611), (N'日次処理停止', NULL)")
        self.conn.commit()
        ro._forget_state()
        self.addCleanup(ro._forget_state)
        p = mock.patch("builtins.print")
        p.start()
        self.addCleanup(p.stop)

    def tearDown(self):
        rows = self.values()
        self.assertEqual((rows.get("出品枠リセット月"), rows.get("日次処理停止", "missing")), (202611, None))

    # ---------- 道具 ----------
    def as_host(self, host):
        ro._forget_state()  # 別の台＝別のプロセス（覚えを持たない）
        return mock.patch.dict(os.environ, {"WORKER_NAME": host})

    def paused(self, host):
        with self.as_host(host):
            try:
                ro.raise_if_paused("https://item.fril.jp/x")
                return False
            except ro.RakumaPausedError:
                return True

    def values(self):
        r = self.sql("SELECT param_key, param_value FROM mst.control").fetchall()
        self.conn.commit()
        return {k: v for k, v in r}

    def left(self, key):
        r = self.sql(f"SELECT param_value - {ro.NOW_SEC_SQL} FROM mst.control WHERE param_key = ?", key).fetchone()
        self.conn.commit()
        return None if r is None or r[0] is None else r[0]

    def set_left(self, key, seconds):
        self.sql(f"UPDATE mst.control SET param_value = {ro.NOW_SEC_SQL} + ? WHERE param_key = ?", seconds, key)
        self.conn.commit()

    def record(self, host, status, retry_after=None):
        with self.as_host(host):
            ro.record_response(status, "https://item.fril.jp/x", retry_after=retry_after)

    # ---------- 5xx ----------
    def test_503_pauses_all_hosts_and_recovers(self):
        self.record("x162-43-15-160", 503)
        start = self.values()[ro.KEY_OUTAGE_START]
        self.record("mouse", 502)  # 2台目（継続中）では開始時刻を変えない
        self.assertEqual(self.values()[ro.KEY_OUTAGE_START], start)
        self.assertTrue(ro.OUTAGE_PAUSE_SEC - 5 <= self.left(ro.KEY_OUTAGE_UNTIL) <= ro.OUTAGE_PAUSE_SEC + 1)
        for host in ("x210-131-209-103", "mouse", "HRSP-server"):
            self.assertTrue(self.paused(host), host)  # 503 を受けていない台も止まる

        # 期限が過ぎたら止めない → 成功で 5xx の2つのキーを NULL に戻す
        self.set_left(ro.KEY_OUTAGE_UNTIL, -1)
        self.assertFalse(self.paused("x162-43-29-154"))
        self.record("x162-43-29-154", 200)
        v = self.values()
        self.assertEqual((v[ro.KEY_OUTAGE_UNTIL], v[ro.KEY_OUTAGE_START]), (None, None))

        # その後の 503 は新しい障害（開始時刻を入れ直す）
        self.record("mouse", 504)
        self.assertIsNotNone(self.values()[ro.KEY_OUTAGE_START])

    def test_extend_never_shortens(self):
        # 1. 停止期限が短く上書きされない
        self.record("a", 503)
        self.set_left(ro.KEY_OUTAGE_UNTIL, 3000)  # 先に誰かが長い期限を書いた
        self.record("b", 503)  # 600秒後の期限では短くしない
        self.assertGreater(self.left(ro.KEY_OUTAGE_UNTIL), 2900)
        # 429 の Retry-After が短くなっても、その台の期限は短くならない
        self.record("h1", 429, retry_after="1200")
        self.record("h1", 429, retry_after="60")
        self.assertGreater(self.left(ro.rate_limit_key("h1")), 1100)

    def test_concurrent_updates_keep_the_latest_deadline(self):
        # 1. 複数のワーカーが同時に書いても、いちばん遅い期限が残る（キーが無い状態から始めて行も重複しない）
        errors = []
        barrier = threading.Barrier(8)

        def worker(i):
            try:
                with mock.patch.dict(os.environ, {"WORKER_NAME": "hX"}):
                    conn_ro = ro
                    barrier.wait()
                    conn_ro.record_response(429, "u", retry_after=str(60 if i % 2 else 1500))
            except Exception as e:  # noqa: BLE001  テストで失敗を集める
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        self.assertEqual(errors, [])
        n = self.sql("SELECT COUNT(*) FROM mst.control WHERE param_key = ?", ro.rate_limit_key("hX")).fetchone()[0]
        self.conn.commit()
        self.assertEqual(n, 1)
        self.assertGreater(self.left(ro.rate_limit_key("hX")), 1400)

        barrier2 = threading.Barrier(6)

        def outage_worker():
            try:
                barrier2.wait()
                ro.record_response(503, "u")
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=outage_worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        self.assertEqual(errors, [])
        self.assertTrue(ro.OUTAGE_PAUSE_SEC - 10 <= self.left(ro.KEY_OUTAGE_UNTIL) <= ro.OUTAGE_PAUSE_SEC + 1)

    def test_stale_success_does_not_clear_new_outage(self):
        # 2. 期限切れの後に新しい 503 で期限が延びたら、それより前に送った要求の成功が届いても解除しない
        self.record("a", 503)
        self.set_left(ro.KEY_OUTAGE_UNTIL, -1)  # 期限切れ
        with self.as_host("b"):
            ro._current_state()  # b は「期限切れの停止が残っている」状態を覚えている（古い成功を記録しようとしている）
            self.record_keep_state = None
        self.record("a", 503)  # 新しい 503（期限が未来へ）
        with mock.patch.dict(os.environ, {"WORKER_NAME": "b"}):
            ro.record_response(200, "u")  # b の古い要求の成功
        self.assertGreater(self.left(ro.KEY_OUTAGE_UNTIL), ro.OUTAGE_PAUSE_SEC - 10)
        self.assertIsNotNone(self.values()[ro.KEY_OUTAGE_START])
        self.assertTrue(self.paused("c"))

    def test_429_pauses_only_that_host(self):
        # 4. 429 はその台だけ
        self.record("x162-43-15-160", 429, retry_after="120")
        self.assertTrue(110 <= self.left(ro.rate_limit_key("x162-43-15-160")) <= 121)
        self.assertTrue(self.paused("x162-43-15-160"))
        self.assertFalse(self.paused("mouse"))
        self.assertIsNone(self.values().get(ro.KEY_OUTAGE_UNTIL))

        # 他の台の成功では解除されない。期限後のその台の成功で解除
        self.set_left(ro.rate_limit_key("x162-43-15-160"), -1)
        self.record("mouse", 200)
        self.assertIsNotNone(self.values()[ro.rate_limit_key("x162-43-15-160")])
        self.record("x162-43-15-160", 200)
        self.assertIsNone(self.values()[ro.rate_limit_key("x162-43-15-160")])

    def test_take_one_drops_rakuma_only_while_paused(self):
        # 5. 障害中もラクマ以外の候補は残る（TAKE_ONE_SQL の ①' と同じ SQL を実行）
        block = re.search(r"DECLARE @rakuma_paused INT = 0;.*?\nEND\n", pe.TAKE_ONE_SQL, re.S).group(0)
        sql = (
            "SET NOCOUNT ON;\n"
            "DECLARE @all TABLE (vendor_name NVARCHAR(100), vendor_item_id NVARCHAR(200));\n"
            "INSERT INTO @all VALUES (N'ラクマ', N'r1'), (N'ラクマ', N'r2'), (N'メルカリshops', N'm1'), (N'メルカリ', N'x1');\n"
            + block +
            "SELECT @rakuma_paused, (SELECT COUNT(*) FROM @all WHERE vendor_name = N'ラクマ'), "
            "(SELECT COUNT(*) FROM @all WHERE vendor_name <> N'ラクマ');"
        )
        self.assertEqual(sql.count("?"), 1)

        def run(host):
            r = tuple(self.sql(sql, host).fetchone())
            self.conn.commit()
            return r

        self.assertEqual(run("mouse"), (0, 2, 2))  # 障害なし
        self.record("x162-43-15-160", 429)
        self.assertEqual(run("mouse"), (0, 2, 2))  # 他の台の 429
        self.assertEqual(run("x162-43-15-160"), (2, 0, 2))  # 自分の 429
        self.record("a", 503)
        self.assertEqual(run("mouse"), (2, 0, 2))  # 503 は全台
        self.set_left(ro.KEY_OUTAGE_UNTIL, -1)
        self.set_left(ro.rate_limit_key("x162-43-15-160"), -1)
        self.assertEqual(run("mouse"), (0, 2, 2))  # 期限後は戻る


# ============================================================
# 在庫確認・在庫ループの表示
# ============================================================
class CheckRemainingRakumaTest(unittest.TestCase):
    def status(self, exc):
        with mock.patch.object(cr, "check_rakuma_item_status", side_effect=exc), mock.patch("builtins.print"):
            return cr.get_status(None, None, "https://item.fril.jp/x")

    def test_transient_is_undetermined(self):
        self.assertEqual(self.status(http_error(503)), ("判定不可", None))
        self.assertEqual(self.status(http_error(429)), ("判定不可", None))
        self.assertEqual(self.status(ro.RakumaPausedError("u", "OUTAGE", "t")), ("判定不可", None))
        self.assertEqual(self.status(ro.RakumaStateError("u", ConnectionError("x"))), ("判定不可", None))

    def test_other_errors_unchanged(self):
        with self.assertRaises(ro.RakumaHttpError):
            self.status(http_error(403))
        self.assertEqual(self.status(ris.RakumaItemUnavailableError("x", state="削除")), ("削除", None))

    def test_error_reason_groups_paused(self):
        msg = ("[再試行を使い切った retry=6/6] RAKUMA_PAUSED kind=OUTAGE "
               "paused_until=2026-11-15 03:27:01 url=https://fril.jp/s?x=1")
        self.assertEqual(il.error_reason(msg), "[再試行を使い切った retry=6/6] ラクマ障害中 OUTAGE")


# ============================================================
# publish_ebay.heavy_check_detail
# ============================================================
def call_heavy(vendor_name, conn=None):
    conn = conn or mock.MagicMock()
    return pe.heavy_check_detail(
        conn, mock.MagicMock(), "https://example/x", "SKU1", "preset", vendor_name, "mode",
        "brand", "1", "d", "t", "財布", {}, 0, 1, 2, mock.MagicMock(), 1,
    ), conn


class HeavyCheckTest(unittest.TestCase):
    def setUp(self):
        self.upsert = mock.patch.object(pe, "upsert_vendor_item").start()
        self.release = mock.patch.object(pe, "release_vendor_item").start()
        mock.patch.object(pe, "playwright_driver_is_dead", return_value=False).start()
        mock.patch("builtins.print").start()
        self.addCleanup(mock.patch.stopall)

    def test_rakuma_transient_releases_without_ng(self):
        for exc in (http_error(503), http_error(429), ro.RakumaPausedError("u", "OUTAGE", "t"),
                    ro.RakumaStateError("u", ConnectionError("SQL Server に接続できません"))):
            with self.subTest(type(exc).__name__):
                self.release.reset_mock()
                self.upsert.reset_mock()
                with mock.patch.object(pe, "parse_detail_rakuma", side_effect=exc):
                    res = call_heavy("ラクマ")[0][0]
                self.assertIsNone(res)
                self.release.assert_called_once()
                self.assertEqual(self.release.call_args.args[1:], ("ラクマ", "SKU1"))
                self.upsert.assert_not_called()  # 解析失敗を書かない

    def test_rakuma_403_is_parse_failure_as_before(self):
        with mock.patch.object(pe, "parse_detail_rakuma", side_effect=http_error(403)):
            call_heavy("ラクマ")
        self.release.assert_not_called()
        self.assertEqual(self.upsert.call_args.args[1]["listing_head"], "解析失敗")


# ============================================================
# publish_ebay.take_one_vendor_item（候補がラクマだけで障害中）
# ============================================================
class TakeOneRakumaPausedTest(unittest.TestCase):
    def test_waits_instead_of_empty(self):
        cols = [("cand_count",), ("rakuma_paused_count",), ("vendor_item_id",)]
        results = iter([(0, 5, None), (0, 0, None)])  # 1回目: ラクマだけ・障害中 → 2回目: 本当に0件

        cur = mock.MagicMock()
        cur.__enter__.return_value = cur
        cur.fetchone.side_effect = lambda: next(results)
        cur.description = cols
        conn = mock.MagicMock()
        conn.cursor.return_value = cur
        with mock.patch.object(pe.time, "sleep") as sleep, mock.patch("builtins.print"):
            self.assertIsNone(pe.take_one_vendor_item(conn, "貴文③", "mouse", "貴文③"))
        sleep.assert_called_once_with(pe.RAKUMA_PAUSED_WAIT_SEC)
        self.assertEqual(cur.execute.call_args.args[1], ("貴文③", "mouse", pe.TAKE_ONE_CANDIDATE_LIMIT, "mouse"))
        self.assertEqual(pe.TAKE_ONE_SQL.count("?"), 4)
        self.assertNotIn("{PAUSED_EXISTS}", pe.TAKE_ONE_SQL)
        self.assertIn("mst.control", pe.TAKE_ONE_SQL)
        self.assertNotIn("rakuma_outage", pe.TAKE_ONE_SQL.replace("rakuma_outage.py", ""))


if __name__ == "__main__":
    unittest.main()
