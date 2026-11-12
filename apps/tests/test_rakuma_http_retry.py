# -*- coding: utf-8 -*-
"""
ラクマ検索のHTTPエラー対策のテスト（モックのみ。DB・ラクマ・eBay には接続しない）。
  - rakuma_search: HTTPエラー時のレスポンス情報の記録
  - scrape_worker: 503などの再試行待ち（retry_wait）・回数上限・売り切れ取得の失敗の扱い

実行: python -m unittest apps.tests.test_rakuma_http_retry -v
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

from apps.adapters import rakuma_search as rs

# scrape_worker は import 時に stdout を自分のログへ差し替え、カレントの logs/ にログを作るため、
# 一時フォルダに向けて import し、stdout / stderr を元に戻す。
_TMP = tempfile.TemporaryDirectory()
_stdout, _stderr = sys.stdout, sys.stderr
with mock.patch.dict(os.environ, {"WORKER_STATUS_DIR": _TMP.name, "WORKER_NAME": "test-worker"}):
    from apps.inventory import scrape_worker as sw
sys.stdout, sys.stderr = _stdout, _stderr
sw._log_file.close()


class FakeResponse:
    def __init__(self, status_code, text="", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


def http_error(status=503):
    return rs.RakumaHttpError(
        url="https://fril.jp/s?x=1", status_code=status, retry_after="120",
        server="nginx", content_type="text/html", body_head="<html> maintenance",
    )


# ============================================================
# rakuma_search
# ============================================================
class FetchRakumaSearchHtmlTest(unittest.TestCase):
    def fetch(self, resp):
        out = io.StringIO()
        with mock.patch.object(rs.requests, "get", return_value=resp) as get, contextlib.redirect_stdout(out):
            try:
                return rs.fetch_rakuma_search_html("https://fril.jp/s?x=1"), out.getvalue(), get
            except rs.RakumaHttpError as e:
                return e, out.getvalue(), get

    def test_ok_and_404(self):
        self.assertEqual(self.fetch(FakeResponse(200, "<html>ok</html>"))[0], "<html>ok</html>")
        # 404 = ページ範囲外（正常な「これ以上なし」）
        self.assertIsNone(self.fetch(FakeResponse(404, "nf"))[0])

    def test_503_logs_response_details(self):
        body = "<html>\n  <head><title>503 Service Temporarily Unavailable</title></head>" + "x" * 1000
        resp = FakeResponse(503, body, {
            "Retry-After": "3600", "Server": "nginx", "Content-Type": "text/html",
            "Set-Cookie": "session=SECRET",
        })
        err, out, get = self.fetch(resp)
        self.assertIsInstance(err, rs.RakumaHttpError)
        self.assertTrue(err.transient)
        self.assertEqual((err.status_code, err.retry_after, err.server), (503, "3600", "nginx"))
        for s in ("[RAKUMA HTTP ERROR]", "status=503", "retry_after=3600", "server=nginx",
                  "content_type=text/html", "url=https://fril.jp/s?x=1",
                  "<html> <head><title>503 Service Temporarily Unavailable</title></head>"):
            self.assertIn(s, out)
        # 本文は先頭だけ（改行・連続空白は1つにまとめる）、Cookie は記録しない
        self.assertLessEqual(len(err.body_head), rs.ERROR_BODY_HEAD_CHARS)
        self.assertNotIn("\n", err.body_head)
        self.assertNotIn("SECRET", out)
        self.assertNotIn("SECRET", str(err))
        # 送るヘッダーは User-Agent だけ（変更なし）
        self.assertEqual(get.call_args.kwargs["headers"], {"User-Agent": rs.RAKUMA_USER_AGENT})

    def test_transient_statuses(self):
        for st in (429, 500, 502, 503, 504):
            self.assertTrue(self.fetch(FakeResponse(st))[0].transient, st)
        for st in (400, 403, 410):
            err = self.fetch(FakeResponse(st))[0]
            self.assertIsInstance(err, rs.RakumaHttpError)
            self.assertFalse(err.transient, st)


# ============================================================
# scrape_worker: 再試行待ち
# ============================================================
class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 1

    def execute(self, sql, *params):
        self.conn.executed.append((sql, params))
        self.rowcount = self.conn.rowcount

    def close(self):
        pass


class FakeWorkerConn:
    def __init__(self, rowcount=1):
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.rowcount = rowcount

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class HandleRakumaHttpErrorTest(unittest.TestCase):
    def setUp(self):
        self.status_file = os.path.join(_TMP.name, "status_test.txt")
        p = mock.patch.object(sw, "STATUS_FILE", self.status_file)
        p.start()
        self.addCleanup(p.stop)
        self.now = datetime(2026, 10, 28, 1, 6, 15)
        p = mock.patch.object(sw, "now_jst", return_value=self.now)
        p.start()
        self.addCleanup(p.stop)

    def handle(self, payload, err, rowcount=1):
        open(self.status_file, "w").close()
        conn = FakeWorkerConn(rowcount)
        with contextlib.redirect_stdout(io.StringIO()):
            sw.handle_rakuma_http_error(conn, 101, payload, err)
        self.assertEqual(len(conn.executed), 1)
        self.assertEqual(conn.commits, 1)
        self.assertFalse(os.path.exists(self.status_file))
        return conn.executed[0]

    def test_first_503_goes_to_retry_wait(self):
        sql, params = self.handle({"preset": "P", "vendor_name": "ラクマ"}, http_error(503))
        self.assertIs(sql, sw.SQL_MARK_RETRY_WAIT)
        retry_at, payload_json, msg, job_id, worker = params
        # 10分 + 0〜2分のばらつき
        self.assertGreaterEqual(retry_at, self.now + timedelta(seconds=600))
        self.assertLessEqual(retry_at, self.now + timedelta(seconds=720))
        self.assertEqual(json.loads(payload_json), {"preset": "P", "vendor_name": "ラクマ", "http_retry_count": 1})
        self.assertIn("[RETRY_WAIT 1/6]", msg)
        self.assertIn("status=503", msg)
        self.assertEqual((job_id, worker), (101, "test-worker"))
        # 自分が running で持っているジョブだけを戻す（二重投入の防止）
        self.assertIn("AND status = 'running'", sql)
        self.assertIn("AND worker_name = ?", sql)

    def test_retry_times_are_spread(self):
        times = set()
        for _ in range(20):
            times.add(self.handle({"vendor_name": "ラクマ"}, http_error(503))[1][0])
        self.assertGreater(len(times), 1)

    def test_error_after_six_retries(self):
        sql, params = self.handle({"vendor_name": "ラクマ", "http_retry_count": 6}, http_error(503))
        self.assertIs(sql, sw.SQL_MARK_ERROR)
        finished_at, msg, job_id = params
        self.assertEqual((finished_at, job_id), (self.now, 101))
        self.assertTrue(msg.startswith("[再試行を使い切った retry=6/6] RAKUMA_HTTP status=503"))

    def test_non_transient_is_error_immediately(self):
        sql, params = self.handle({"vendor_name": "ラクマ"}, http_error(403))
        self.assertIs(sql, sw.SQL_MARK_ERROR)
        self.assertTrue(params[1].startswith("[再試行対象外 retry=0/6] RAKUMA_HTTP status=403"))

    def test_seven_attempts_then_error(self):
        """初回 + 再試行6回 = 7回失敗したら error。回数は job_payload に保存されて引き継がれる。"""
        payload_json = json.dumps({"vendor_name": "ラクマ"})
        kinds = []
        for _ in range(10):
            sql, params = self.handle(json.loads(payload_json), http_error(503))
            kinds.append("retry" if sql is sw.SQL_MARK_RETRY_WAIT else "error")
            if sql is sw.SQL_MARK_RETRY_WAIT:
                payload_json = params[1]
            else:
                break
        self.assertEqual(kinds, ["retry"] * 6 + ["error"])


class PickSqlTest(unittest.TestCase):
    def test_retry_wait_is_picked_only_after_retry_time(self):
        self.assertIn("status IN ('pending','pending2')", sw.SQL_PICK_JOBS)
        self.assertIn("OR (status = 'retry_wait' AND locked_at <= ?)", sw.SQL_PICK_JOBS)
        self.assertIn("UPDLOCK, READPAST", sw.SQL_PICK_JOBS)
        self.assertEqual(sw.SQL_PICK_JOBS.count("?"), 3)


# ============================================================
# scrape_worker: ラクマの売り切れ取得
# ============================================================
class SoldRakumaTest(unittest.TestCase):
    PAYLOAD = {
        "preset": "P", "vendor_name": "ラクマ", "brand_id": 1, "category_id": 2,
        "mode": "DDP", "low_usd_target": 50, "high_usd_target": 500,
    }

    def setUp(self):
        self.deleted = []
        self.upserts = []
        patches = {
            "get_sql_server_connection": mock.Mock(return_value=mock.Mock()),
            "make_search_url": mock.Mock(return_value="https://fril.jp/s?q=1"),
            "write_status": mock.Mock(),
            "get_vendor_item_dates_batch": mock.Mock(return_value={}),
            "handle_listing_delete": lambda conn, iid, vendor, reason: self.deleted.append(iid),
            "upsert_vendor_items": lambda conn, rows, now: self.upserts.append(len(rows)),
        }
        for name, value in patches.items():
            p = mock.patch.object(sw, name, value)
            p.start()
            self.addCleanup(p.stop)
        p = mock.patch.object(sw.time, "sleep")
        p.start()
        self.addCleanup(p.stop)

    def run_sold(self, pages):
        """pages: ページごとの fetch の結果（str=HTML / None=404 / 例外）"""
        fetch = mock.Mock(side_effect=pages)
        extract = lambda html: [{"vendor_item_id": x, "title_jp": x} for x in html.split(",") if x]
        with mock.patch.object(sw, "fetch_rakuma_search_html", fetch), \
                mock.patch.object(sw, "extract_items_from_rakuma_html", side_effect=extract), \
                contextlib.redirect_stdout(io.StringIO()):
            return sw.run_fetch_sold_rakuma(dict(self.PAYLOAD), 1)

    def test_fetch_failure_is_not_swallowed(self):
        # 以前は握りつぶして done(pages=0) になっていた
        with self.assertRaises(rs.RakumaHttpError):
            self.run_sold([http_error(503)])
        self.assertEqual(self.deleted, [])

    def test_failure_on_second_page_is_not_swallowed(self):
        with self.assertRaises(rs.RakumaHttpError):
            self.run_sold(["a,b", http_error(503)])
        self.assertEqual(self.deleted, ["a", "b"])

    def test_connection_error_is_not_swallowed(self):
        with self.assertRaises(sw_requests_error()):
            self.run_sold([sw_requests_error()("timeout")])

    def test_normal_zero_items(self):
        # 404（ページ範囲外）・0件のページは正常な0件
        self.assertEqual(self.run_sold([None]), (1, 0))
        self.assertEqual(self.run_sold([""]), (1, 0))

    def test_normal_pages(self):
        self.assertEqual(self.run_sold(["a,b", "c"]), (2, 3))
        self.assertEqual(self.deleted, ["a", "b", "c"])

    def test_db_error_on_page_is_still_logged_and_continues(self):
        # 取得後の DB 処理の失敗は従来どおり（ページ単位で記録して続行）
        with mock.patch.object(sw, "upsert_vendor_items", side_effect=RuntimeError("db")):
            self.assertEqual(self.run_sold(["a", "b"]), (2, 2))


def sw_requests_error():
    import requests
    return requests.exceptions.ConnectionError


# ============================================================
# scrape_worker: main のジョブ振り分け
# ============================================================
class WorkerMainRoutingTest(unittest.TestCase):
    """main() を1ジョブだけ動かし（ONESHOT=1）、例外ごとのジョブの終わり方を確認する。"""

    def run_one_job(self, payload, run_side_effect):
        conn = FakeWorkerConn()
        picked = [(201, "fetch_active_ebay", json.dumps(payload, ensure_ascii=False), None)]

        class PickCursor(FakeCursor):
            def fetchall(self_inner):
                return picked

        conn.cursor = lambda: PickCursor(conn)
        status_file = os.path.join(_TMP.name, "status_main.txt")
        with mock.patch.object(sw, "sync_playwright", mock.MagicMock()), \
                mock.patch.object(sw, "get_sql_server_connection", return_value=conn), \
                mock.patch.object(sw, "run_fetch_active_rakuma", side_effect=run_side_effect), \
                mock.patch.object(sw, "run_fetch_active_ebay", side_effect=run_side_effect), \
                mock.patch.object(sw, "STATUS_FILE", status_file), \
                mock.patch.object(sw, "CHECK_SWAP", False), \
                mock.patch.dict(os.environ, {"ONESHOT": "1"}), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            sw.main()
        return [sql for sql, _ in conn.executed[1:]]  # 先頭は取り出し(PICK)

    def test_rakuma_503_goes_to_retry_wait(self):
        sqls = self.run_one_job({"vendor_name": "ラクマ", "preset": "P"}, http_error(503))
        self.assertEqual(sqls, [sw.SQL_MARK_RETRY_WAIT])

    def test_rakuma_403_is_error(self):
        sqls = self.run_one_job({"vendor_name": "ラクマ", "preset": "P"}, http_error(403))
        self.assertEqual(sqls, [sw.SQL_MARK_ERROR])

    def test_other_exceptions_unchanged(self):
        sqls = self.run_one_job({"vendor_name": "メルカリ", "preset": "P"}, ValueError("boom"))
        self.assertEqual(sqls, [sw.SQL_MARK_ERROR])

    def test_success_unchanged(self):
        sqls = self.run_one_job({"vendor_name": "ラクマ", "preset": "P"}, [(2, 10)])
        self.assertEqual(sqls, [sw.SQL_MARK_DONE])


if __name__ == "__main__":
    unittest.main()
