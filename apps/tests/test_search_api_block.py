# -*- coding: utf-8 -*-
"""
メルカリ検索APIの 401・403・429 による停止の共有と自動復旧（apps/adapters/search_api_block.py）のテスト（2026-11-22）。

DB を使うテストは、同じ SQL Server に専用DB（nostock_test_blocked。test_account_listing_block と同じ）を作り、
本番と同じ定義の mst.control を作って使い、最後に削除する（本番DBには触れない）。
メルカリには接続しない（確認アクセスはモック）。

実行: python -m unittest apps.tests.test_search_api_block -v
"""

import contextlib
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from unittest import mock

from apps.adapters import search_api_block as sab
from apps.inventory import inventory_loop as il
from apps.tests.test_account_listing_block import DbTestBase
from apps.tests.test_rakuma_outage import CONTROL_DDL

HOSTS = ["mouse", "x162-43-15-160", "x162-43-29-154", "x162-43-39-209", "x162-43-42-135",
         "x210-131-209-103", "x210-131-209-232", "x85-131-251-127"]


class SearchApiBlockDbTest(DbTestBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from apps.common.utils import get_sql_server_connection
        with contextlib.closing(get_sql_server_connection()) as c:
            cur = c.cursor()
            cur.execute("SELECT DB_NAME()")
            assert cur.fetchone()[0] != "nostock"
            cur.execute(CONTROL_DDL)
            c.commit()

    def setUp(self):
        super().setUp()
        self.sql("DELETE FROM mst.control")
        self.conn.commit()
        p = mock.patch("builtins.print")
        p.start()
        self.addCleanup(p.stop)
        self.tmp = tempfile.mkdtemp()

    # ---- 補助 ----
    def flag(self, host, age_sec=0):
        """その台の search_api_blocked.txt（age_sec 秒前に書いたことにする）"""
        path = os.path.join(self.tmp, f"{host}.txt")
        with open(path, "a", encoding="utf-8") as f:
            f.write("status=403\n")
        t = time.time() - age_sec
        os.utime(path, (t, t))
        return path

    def state(self):
        return sab.read_state()

    def make_due(self):
        """次回確認の時刻を過ぎたことにする"""
        self.sql(f"UPDATE mst.control SET param_value = {sab.NOW_SEC_SQL} - 1 "
                 "WHERE param_key = ? AND param_value IS NOT NULL", sab.KEY_NEXT)
        self.conn.commit()

    # ---- 停止の記録 ----
    def test_register_new_then_joined(self):
        self.assertEqual(sab.register_block(403), "new")
        st = self.state()
        self.assertEqual(st[sab.KEY_HTTP], 403)
        self.assertEqual(st[sab.KEY_ATTEMPTS], 0)
        self.assertEqual(st[sab.KEY_NEXT] - st[sab.KEY_START], 900)  # 15分後に1回目
        start = st[sab.KEY_START]
        time.sleep(1.1)
        self.assertEqual(sab.register_block(429), "joined")  # 他の台が同じ停止に加わる
        st = self.state()
        self.assertEqual((st[sab.KEY_START], st[sab.KEY_HTTP]), (start, 403))

    def test_status_from_message(self):
        self.assertEqual(sab.status_from_message("status=403 <!DOCTYPE html>"), 403)
        self.assertIsNone(sab.status_from_message("network"))

    # ---- 1台だけが確認する ----
    def test_only_one_worker_probes_among_8_hosts(self):
        sab.register_block(403)
        flags = {h: self.flag(h, age_sec=1000) for h in HOSTS}
        probe = mock.Mock(return_value=True)

        # 15分たつ前は誰も確認しない
        results = [sab.on_blocked_wait(flags[h], probe) for h in HOSTS]
        self.assertEqual(results, ["wait"] * 8)
        probe.assert_not_called()

        # 時刻を過ぎたら、8台×2ワーカーが同時に呼んでも確認は1回だけ
        self.make_due()
        probe_ng = mock.Mock(return_value=False)
        out = []
        barrier = threading.Barrier(16)

        def run(h):
            barrier.wait()
            out.append(sab.on_blocked_wait(flags[h], probe_ng))
        threads = [threading.Thread(target=run, args=(h,)) for h in HOSTS + HOSTS]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(probe_ng.call_count, 1)
        self.assertEqual(sorted(out), ["probe_ng"] + ["wait"] * 15)
        self.assertTrue(all(os.path.exists(p) for p in flags.values()))  # 失敗なら誰も再開しない
        st = self.state()
        self.assertEqual(st[sab.KEY_ATTEMPTS], 1)
        self.assertAlmostEqual(st[sab.KEY_NEXT] - st["now"], 1800, delta=5)  # 次は30分後

    def test_success_resumes_all_8_hosts(self):
        sab.register_block(403)
        flags = {h: self.flag(h, age_sec=1000) for h in HOSTS}
        self.make_due()
        probe = mock.Mock(return_value=True)
        first = sab.on_blocked_wait(flags[HOSTS[3]], probe)
        self.assertEqual(first, "probe_ok")
        self.assertFalse(os.path.exists(flags[HOSTS[3]]))
        self.assertIsNone(self.state()[sab.KEY_START])
        # 他の台は次の待機で解除に気づいて自分のファイルを消す（確認アクセスはしない）
        for h in HOSTS:
            if h != HOSTS[3]:
                self.assertEqual(sab.on_blocked_wait(flags[h], probe), "removed")
        self.assertEqual(probe.call_count, 1)
        self.assertFalse(any(os.path.exists(p) for p in flags.values()))

    def test_backoff_and_limit_then_manual(self):
        sab.register_block(403)
        path = self.flag("mouse", age_sec=1000)
        gaps = []
        for i in range(len(sab.PROBE_DELAYS_SEC)):
            self.make_due()
            self.assertEqual(sab.on_blocked_wait(path, lambda: False), "probe_ng")
            st = self.state()
            self.assertEqual(st[sab.KEY_ATTEMPTS], i + 1)
            gaps.append(None if st[sab.KEY_NEXT] is None else round((st[sab.KEY_NEXT] - st["now"]) / 60))
        self.assertEqual(gaps, [30, 60, 60, 60, None])  # 15分→30分→60分…、上限5回で打ち切り
        # 上限後は確認しない（手動対応待ち）
        self.make_due()  # next が NULL なので変わらない
        probe = mock.Mock(return_value=True)
        self.assertEqual(sab.on_blocked_wait(path, probe), "wait")
        probe.assert_not_called()
        self.assertIn("手動対応を待っています", sab.describe_state(self.state()))
        # 手動解除で全台再開
        self.assertTrue(sab.clear_block())
        self.assertEqual(sab.on_blocked_wait(path, probe), "removed")

    def test_new_block_after_manual_wait_starts_new_schedule(self):
        sab.register_block(403)
        self.sql("UPDATE mst.control SET param_value = NULL WHERE param_key = ?", sab.KEY_NEXT)
        self.sql("UPDATE mst.control SET param_value = 5 WHERE param_key = ?", sab.KEY_ATTEMPTS)
        self.conn.commit()
        self.assertEqual(sab.register_block(429), "new")
        st = self.state()
        self.assertEqual((st[sab.KEY_ATTEMPTS], st[sab.KEY_HTTP]), (0, 429))
        self.assertIsNotNone(st[sab.KEY_NEXT])

    def test_flag_written_after_clear_is_kept(self):
        """解除より後に書かれたファイル（解除後に別の 403 を受けた台）は消さず、停止として記録し直す"""
        sab.clear_block()
        time.sleep(2)
        path = self.flag("x162-43-29-154", age_sec=0)
        self.assertEqual(sab.on_blocked_wait(path, mock.Mock()), "reregistered")
        self.assertTrue(os.path.exists(path))
        self.assertIsNotNone(self.state()[sab.KEY_START])

    def test_missing_record_is_reregistered(self):
        path = self.flag("mouse", age_sec=100)
        self.assertEqual(sab.on_blocked_wait(path, mock.Mock()), "reregistered")
        st = self.state()
        self.assertIsNotNone(st[sab.KEY_START])
        self.assertIsNone(st[sab.KEY_HTTP])
        self.assertTrue(os.path.exists(path))

    def test_probe_exception_counts_as_failure(self):
        sab.register_block(403)
        path = self.flag("mouse", age_sec=1000)
        self.make_due()

        def boom():
            raise RuntimeError("x")
        self.assertEqual(sab.on_blocked_wait(path, boom), "probe_ng")
        self.assertIsNotNone(self.state()[sab.KEY_START])

    # ---- メール ----
    def test_describe_state(self):
        self.assertEqual(sab.describe_state(self.state()), "")
        sab.register_block(403)
        text = sab.describe_state(self.state())
        for s in ("HTTPステータス: 403", "api.mercari.jp/v2/entities:search", "停止開始:", "確認 0/5 回済み",
                  "断定できません"):
            self.assertIn(s, text)

    def test_stall_mail_includes_block_and_recovery_mail_kept(self):
        sab.register_block(403)
        t0 = datetime(2026, 11, 22, 14, 13)
        times = iter([t0, t0 + timedelta(minutes=61), t0 + timedelta(minutes=62), t0 + timedelta(minutes=63)])
        counts = iter([6849, 6849, 6849, 6833, 0])

        class Conn:
            def run(self, fn, *a):
                return fn(self, *a)
        with mock.patch.object(il, "_now", side_effect=lambda: next(times, t0 + timedelta(minutes=64))), \
                mock.patch.object(il, "send_mail") as sm, mock.patch.object(il, "write_status"), \
                mock.patch.object(il, "log"), mock.patch.object(il.time, "sleep"):
            il.wait_until_zero(Conn(), "完全 fetch_sold", lambda c: next(counts))
        subjects = [c.args[0] for c in sm.call_args_list]
        self.assertEqual(subjects, ["【要確認】在庫管理の進捗が停止しています（完全 fetch_sold）",
                                    "【回復】在庫管理の進捗が再開しました（完全 fetch_sold）"])
        stall = sm.call_args_list[0].args[1]
        self.assertIn("HTTPステータス: 403", stall)
        self.assertIn("自動復旧:", stall)
        self.assertNotIn("検索APIの停止", sm.call_args_list[1].args[1])

    def test_stall_mail_without_block_has_no_cause(self):
        self.assertEqual(il.search_api_block_note(), "")


class ScrapeWorkerBlockTest(unittest.TestCase):
    """scrape_worker の 401・403・429 で停止を記録すること（DB はモック）"""

    def test_blocked_registers(self):
        from apps.adapters.mercari_search_api import SearchApiBlocked
        from apps.inventory import scrape_worker as sw
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(sw, "SEARCH_API_BLOCKED_FILE", os.path.join(d, "b.txt")), \
                mock.patch("apps.adapters.mercari_search_api.fetch_search_json",
                           side_effect=SearchApiBlocked("status=403 <!DOCTYPE html>")), \
                mock.patch.object(sw.search_api_block, "register_block", return_value="new") as reg, \
                mock.patch.object(sw, "begin_browser_wait"), mock.patch.object(sw, "end_browser_wait"), \
                mock.patch("builtins.print"):
            with self.assertRaises(RuntimeError) as c:
                sw.fetch_page_json_api("https://jp.mercari.com/search?x", 1)
            self.assertTrue(os.path.exists(os.path.join(d, "b.txt")))
        self.assertIn("SEARCH_API_BLOCKED", str(c.exception))
        reg.assert_called_once_with(403)

    def test_register_failure_still_blocks(self):
        from apps.adapters.mercari_search_api import SearchApiBlocked
        from apps.inventory import scrape_worker as sw
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(sw, "SEARCH_API_BLOCKED_FILE", os.path.join(d, "b.txt")), \
                mock.patch("apps.adapters.mercari_search_api.fetch_search_json",
                           side_effect=SearchApiBlocked("status=429 x")), \
                mock.patch.object(sw.search_api_block, "register_block", side_effect=RuntimeError("db down")), \
                mock.patch.object(sw, "begin_browser_wait"), mock.patch.object(sw, "end_browser_wait"), \
                mock.patch("builtins.print"):
            with self.assertRaises(RuntimeError):
                sw.fetch_page_json_api("https://jp.mercari.com/search?x", 1)
            self.assertTrue(os.path.exists(os.path.join(d, "b.txt")))

    def test_probe_uses_api_once_without_flag(self):
        from apps.inventory import scrape_worker as sw
        with mock.patch("apps.adapters.mercari_search_api.fetch_search_json", return_value={"items": []}) as f:
            self.assertTrue(sw._probe_search_api())
        f.assert_called_once_with(sab.PROBE_URL)
        from apps.adapters.mercari_search_api import SearchApiBlocked
        with mock.patch("apps.adapters.mercari_search_api.fetch_search_json", side_effect=SearchApiBlocked("status=403")), \
                mock.patch("builtins.print"):
            self.assertFalse(sw._probe_search_api())

    def test_probe_url_is_supported(self):
        from apps.adapters.mercari_search_api import build_search_body
        body = build_search_body(sab.PROBE_URL, "uuid")  # 対応外の条件なら例外
        self.assertEqual(body["searchCondition"]["status"], ["STATUS_SOLD_OUT", "STATUS_TRADING"])


if __name__ == "__main__":
    unittest.main()
