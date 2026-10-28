# -*- coding: utf-8 -*-
"""
在庫管理の日次実績メール（inventory_daily_report）のテスト（モックのみ。DB・メールには接続しない）。

実行: python -m unittest apps.tests.test_inventory_daily_report -v
"""

import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest import mock

from apps.inventory import inventory_daily_report as rep

# 1起動目は簡易 fetch_sold の途中で再起動（数えない）。2起動目は日付をまたいでサイクル1が完了し、サイクル2の途中。
LOG = """\
2026-10-28 11:44:39,441 === inventory_loop 開始 PID=12980 ===
2026-10-28 11:44:39,480 ===== サイクル 1 開始 =====
2026-10-28 11:44:39,648 [簡易 fetch_sold] 開始: fetch_sold_ebay.py
2026-10-28 11:44:43,141 [簡易 fetch_sold] 完了待ち開始: 残り 5000 件
2026-10-28 13:24:06,068 === inventory_loop 開始 PID=42140 ===
2026-10-28 13:24:06,101 ===== サイクル 1 開始 =====
2026-10-28 13:24:06,287 [簡易 fetch_sold] 開始: fetch_sold_ebay.py
2026-10-28 13:24:09,961 [簡易 fetch_sold] 完了待ち開始: 残り 4980 件
2026-10-28 14:43:10,665 [簡易 fetch_sold] 終了: ジョブ 4980 件 / 所要 1:19:04.378201
2026-10-28 14:43:10,666 [完全 fetch_active] 開始: fetch_active_ebay.py
2026-10-28 17:24:54,704 [完全 fetch_active] 終了: ジョブ 5002 件 / 所要 2:41:44.036546
2026-10-28 17:24:54,799 [完全 fetch_sold] 開始: fetch_sold_ebay.py
2026-10-28 18:59:59,045 [完全 fetch_sold] 終了: ジョブ 4995 件 / 所要 1:35:04.244547
2026-10-28 19:00:02,047 [完全 check_remaining] 開始: check_remaining
2026-10-28 19:00:15,160 [完全 check_remaining] 完了待ち開始: 残り 5382 件
2026-10-29 01:00:02,000 [完全 check_remaining] 終了: 対象 5382 件 / 所要 6:00:00
2026-10-29 01:00:02,100 ===== サイクル 1 終了（所要 11:35:56） =====
2026-10-29 01:00:02,200 ===== サイクル 2 開始 =====
2026-10-29 01:00:02,300 [簡易 fetch_sold] 開始: fetch_sold_ebay.py
2026-10-29 01:00:05,300 [簡易 fetch_sold] 完了待ち開始: 残り 5000 件
"""


class ParseLogTest(unittest.TestCase):
    def setUp(self):
        self.phases, self.cycles, self.open_phase = rep.parse_loop_log(LOG.splitlines())

    def test_restarted_run_is_not_counted(self):
        # 1起動目の簡易 fetch_sold（終了行なし）とサイクル1は数えない
        self.assertEqual([r.phase for r in self.phases],
                         ["簡易 fetch_sold", "完全 fetch_active", "完全 fetch_sold", "完全 check_remaining"])
        self.assertEqual(self.phases[0].started, datetime(2026, 10, 28, 13, 24, 6))
        self.assertEqual(self.phases[0].count, 4980)
        self.assertEqual(len(self.cycles), 1)
        self.assertEqual(self.cycles[0].started, datetime(2026, 10, 28, 13, 24, 6))

    def test_open_phase(self):
        self.assertEqual(self.open_phase.phase, "簡易 fetch_sold")
        self.assertEqual(self.open_phase.initial, 5000)

    def test_cross_midnight_counted_once_on_end_day(self):
        d28 = rep.runs_ended_on(self.cycles, {date(2026, 10, 28)})
        d29 = rep.runs_ended_on(self.cycles, {date(2026, 10, 29)})
        self.assertEqual((len(d28), len(d29)), (0, 1))
        cr = [r for r in rep.runs_ended_on(self.phases, {date(2026, 10, 29)})]
        self.assertEqual([r.phase for r in cr], ["完全 check_remaining"])

    def test_stop_line_discards_open(self):
        lines = LOG.splitlines() + ["2026-10-29 02:00:00,000 === inventory_loop 手動停止（Ctrl+C） ==="]
        _, _, op = rep.parse_loop_log(lines)
        self.assertIsNone(op)

    def test_untimestamped_lines_are_ignored(self):
        lines = ["2026-10-29 03:00:00,000 在庫管理が既に動いているため起動を中止します", "  PID 1 inventory_loop.py"]
        self.assertEqual(rep.parse_loop_log(lines), ([], [], None))


class BuildReportTest(unittest.TestCase):
    def build(self, target, status=None, progress=None, windows=None):
        lines = LOG.splitlines()
        phases, cycles, op = rep.parse_loop_log(lines)
        return rep.build_report(target, phases, cycles, op, rep.log_days_of(lines), windows or {},
                                status, progress, datetime(2026, 10, 30, 8, 0))

    def test_no_data_days(self):
        report = self.build(date(2026, 10, 27))
        title, text, body_html = rep.render(report, sample=False)
        self.assertIn("2026/10/27", title)
        self.assertNotIn("サンプル", title)
        self.assertIn("完了回数（回転数） | データなし", text)
        self.assertIn("データなし", body_html)
        self.assertNotIn("0 回", text.split("■ 2.")[0])

    def test_day_with_log_but_no_completed_cycle_is_zero(self):
        report = self.build(date(2026, 10, 28))
        _, text, _ = rep.render(report, sample=True)
        self.assertIn("完了回数（回転数） | 0 回", text)
        self.assertIn("簡易 fetch_sold | 1 回 | 1時間19分", text)
        self.assertIn("完全 check_remaining | データなし", text)

    def test_target_day_vs_prev_and_week(self):
        report = self.build(date(2026, 10, 29))
        _, text, _ = rep.render(report, sample=False)
        self.assertIn("完了回数（回転数） | 1 回", text)
        self.assertIn("1サイクルの平均所要時間 | 11時間36分", text)
        # check_remaining 5382 件 / 6時間 = 897 件/時
        self.assertIn("完全 check_remaining | 1 回 | 6時間00分 | 897 件/時", text)
        # 前日(10/28)・直近7日（10/22〜10/28、記録は10/28の1日）
        self.assertIn("直近7日 = 10/22〜10/28のうち在庫管理の記録がある 1 日の平均", text)
        self.assertIn("完全在庫チェック 完了回数（1日あたり） | 1 回 | 0 回 | 0.0 回", text)

    def test_concurrent_split(self):
        # 出品が 10/28 14:00〜15:00 → 簡易 fetch_sold（13:24〜14:43）と完全 fetch_active（14:43〜17:24）が並行あり
        windows = {date(2026, 10, 28): (datetime(2026, 10, 28, 14), datetime(2026, 10, 28, 15), 100)}
        report = self.build(date(2026, 10, 28), windows=windows)
        con = report["concurrent"]
        self.assertEqual(con["簡易 fetch_sold"][0]["n"], 1)
        self.assertIsNone(con["簡易 fetch_sold"][1])
        self.assertEqual(con["完全 fetch_sold"][1]["n"], 1)
        self.assertIsNone(con["完全 fetch_sold"][0])
        _, text, _ = rep.render(report, sample=False)
        self.assertIn("対象日の出品処理の時間帯: 14:00〜15:00（eBay出品 100 件）", text)

    def test_current_progress(self):
        status = {"state": "running", "phase": "簡易 fetch_sold", "phase_started_at": "2026-10-29T01:00:02",
                  "cycle_no": 2, "cycle_started_at": "2026-10-29T01:00:02"}
        report = self.build(date(2026, 10, 29), status=status, progress=(1234, 5000))
        _, text, _ = rep.render(report, sample=False)
        self.assertIn("実行中の工程 | 簡易 fetch_sold", text)
        self.assertIn("処理済み／総件数 | 1,234 / 5,000 件", text)
        self.assertIn("進捗率 | 24.7%", text)

    def test_not_running(self):
        report = self.build(date(2026, 10, 29), status={"state": "stopped"})
        _, text, _ = rep.render(report, sample=False)
        self.assertIn("在庫管理は稼働していません（stopped）", text)


class ProgressTest(unittest.TestCase):
    def test_check_remaining_uses_initial_and_outstanding(self):
        op = rep.OpenPhase("完全 check_remaining", datetime(2026, 10, 28, 19), 5382)
        status = {"state": "running", "phase": "完全 check_remaining", "phase_started_at": "2026-10-28T19:00:02",
                  "outstanding": 1644}
        self.assertEqual(rep.fetch_progress(None, status, op), (3738, 5382))

    def test_scrape_phase_uses_db(self):
        cur = mock.MagicMock()
        cur.fetchone.return_value = (5000, 1200)
        conn = mock.MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        status = {"state": "running", "phase": "完全 fetch_active", "phase_started_at": "2026-10-28T14:43:10"}
        self.assertEqual(rep.fetch_progress(conn, status, None), (1200, 5000))
        cur.execute.assert_called_once_with(rep.SQL_PHASE_JOB_PROGRESS, "fetch_active_ebay", datetime(2026, 10, 28, 14, 43, 10))


class MainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        tmp = Path(self.tmp.name)
        (tmp / "loop.log").write_text(LOG, encoding="utf-8")
        self.patches = [
            mock.patch.object(rep, "STATE_PATH", tmp / "state.json"),
            mock.patch.object(rep, "LOOP_LOG_PATH", tmp / "loop.log"),
            mock.patch.object(rep, "read_status", return_value=None),
            mock.patch.object(rep, "get_sql_server_connection"),
            mock.patch.object(rep, "fetch_publish_windows", return_value={}),
            mock.patch.object(rep, "log"),
        ]
        for p in self.patches:
            p.start()
        self.state = tmp / "state.json"

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def test_sent_once_per_target_date(self):
        with mock.patch.object(rep, "send_mail", return_value=True) as sm:
            self.assertEqual(rep.main(["--date", "2026-10-29"]), 0)
            self.assertEqual(rep.main(["--date", "2026-10-29"]), 0)
        self.assertEqual(sm.call_count, 1)
        self.assertIn("2026-10-29", json.loads(self.state.read_text(encoding="utf-8"))["sent"])

    def test_failure_is_not_recorded_and_retried(self):
        with mock.patch.object(rep, "send_mail", return_value=False):
            self.assertEqual(rep.main(["--date", "2026-10-29"]), 1)
        self.assertFalse(self.state.exists())
        with mock.patch.object(rep, "send_mail", return_value=True) as sm:
            self.assertEqual(rep.main(["--date", "2026-10-29"]), 0)
        sm.assert_called_once()

    def test_sample_does_not_block_scheduled_mail(self):
        with mock.patch.object(rep, "send_mail", return_value=True) as sm:
            rep.main(["--date", "2026-10-29", "--sample"])
            self.assertTrue(sm.call_args.args[0].startswith("【サンプル】"))
            self.assertFalse(self.state.exists())
            rep.main(["--date", "2026-10-29"])
        self.assertEqual(sm.call_count, 2)
        self.assertFalse(sm.call_args.args[0].startswith("【サンプル】"))

    def test_default_target_is_yesterday(self):
        with mock.patch.object(rep, "send_mail", return_value=True) as sm:
            rep.main([])
        yesterday = datetime.now().date().toordinal() - 1
        self.assertIn(f"{date.fromordinal(yesterday):%Y/%m/%d}", sm.call_args.args[0])


def tearDownModule():
    for h in list(rep._logger.handlers):
        h.close()


if __name__ == "__main__":
    unittest.main()
