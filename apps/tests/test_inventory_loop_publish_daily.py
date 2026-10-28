# -*- coding: utf-8 -*-
"""
inventory_loop.py / publish_daily.py のテスト（モックのみ。DB・メール・SSH・eBay には接続しない）。

実行: python -m unittest apps.tests.test_inventory_loop_publish_daily -v
"""

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from apps.inventory import inventory_loop as il
from apps.inventory import inventory_monitor as im
from apps.publish import publish_daily as pd


class FakeConn:
    """ReconnectingConnection の代わり。run(func, *args) は func(self, *args) を呼ぶだけ。"""

    def __init__(self):
        self.autocommit = False
        self.closed = False
        self.rollbacks = 0

    def run(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class TempStatusMixin:
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        p = mock.patch.object(il, "STATUS_PATH", Path(self._tmp.name) / "status.json")
        p.start()
        self.addCleanup(p.stop)
        self.addCleanup(self._tmp.cleanup)
        # テストでは本番ログ（logs/inventory_loop.log）に書かない
        self.logs = []
        p = mock.patch.object(il, "log", side_effect=self.logs.append)
        p.start()
        self.addCleanup(p.stop)


# ============================================================
# 月の切り替え（米国太平洋時間）
# ============================================================
class PacificMonthTest(unittest.TestCase):
    def k(self, iso):
        return pd.pacific_month_key(datetime.fromisoformat(iso))

    def test_boundary_in_daylight_time(self):
        # 2026-11-01 0:00 PDT(UTC-7) = 07:00Z
        self.assertEqual(self.k("2026-11-01T06:59:59+00:00"), 202610)
        self.assertEqual(self.k("2026-11-01T07:00:00+00:00"), 202611)

    def test_boundary_in_standard_time(self):
        # 2026-12-01 0:00 PST(UTC-8) = 08:00Z
        self.assertEqual(self.k("2026-12-01T07:59:59+00:00"), 202611)
        self.assertEqual(self.k("2026-12-01T08:00:00+00:00"), 202612)
        # 2027-03-01 は夏時間開始(3/14)前なので PST
        self.assertEqual(self.k("2027-03-01T07:59:59+00:00"), 202702)
        self.assertEqual(self.k("2027-03-01T08:00:00+00:00"), 202703)
        # 2027-04-01 は夏時間(PDT)
        self.assertEqual(self.k("2027-04-01T06:59:59+00:00"), 202703)
        self.assertEqual(self.k("2027-04-01T07:00:00+00:00"), 202704)

    def test_jst_3am_runs(self):
        # JST 11/1 3:00 は太平洋時間ではまだ 10/31 → 初期化しない。JST 11/2 3:00 で初めて11月
        self.assertEqual(self.k("2026-11-01T03:00:00+09:00"), 202610)
        self.assertEqual(self.k("2026-11-02T03:00:00+09:00"), 202611)
        # 冬時間: JST 12/1 3:00 = PST 11/30 10:00、JST 12/2 3:00 = PST 12/1 10:00
        self.assertEqual(self.k("2026-12-01T03:00:00+09:00"), 202611)
        self.assertEqual(self.k("2026-12-02T03:00:00+09:00"), 202612)

    def test_month_start_in_server_time(self):
        self.assertEqual(pd.pacific_month_start_server_time(202611), datetime(2026, 11, 1, 16, 0))
        self.assertEqual(pd.pacific_month_start_server_time(202612), datetime(2026, 12, 1, 17, 0))


# ============================================================
# 出品枠の初期化
# ============================================================
class FakeQuotaDB:
    """mst.control と mst.ebay_accounts の必要な部分だけを模したもの。"""

    def __init__(self, reset_month, accounts):
        self.reset_month = reset_month          # None = 行なし
        self.accounts = dict(accounts)          # account -> [listing_left, additional_listing_left]
        self.commits = 0
        self.rollbacks = 0
        self.pending = None


class FakeQuotaConn(FakeConn):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.autocommit = True

    def cursor(self):
        return FakeQuotaCursor(self)

    def commit(self):
        if self.db.pending:
            self.db.pending()
        self.db.pending = None
        self.db.commits += 1

    def rollback(self):
        self.db.pending = None
        self.db.rollbacks += 1


class FakeQuotaCursor:
    def __init__(self, conn):
        self.conn = conn
        self.db = conn.db
        self._result = []
        self.ops = []

    def execute(self, sql, *params):
        assert self.conn.autocommit is False, "初期化はトランザクション内で行うこと"
        s = " ".join(sql.split())
        if s.startswith("SELECT param_value FROM mst.control WITH (UPDLOCK, HOLDLOCK)"):
            self._result = [] if self.db.reset_month == "NO_ROW" else [(self.db.reset_month,)]
        elif s.startswith("SELECT account, listing_left"):
            self._result = [(a, v[0], v[1]) for a, v in sorted(self.db.accounts.items())]
        elif s.startswith("UPDATE mst.ebay_accounts SET listing_left = ?"):
            left, add_left = params
            prev = self.db.pending

            def apply():
                if prev:
                    prev()
                for v in self.db.accounts.values():
                    v[0], v[1] = left, add_left
            self.db.pending = apply
        elif s.startswith("UPDATE mst.control SET param_value = ?"):
            month, key = params
            assert key == pd.CONTROL_KEY_LISTING_RESET_MONTH
            prev = self.db.pending

            def apply():
                if prev:
                    prev()
                self.db.reset_month = month
            self.db.pending = apply
        else:
            raise AssertionError(f"想定外のSQL: {s}")

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result


class ListingQuotaResetTest(unittest.TestCase):
    NOV_2_JST_3AM = datetime.fromisoformat("2026-11-02T03:00:00+09:00")

    def db(self, month):
        return FakeQuotaDB(month, {"BUZZ": [1611, 50000], "谷川②": [4400, 43877]})

    def test_resets_once_when_month_changed(self):
        db = self.db(202610)
        conn = FakeQuotaConn(db)
        result = pd.reset_listing_quota_if_new_month(conn, self.NOV_2_JST_3AM)
        self.assertEqual(result["month"], 202611)
        self.assertEqual(result["prev_month"], 202610)
        self.assertEqual(result["before"], [("BUZZ", 1611, 50000), ("谷川②", 4400, 43877)])
        self.assertEqual(db.accounts, {"BUZZ": [10000, 50000], "谷川②": [10000, 50000]})
        self.assertEqual(db.reset_month, 202611)
        self.assertEqual(db.commits, 1)
        self.assertTrue(conn.autocommit)

        # 再実行・再起動しても同じ月には二重リセットしない（出品で減った残数を戻さない）
        db.accounts["BUZZ"][0] = 9500
        self.assertIsNone(pd.reset_listing_quota_if_new_month(conn, self.NOV_2_JST_3AM))
        self.assertEqual(db.accounts["BUZZ"][0], 9500)
        self.assertEqual(db.commits, 1)

    def test_no_reset_before_pacific_month_changes(self):
        db = self.db(202610)
        conn = FakeQuotaConn(db)
        # JST 11/1 3:00（太平洋時間は 10/31）
        self.assertIsNone(pd.reset_listing_quota_if_new_month(
            conn, datetime.fromisoformat("2026-11-01T03:00:00+09:00")))
        self.assertEqual(db.accounts["BUZZ"], [1611, 50000])
        self.assertEqual(db.commits, 0)
        self.assertTrue(conn.autocommit)

    def test_missing_control_row_stops(self):
        for value in ("NO_ROW", None):
            db = self.db(value)
            conn = FakeQuotaConn(db)
            with self.assertRaisesRegex(RuntimeError, "出品枠リセット月"):
                pd.reset_listing_quota_if_new_month(conn, self.NOV_2_JST_3AM)
            self.assertEqual(db.accounts["BUZZ"], [1611, 50000])
            self.assertEqual(db.commits, 0)
            self.assertEqual(db.rollbacks, 1)
            self.assertTrue(conn.autocommit)

    def test_failure_before_commit_changes_nothing(self):
        db = self.db(202610)
        conn = FakeQuotaConn(db)
        orig = FakeQuotaCursor.execute

        def failing(self_, sql, *params):
            if "UPDATE mst.control" in sql:
                raise RuntimeError("接続断")
            return orig(self_, sql, *params)

        with mock.patch.object(FakeQuotaCursor, "execute", failing):
            with self.assertRaises(RuntimeError):
                pd.reset_listing_quota_if_new_month(conn, self.NOV_2_JST_3AM)
        self.assertEqual(db.accounts["BUZZ"], [1611, 50000])
        self.assertEqual(db.reset_month, 202610)
        # 失敗後の再実行で初期化される
        self.assertIsNotNone(pd.reset_listing_quota_if_new_month(conn, self.NOV_2_JST_3AM))
        self.assertEqual(db.accounts["BUZZ"], [10000, 50000])


class CrossedMonthNotifyTest(unittest.TestCase):
    def test_notifies_when_publish_crosses_month(self):
        conn = FakeConn()
        start = datetime.fromisoformat("2026-10-31T10:00:00-07:00")
        end = datetime.fromisoformat("2026-11-01T01:00:00-07:00")
        with mock.patch.object(pd, "count_listings_since", return_value=[("BUZZ", 12)]) as cnt, \
             mock.patch.object(pd, "send_mail") as mail:
            self.assertTrue(pd.notify_if_publish_crossed_pacific_month(conn, start, end))
        cnt.assert_called_once_with(conn, datetime(2026, 11, 1, 16, 0))
        self.assertIn("月が替わりました", mail.call_args[0][0])
        self.assertIn("BUZZ: 12 件", mail.call_args[0][1])

    def test_silent_within_same_month(self):
        start = datetime.fromisoformat("2026-10-30T18:00:00+00:00")
        end = datetime.fromisoformat("2026-10-31T06:00:00+00:00")
        with mock.patch.object(pd, "send_mail") as mail:
            self.assertFalse(pd.notify_if_publish_crossed_pacific_month(FakeConn(), start, end))
        mail.assert_not_called()


# ============================================================
# 二重起動の確認
# ============================================================
class FakeProc:
    def __init__(self, pid, name, cmdline):
        self.info = {"pid": pid, "name": name, "cmdline": cmdline, "create_time": 1_800_000_000}


class FindRunningScriptsTest(unittest.TestCase):
    PROCS = [
        FakeProc(10, "python.exe", ["python", r"D:\apps_nostock\apps\inventory\daily_check.py"]),
        FakeProc(11, "python.exe", ["python", r"apps\publish\publish_manager.py"]),
        FakeProc(12, "python.exe", ["python", r"D:\apps_nostock\apps\publish\publish_daily.py"]),
        FakeProc(13, "python.exe", ["python", r"D:\apps_nostock\apps\publish\publish_ebay.py"]),
        FakeProc(14, "notepad.exe", ["notepad", r"D:\x\publish_daily.py"]),
        FakeProc(15, "python.exe", None),
        # 現行の daily_check は「python -m apps.inventory.daily_check」で起動されている（2026-10-27 実測）
        FakeProc(16, "python.exe", ["python.exe", "-m", "apps.inventory.daily_check"]),
        FakeProc(17, "python.exe", ["python.exe", "-u", "-m", "apps.inventory.scrape_worker"]),
        FakeProc(18, "python.exe", ["python", "-m", "apps.inventory.inventory_loop"]),
    ]

    def run_find(self, names, exclude=None):
        with mock.patch.object(il.psutil, "process_iter", return_value=self.PROCS):
            return [(pid, name) for pid, name, _ in il.find_running_scripts(names, exclude_pid=exclude)]

    def test_publish_conflicts(self):
        self.assertEqual(
            self.run_find(pd.PUBLISH_CONFLICT_SCRIPTS, exclude=12),
            [(10, "daily_check.py"), (11, "publish_manager.py"), (16, "daily_check.py")],
        )

    def test_self_and_unrelated_processes_are_ignored(self):
        self.assertEqual(self.run_find(("publish_daily.py",), exclude=12), [])
        self.assertEqual(
            self.run_find(il.INVENTORY_CONFLICT_SCRIPTS),
            [(10, "daily_check.py"), (16, "daily_check.py"), (18, "inventory_loop.py")],
        )


# ============================================================
# 在庫管理の実行順序・停止制御・異常終了
# ============================================================
class InventoryMainTest(TempStatusMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.calls = []
        self.mails = []
        self.stop_flags = [False, True]
        self.run_script_codes = {}

        def run_script(path, **_):
            self.calls.append(f"run:{path.name}")
            return self.run_script_codes.get(path.name, (0, ""))

        def wait_until_zero(conn, phase, count_fn):
            self.calls.append(f"wait:{phase}:{count_fn.__name__}")
            return 3

        def should_stop(conn):
            self.calls.append("stop?")
            return self.stop_flags.pop(0)

        self.presets_error = None

        def refresh_presets(conn):
            # TRUNCATE〜INSERT を1トランザクションで行うこと（autocommit=False の間に呼ばれる）
            self.assertFalse(conn.autocommit)
            self.calls.append("presets")
            if self.presets_error:
                raise self.presets_error
        self.refresh_presets = refresh_presets

        patches = {
            "disable_console_quick_edit": mock.Mock(),
            "find_running_scripts": mock.Mock(return_value=[]),
            "ReconnectingConnection": FakeConn,
            "refresh_presets_lookup": self.refresh_presets,
            "run_script": run_script,
            "wait_until_zero": wait_until_zero,
            "reset_remaining_flags": lambda c: self.calls.append("reset_flags"),
            "launch_remaining_workers": lambda: self.calls.append("launch_remaining"),
            "notify_undetermined_remaining": lambda c, start: self.calls.append("notify_undetermined"),
            "notify_no_preset_listings": lambda c: self.calls.append("notify_no_preset"),
            "notify_error_jobs": lambda c, cycle_no, started: self.calls.append("notify_errors"),
            "should_stop_daily_check": should_stop,
            "send_mail": lambda s, b: self.mails.append((s, b)) or True,
        }
        for name, value in patches.items():
            p = mock.patch.object(il, name, value)
            p.start()
            self.addCleanup(p.stop)

    CYCLE = [
        "presets",
        "run:fetch_sold_ebay.py", "wait:簡易 fetch_sold:count_outstanding_jobs",
        "run:fetch_active_ebay.py", "wait:完全 fetch_active:count_outstanding_jobs",
        "run:fetch_sold_ebay.py", "wait:完全 fetch_sold:count_outstanding_jobs",
        "reset_flags", "launch_remaining", "wait:完全 check_remaining:count_remaining_targets",
        "notify_undetermined", "notify_no_preset",
        "notify_errors",
        "stop?",
    ]

    def test_cycles_run_in_order_and_stop_by_flag(self):
        self.assertEqual(il.main(), 0)
        self.assertEqual(self.calls, self.CYCLE + self.CYCLE)
        # 正常終了メールは停止時の1通だけ（工程ごとの正常終了メールは送らない）
        self.assertEqual([s for s, _ in self.mails], ["在庫管理を停止しました（停止フラグ）"])
        status = il.read_status()
        self.assertEqual(status["state"], "stopped")
        self.assertEqual(status["last_cycle_no"], 2)

    def test_stop_flag_is_checked_only_at_cycle_end(self):
        self.stop_flags = [True]
        self.assertEqual(il.main(), 0)
        self.assertEqual(self.calls.count("stop?"), 1)
        self.assertEqual(self.calls[-1], "stop?")
        self.assertEqual(self.calls[-2], "notify_errors")

    def test_step_failure_ends_with_alert(self):
        self.run_script_codes["fetch_active_ebay.py"] = (
            1, 'Traceback (most recent call last):\n  File "x.py", line 1, in <module>\npyodbc.Error: boom\n')
        self.assertEqual(il.main(), 1)
        self.assertNotIn("launch_remaining", self.calls)
        self.assertNotIn("stop?", self.calls)
        self.assertEqual(len(self.mails), 1)
        subject, body = self.mails[0]
        self.assertEqual(subject, "【異常】在庫管理が異常終了しました")
        self.assertIn("fetch_active_ebay.py（完全 fetch_active）", body)
        self.assertIn("pyodbc.Error", body)
        self.assertIn("工程: 完全 fetch_active", body)
        self.assertEqual(il.read_status()["state"], "error")

    def test_undetermined_is_notified_even_if_remaining_wait_fails(self):
        def failing_wait(conn, phase, count_fn):
            self.calls.append(f"wait:{phase}")
            if "remaining" in phase:
                raise RuntimeError("DB切断")
            return 0

        with mock.patch.object(il, "wait_until_zero", failing_wait):
            self.assertEqual(il.main(), 1)
        self.assertEqual(self.calls[-2:], ["wait:完全 check_remaining", "notify_undetermined"])
        self.assertIn("DB切断", self.mails[0][1])

    def test_presets_refreshed_every_cycle_in_transaction(self):
        conns = []
        with mock.patch.object(il, "ReconnectingConnection", lambda: conns.append(FakeConn()) or conns[-1]):
            self.assertEqual(il.main(), 0)
        self.assertEqual(self.calls.count("presets"), 2)
        self.assertTrue(conns[0].autocommit)  # 更新後は autocommit に戻す

    def test_presets_failure_rolls_back_and_alerts(self):
        self.presets_error = RuntimeError("presets_brand 読み取り失敗")
        conns = []
        with mock.patch.object(il, "ReconnectingConnection", lambda: conns.append(FakeConn()) or conns[-1]):
            self.assertEqual(il.main(), 1)
        self.assertEqual(conns[0].rollbacks, 1)
        self.assertTrue(conns[0].autocommit)
        self.assertEqual(self.calls, ["presets"])
        self.assertIn("presets_brand 読み取り失敗", self.mails[0][1])
        self.assertIn("工程: presets_lookup 更新", self.mails[0][1])

    def test_error_notified_is_recorded(self):
        self.run_script_codes["fetch_sold_ebay.py"] = (1, "")
        self.assertEqual(il.main(), 1)
        self.assertTrue(il.read_status()["error_notified"])
        with mock.patch.object(il, "send_mail", return_value=False):
            self.assertEqual(il.main(), 1)
        self.assertFalse(il.read_status()["error_notified"])

    def test_refuses_to_start_when_another_inventory_runs(self):
        il.find_running_scripts.return_value = [(99, "daily_check.py", datetime(2026, 10, 27, 10, 28))]
        self.assertEqual(il.main(), 1)
        self.assertEqual(self.calls, [])
        self.assertFalse(il.STATUS_PATH.exists())


# ============================================================
# 進捗停止の検知
# ============================================================
class WaitUntilZeroTest(TempStatusMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.clock = [datetime(2026, 11, 1, 9, 0)]
        self.mails = []
        patches = {
            "_now": lambda: self.clock[0],
            "send_mail": lambda s, b: self.mails.append((s, b)) or True,
        }
        for name, value in patches.items():
            p = mock.patch.object(il, name, value)
            p.start()
            self.addCleanup(p.stop)
        p = mock.patch.object(il.time, "sleep", side_effect=self.advance)
        p.start()
        self.addCleanup(p.stop)

    def advance(self, seconds):
        self.clock[0] += timedelta(seconds=seconds)

    def counts_by_minute(self, schedule):
        """schedule: [(この分数まで, 件数)]。経過分数に応じた件数を返す count_fn"""
        start = self.clock[0]

        def count_fn(conn):
            minutes = (self.clock[0] - start).total_seconds() / 60
            for until, n in schedule:
                if minutes < until:
                    return n
            return 0
        return count_fn

    def test_stall_alert_once_and_recovery(self):
        # 10件のまま90分止まり、その後 9件 → 120分で0件
        fn = self.counts_by_minute([(90, 10), (120, 9)])
        self.assertEqual(il.wait_until_zero(FakeConn(), "完全 fetch_active", fn), 10)
        subjects = [s for s, _ in self.mails]
        self.assertEqual(subjects, [
            "【要確認】在庫管理の進捗が停止しています（完全 fetch_active）",
            "【回復】在庫管理の進捗が再開しました（完全 fetch_active）",
        ])
        status = il.read_status()
        self.assertEqual(status["outstanding"], 0)

    def test_long_run_with_progress_is_not_alerted(self):
        # 合計10時間かかっても、59分ごとに件数が減っていれば通知しない（所要時間では判定しない）
        fn = self.counts_by_minute([(59 * (i + 1), 10 - i) for i in range(10)])
        self.assertEqual(il.wait_until_zero(FakeConn(), "完全 check_remaining", fn), 10)
        self.assertEqual(self.mails, [])

    def test_stall_alert_boundary(self):
        # 59分30秒で件数が動けば通知しない、60分ちょうど止まれば通知
        fn = self.counts_by_minute([(59.5, 5), (61, 4)])
        il.wait_until_zero(FakeConn(), "x", fn)
        self.assertEqual(self.mails, [])
        self.clock[0] = datetime(2026, 11, 1, 9, 0)
        fn = self.counts_by_minute([(60.5, 5)])
        il.wait_until_zero(FakeConn(), "x", fn)
        self.assertEqual(len([s for s, _ in self.mails if s.startswith("【要確認】")]), 1)


# ============================================================
# 出品処理（publish_daily）
# ============================================================
class PublishMainTest(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.mails = []
        patches = {
            "disable_console_quick_edit": mock.Mock(),
            "find_running_scripts": mock.Mock(return_value=[]),
            "ReconnectingConnection": FakeConn,
            "reset_listing_quota_if_new_month": mock.Mock(side_effect=lambda c, now: self.calls.append("quota")),
            "run_delete_ebay_daily": lambda c: self.calls.append("delete") or 0,
            "run_publish_manager": lambda c: self.calls.append("publish") or 0,
            "notify_if_publish_crossed_pacific_month": lambda c, s, e: self.calls.append("cross_check"),
            "send_mail": lambda s, b: self.mails.append((s, b)) or True,
        }
        for name, value in patches.items():
            p = mock.patch.object(pd, name, value)
            p.start()
            self.addCleanup(p.stop)

    def test_normal_order(self):
        self.assertEqual(pd.main(), 0)
        self.assertEqual(self.calls, ["quota", "delete", "publish", "cross_check"])
        self.assertEqual(self.mails, [])

    def test_quota_reset_notified_before_delete_and_publish(self):
        result = {"month": 202611, "prev_month": 202610, "before": [("BUZZ", 1611, 50000)]}

        def quota(c, now):
            self.calls.append("quota")
            return result
        pd.reset_listing_quota_if_new_month.side_effect = quota
        self.assertEqual(pd.main(), 0)
        self.assertEqual(self.calls, ["quota", "delete", "publish", "cross_check"])
        self.assertEqual(self.mails[0][0], "出品枠を初期化しました（202611）")
        self.assertIn("BUZZ: listing_left 1611 → 10000", self.mails[0][1])

    def test_previous_publish_running_blocks_everything(self):
        pd.find_running_scripts.return_value = [(77, "publish_manager.py", datetime(2026, 11, 1, 3, 0))]
        self.assertEqual(pd.main(), 1)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.mails[0][0], "【異常】前回の出品処理が継続中のため本日の出品を開始しません")
        self.assertIn("PID 77 publish_manager.py", self.mails[0][1])

    def test_quota_error_stops_before_publish(self):
        pd.reset_listing_quota_if_new_month.side_effect = RuntimeError("mst.control『出品枠リセット月』が未設定です")
        self.assertEqual(pd.main(), 1)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.mails[0][0], "【異常】出品処理（publish_daily）が異常終了しました")
        self.assertIn("出品枠リセット月", self.mails[0][1])

    def test_publish_manager_failure_returns_error(self):
        with mock.patch.object(pd, "run_publish_manager", lambda c: self.calls.append("publish") or 1):
            self.assertEqual(pd.main(), 1)
        self.assertEqual(self.calls, ["quota", "delete", "publish", "cross_check"])


# ============================================================
# 在庫管理の独立監視（inventory_monitor）
# ============================================================
class InventoryMonitorTest(TempStatusMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        p = mock.patch.object(im, "MONITOR_STATE_PATH", Path(self._tmp.name) / "monitor.json")
        p.start()
        self.addCleanup(p.stop)
        for target in (im._logger, ):
            p = mock.patch.object(target, "info")
            p.start()
            self.addCleanup(p.stop)
        self.alive = set()
        self.now = datetime(2026, 11, 2, 3, 0)
        self.mail_ok = True
        self.mails = []

    def write(self, **status):
        status.setdefault("started_at", "2026-11-01T10:00:00")
        status.setdefault("pid", 5)
        status.setdefault("updated_at", (self.now - timedelta(seconds=20)).isoformat())
        il.STATUS_PATH.write_text(json.dumps(status), encoding="utf-8")

    def tick(self):
        """10分ごとの1回分を実行し、送ったメールの件名を返す"""
        procs = [(pid, "inventory_loop.py", self.now) for pid in self.alive]
        before = len(self.mails)
        with mock.patch.object(im, "find_running_scripts", return_value=procs), \
             mock.patch.object(im, "read_status", il.read_status), \
             mock.patch.object(im, "datetime", wraps=datetime) as dt, \
             mock.patch.object(im, "send_mail", side_effect=lambda s, b: self.mails.append(s) or self.mail_ok):
            dt.now.return_value = self.now
            self.assertEqual(im.main(), 0)
        self.now += timedelta(minutes=10)
        return self.mails[before:]

    def test_running_normally_no_mail(self):
        self.alive = {5}
        self.write(state="running")
        self.assertEqual(self.tick(), [])

    def test_process_gone_notified_once(self):
        self.write(state="running")
        self.assertEqual(self.tick(), ["【異常】在庫管理のプロセスが停止しています"])
        self.write(state="running", updated_at="2026-11-02T02:59:40")
        self.assertEqual(self.tick(), [])
        self.assertEqual(self.tick(), [])

    def test_restart_then_stop_again_is_notified_again(self):
        self.write(state="running")
        self.assertEqual(len(self.tick()), 1)
        # 手動で再起動 → 正常稼働（記録が消える）
        self.alive = {6}
        self.write(state="running", pid=6, started_at="2026-11-02T03:15:00")
        self.assertEqual(self.tick(), [])
        self.assertIsNone(im.load_notified_key())
        # 再び停止 → 再通知
        self.alive = set()
        self.write(state="running", pid=6, started_at="2026-11-02T03:15:00")
        self.assertEqual(self.tick(), ["【異常】在庫管理のプロセスが停止しています"])

    def test_hang_notified_once_and_again_after_recovery(self):
        self.alive = {5}
        self.write(state="running", updated_at="2026-11-02T02:30:00")   # 30分更新なし
        self.assertEqual(self.tick(), ["【要確認】在庫管理が応答していません"])
        self.write(state="running", updated_at="2026-11-02T02:30:00")
        self.assertEqual(self.tick(), [])
        self.write(state="running")                                       # 回復
        self.assertEqual(self.tick(), [])
        self.write(state="running", updated_at=(self.now - timedelta(minutes=31)).isoformat())
        self.assertEqual(self.tick(), ["【要確認】在庫管理が応答していません"])

    def test_heartbeat_just_under_threshold(self):
        self.alive = {5}
        self.write(state="running", updated_at=(self.now - timedelta(minutes=29, seconds=59)).isoformat())
        self.assertEqual(self.tick(), [])

    def test_stop_flag_and_ctrl_c_are_not_notified(self):
        for reason in ("mst.control 日次処理停止", "Ctrl+C"):
            self.write(state="stopped", stop_reason=reason)
            self.assertEqual(self.tick(), [])
        self.assertFalse(im.MONITOR_STATE_PATH.exists())

    def test_not_started(self):
        self.assertEqual(self.tick(), [])

    def test_error_state(self):
        # inventory_loop 自身が通知済みなら送らない
        self.write(state="error", error_notified=True)
        self.assertEqual(self.tick(), [])
        # 自身のメール送信が失敗していたら代わりに送る（1回だけ）
        self.write(state="error", error_notified=False)
        self.assertEqual(self.tick(), ["【異常】在庫管理が異常終了しました"])
        self.assertEqual(self.tick(), [])

    def test_mail_failure_is_retried_next_time(self):
        self.write(state="running")
        self.mail_ok = False
        self.assertEqual(len(self.tick()), 1)
        self.assertIsNone(im.load_notified_key())
        self.mail_ok = True
        self.assertEqual(len(self.tick()), 1)
        self.assertEqual(self.tick(), [])


# ============================================================
# サイクル終了時の error ジョブ通知・待機対象
# ============================================================
class FakeSelectConn(FakeConn):
    """cursor().execute(sql, *params) を記録し、fetchall() で rows を返す。"""

    def __init__(self, rows):
        super().__init__()
        self.rows = rows
        self.executed = []

    def cursor(self):
        conn = self

        class _Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, *params):
                conn.executed.append((sql, params))

            def fetchall(self):
                return conn.rows

        return _Cur()


TB_503 = (
    "Traceback (most recent call last):\n  File ...\n"
    "requests.exceptions.HTTPError: 503 Server Error: Service Temporarily Unavailable for url: https://fril.jp/s?x=1\n"
)
RETRY_OUT_503 = (
    "[再試行を使い切った retry=6/6] RAKUMA_HTTP status=503 retry_after=None server=nginx "
    "content_type=text/html url=https://fril.jp/s?x=1 body_head='<html>'"
)
NO_RETRY_403 = (
    "[再試行対象外 retry=0/6] RAKUMA_HTTP status=403 retry_after=None server=nginx "
    "content_type=text/html url=https://fril.jp/s?x=2 body_head='forbidden'"
)


class ErrorJobNotifyTest(TempStatusMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.mails = []
        p = mock.patch.object(il, "send_mail", lambda s, b: self.mails.append((s, b)) or True)
        p.start()
        self.addCleanup(p.stop)

    def test_no_error_jobs_no_mail(self):
        conn = FakeSelectConn([])
        started = datetime(2026, 10, 28, 1, 0)
        il.notify_error_jobs(conn, 3, started)
        self.assertEqual(self.mails, [])
        # このサイクルで投入したジョブだけを対象にする
        sql, params = conn.executed[0]
        self.assertIn("status = 'error'", sql)
        self.assertIn("created_at >= ?", sql)
        self.assertEqual(params, (started,))

    def test_one_error_job_is_notified_with_breakdown(self):
        rows = [
            (11, "fetch_active_ebay", "ラクマ", "P1", RETRY_OUT_503),
            (12, "fetch_active_ebay", "ラクマ", "P2", RETRY_OUT_503),
            (13, "fetch_sold_ebay", "ラクマ", "P3", NO_RETRY_403),
            (14, "fetch_active_ebay", "メルカリ", "P4", "Traceback\nValueError: boom\n"),
        ]
        il.notify_error_jobs(FakeSelectConn(rows), 3, datetime(2026, 10, 28, 1, 0))
        self.assertEqual(len(self.mails), 1)
        subject, body = self.mails[0]
        self.assertIn("4 件", subject)
        self.assertIn("2 件  fetch_active_ebay / ラクマ / [再試行を使い切った retry=6/6] HTTP 503", body)
        self.assertIn("1 件  fetch_sold_ebay / ラクマ / [再試行対象外 retry=0/6] HTTP 403", body)
        self.assertIn("1 件  fetch_active_ebay / メルカリ / ValueError: boom", body)
        self.assertIn("job_id=11 fetch_active_ebay / ラクマ / P1", body)

        # 1件だけでも通知する
        self.mails.clear()
        il.notify_error_jobs(FakeSelectConn(rows[:1]), 4, datetime(2026, 10, 28, 5, 0))
        self.assertEqual(len(self.mails), 1)

    def test_examples_are_limited(self):
        rows = [(i, "fetch_active_ebay", "ラクマ", f"P{i}", RETRY_OUT_503) for i in range(50)]
        il.notify_error_jobs(FakeSelectConn(rows), 1, datetime(2026, 10, 28))
        body = self.mails[0][1]
        self.assertIn("50 件  fetch_active_ebay", body)
        self.assertIn("job_id=19 ", body)
        self.assertNotIn("job_id=20 ", body)

    def test_error_reason(self):
        self.assertEqual(
            il.error_reason(TB_503),
            "requests.exceptions.HTTPError: 503 Server Error: Service Temporarily Unavailable",
        )
        self.assertEqual(il.error_reason(RETRY_OUT_503), "[再試行を使い切った retry=6/6] HTTP 503")
        self.assertEqual(il.error_reason(None), "(エラー内容なし)")

    def test_wait_includes_retry_wait_jobs(self):
        # 再試行待ちのジョブが残っている間は次の工程へ進まない
        self.assertIn("'retry_wait'", il.SQL_COUNT_OUTSTANDING_JOBS)
        for st in ("'pending'", "'pending2'", "'running'"):
            self.assertIn(st, il.SQL_COUNT_OUTSTANDING_JOBS)


if __name__ == "__main__":
    unittest.main()
