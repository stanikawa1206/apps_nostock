# -*- coding: utf-8 -*-
"""
ローカル publish_ebay の終了検知・担当行の解放と、監視による自己終了の記録のテスト。
DB・eBay・ブラウザには接続しない（DBはモック。プロセスの kill は、このテスト自身が起動した python だけが対象）。

  - publish_manager.check_local_publish_exit: 生存中は解放しない／終了確認後だけ解放／別の publish_ebay が居れば解放しない
  - publish_manager.monitor_limit_accounts: 2026-10-31 の再現（終了済みローカルの行が残っていても出品枠確保が始まる）、
    実際に処理中のworkerが居る間は出品枠確保（古い出品の削除）が始まらない
  - process_watchdog: 30分監視・Renderer異常の自己終了で、終了記録（理由・処理中のSKU・止まっていた場所）が残り、
    自分の子孫だけが終了し、無関係のプロセスは残る。終了コードは従来どおり（VPSの再起動ループの判定が変わらない）
  - publish_ebay: 画面出力のファイル保存（os._exit 直前の行も残る）、Renderer異常の経路が子孫を終了させる

実行: python -m unittest apps.tests.test_publish_local_exit -v
"""

import inspect
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest import mock

import psutil

from apps.publish import publish_manager as pm

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FakePopen:
    def __init__(self, returncode=None, pid=99999):
        self.returncode = returncode
        self.pid = pid

    def poll(self):
        return self.returncode


class CheckLocalPublishExitTest(unittest.TestCase):
    def setUp(self):
        self.logs = []
        patches = [
            mock.patch.object(pm, "_pm_log", self.logs.append),
            mock.patch.object(pm, "find_local_publish_ebay_pids", return_value=[]),
            mock.patch.object(pm, "release_local_execute_pc", return_value="川島"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_alive_process_is_not_released(self):
        proc = FakePopen(returncode=None)
        self.assertIs(pm.check_local_publish_exit(proc), proc)
        pm.release_local_execute_pc.assert_not_called()

    def test_abnormal_exit_releases_and_logs_exit_code(self):
        self.assertIsNone(pm.check_local_publish_exit(FakePopen(returncode=2)))
        pm.release_local_execute_pc.assert_called_once()
        self.assertTrue(any("exit_code=2" in line for line in self.logs))
        self.assertTrue(any("released_account=川島" in line for line in self.logs))

    def test_normal_exit_also_checks_release(self):
        pm.release_local_execute_pc.return_value = None  # publish_ebay 自身が解放済み
        self.assertIsNone(pm.check_local_publish_exit(FakePopen(returncode=10)))
        pm.release_local_execute_pc.assert_called_once()
        self.assertTrue(any("exit_code=10" in line for line in self.logs))

    def test_other_publish_ebay_running_is_not_released(self):
        pm.find_local_publish_ebay_pids.return_value = [12345]
        self.assertIsNone(pm.check_local_publish_exit(FakePopen(returncode=1)))
        pm.release_local_execute_pc.assert_not_called()

    def test_handled_only_once(self):
        self.assertIsNone(pm.check_local_publish_exit(FakePopen(returncode=2)))
        self.assertIsNone(pm.check_local_publish_exit(None))
        pm.release_local_execute_pc.assert_called_once()

class FindLocalPublishEbayTest(unittest.TestCase):
    def test_find_local_publish_ebay_pids_detects_both_launch_styles(self):
        procs = [
            mock.Mock(info={"pid": 1, "name": "python.exe", "cmdline": ["python", r"D:\apps_nostock\apps\publish\publish_ebay.py"]}),
            mock.Mock(info={"pid": 2, "name": "python3", "cmdline": ["python3", "-m", "publish_ebay"]}),
            mock.Mock(info={"pid": 3, "name": "python.exe", "cmdline": ["python", r"D:\apps_nostock\apps\publish\publish_manager.py"]}),
            mock.Mock(info={"pid": 4, "name": "chrome.exe", "cmdline": ["chrome.exe", "publish_ebay.py"]}),
        ]
        with mock.patch.object(pm.psutil, "process_iter", return_value=procs):
            self.assertEqual(pm.find_local_publish_ebay_pids(), [1, 2])


class FakeDb:
    """mst.execute_pcs / mst.ebay_accounts の状態だけを持つ模擬DB。"""

    def __init__(self, execute_pcs, close_reasons):
        self.execute_pcs = dict(execute_pcs)
        self.close_reasons = dict(close_reasons)

    def get_limit_accounts(self):
        result = []
        for account, reason in self.close_reasons.items():
            if reason == "LIMIT":
                workers = sum(1 for a in self.execute_pcs.values() if a == account)
                result.append({"account": account, "post_target": 350, "active_workers": workers})
        return result

    def release_local(self):
        account = self.execute_pcs.get(pm.LOCAL_EXECUTE_PC)
        self.execute_pcs[pm.LOCAL_EXECUTE_PC] = None
        return account

    def clear_close_status(self, account):
        self.close_reasons[account] = None


class MonitorLoopTest(unittest.TestCase):
    """monitor_limit_accounts を数サイクル回し、出品枠確保（古い出品の削除）が始まる条件を確かめる。"""

    def run_loops(self, db, local_proc, loops, on_loop=None, other_local_pids=()):
        self.space_requests = []
        counter = {"n": 0}

        def is_all_finished():
            counter["n"] += 1
            if on_loop:
                on_loop(counter["n"], db)
            return counter["n"] >= loops

        def request_listing_space(account, remaining):
            self.space_requests.append((account, remaining))
            return remaining

        with mock.patch.object(pm, "_pm_log", lambda m: None), \
             mock.patch.object(pm, "get_limit_accounts", db.get_limit_accounts), \
             mock.patch.object(pm, "release_local_execute_pc", db.release_local), \
             mock.patch.object(pm, "find_local_publish_ebay_pids", return_value=list(other_local_pids)), \
             mock.patch.object(pm, "calculate_remaining_count", return_value=208), \
             mock.patch.object(pm, "request_listing_space", request_listing_space), \
             mock.patch.object(pm, "clear_close_status", db.clear_close_status), \
             mock.patch.object(pm, "auto_fix_done_accounts", return_value=[]), \
             mock.patch.object(pm, "is_all_accounts_finished", is_all_finished), \
             mock.patch.object(pm.time, "sleep", lambda s: None):
            pm.monitor_limit_accounts(local_proc)

    def test_incident_20261031_stale_local_row_is_released(self):
        # mouse が川島を担当したまま異常終了(exit 2)、その後 VPS が川島で LIMIT を書いた状態
        db = FakeDb({pm.LOCAL_EXECUTE_PC: "川島", "x162-43-42-135": None}, {"川島": "LIMIT"})
        self.run_loops(db, FakePopen(returncode=2), loops=3)
        self.assertEqual(self.space_requests, [("川島", 208)])  # 1回だけ確保（二重削除なし）
        self.assertIsNone(db.execute_pcs[pm.LOCAL_EXECUTE_PC])
        self.assertIsNone(db.close_reasons["川島"])

    def test_before_fix_behavior_would_wait_forever(self):
        # 参考: 終了を見張らない（従来の main）と、同じ状態で何サイクル回っても確保は始まらない
        db = FakeDb({pm.LOCAL_EXECUTE_PC: "川島"}, {"川島": "LIMIT"})
        self.run_loops(db, None, loops=5)
        self.assertEqual(self.space_requests, [])

    def test_live_local_worker_blocks_listing_space(self):
        # ローカルが実際に川島を処理中（生存中）の間は、古い出品の削除を始めない
        db = FakeDb({pm.LOCAL_EXECUTE_PC: "川島"}, {"川島": "LIMIT"})
        self.run_loops(db, FakePopen(returncode=None), loops=5)
        self.assertEqual(self.space_requests, [])
        self.assertEqual(db.execute_pcs[pm.LOCAL_EXECUTE_PC], "川島")

    def test_live_vps_worker_blocks_listing_space_after_local_exit(self):
        # ローカルは終了済みでも、VPSがまだ川島を処理中ならその終了まで待つ（3サイクル目でVPSが手放す）
        db = FakeDb({pm.LOCAL_EXECUTE_PC: "川島", "x162-43-42-135": "川島"}, {"川島": "LIMIT"})

        def on_loop(n, db):
            if n == 3:
                db.execute_pcs["x162-43-42-135"] = None

        requests_by_loop = []

        def on_loop_recording(n, db):
            requests_by_loop.append(len(self.space_requests))
            on_loop(n, db)

        self.run_loops(db, FakePopen(returncode=1), loops=6, on_loop=on_loop_recording)
        self.assertEqual(requests_by_loop[:3], [0, 0, 0])
        self.assertEqual(self.space_requests, [("川島", 208)])

    def test_other_local_publish_ebay_keeps_row(self):
        # このPCで別の publish_ebay（手動起動など）が動いていれば、行を消さないので確保も始まらない
        db = FakeDb({pm.LOCAL_EXECUTE_PC: "川島"}, {"川島": "LIMIT"})
        self.run_loops(db, FakePopen(returncode=1), loops=4, other_local_pids=[4321])
        self.assertEqual(self.space_requests, [])
        self.assertEqual(db.execute_pcs[pm.LOCAL_EXECUTE_PC], "川島")

    def test_local_waiting_in_limit_loop_does_not_block(self):
        # ローカルがLIMIT待機中（生存・担当なし）の時は、従来どおりすぐ確保する
        db = FakeDb({pm.LOCAL_EXECUTE_PC: None}, {"川島": "LIMIT"})
        self.run_loops(db, FakePopen(returncode=None), loops=2)
        self.assertEqual(self.space_requests, [("川島", 208)])


class RunLocalTest(unittest.TestCase):
    def test_run_local_returns_popen_and_passes_log_path(self):
        with mock.patch.object(pm.subprocess, "Popen", return_value=FakePopen(pid=777)) as popen, \
             mock.patch.object(pm, "_pm_log", lambda m: None):
            proc = pm.run_local()
        self.assertEqual(proc.pid, 777)
        env = popen.call_args.kwargs["env"]
        self.assertRegex(env["PUBLISH_EBAY_LOG_PATH"], r"publish_ebay_local_\d{8}\.log$")
        self.assertEqual(popen.call_args.args[0][1], str(pm.PUBLISH_SCRIPT))


# ======================
# 監視による自己終了（実プロセス）
# ======================
SLEEPER = [sys.executable, "-c", "import time; time.sleep(120)"]

CHILD_SCRIPT = textwrap.dedent("""
    import subprocess, sys, time
    sys.path.insert(0, {root!r})
    from apps.common import process_watchdog as w

    if {report!r}:
        w.set_exit_report({report!r})
    w.set_exit_context(account="川島", sku="2JYr9irHmhLmXi58BfJp4E")

    # 子（chromedriver 役）と、その子（Chrome 役）
    child = subprocess.Popen([sys.executable, "-c",
        "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)']); time.sleep(120)"])
    time.sleep(1.5)
    print("CHILDREN", " ".join(str(c.pid) for c in w.psutil.Process().children(recursive=True)), flush=True)

    def stuck_in_shops_detail():
        time.sleep(120)

    if {mode!r} == "watchdog":
        w.start_watchdog(1.0, "1商品の処理が1.0秒以内に終わりませんでした SKU=2JYr9irHmhLmXi58BfJp4E")
        stuck_in_shops_detail()
    else:
        w.kill_own_descendants_and_exit("Renderer異常 account=川島 SKU=2JYr9irHmhLmXi58BfJp4E", 1)
""")


class SelfExitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # このテストが起動した、無関係のプロセス（利用者の Chrome などの代わり）
        self.outsider = subprocess.Popen(SLEEPER)
        self.addCleanup(self.outsider.kill)

    def run_child(self, mode, report=True):
        report_path = str(Path(self.tmp.name) / "publish_ebay_exit.log") if report else ""
        script = CHILD_SCRIPT.format(root=str(PROJECT_ROOT), report=report_path, mode=mode)
        r = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                           encoding="utf-8", timeout=60, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
        line = next(l for l in r.stdout.splitlines() if l.startswith("CHILDREN"))
        pids = [int(x) for x in line.split()[1:]]
        return r, pids, report_path

    def assert_all_dead(self, pids):
        deadline = time.time() + 5
        while time.time() < deadline and any(psutil.pid_exists(p) for p in pids):
            time.sleep(0.2)
        self.assertFalse([p for p in pids if psutil.pid_exists(p)])

    def test_watchdog_exit_records_where_it_was_stuck(self):
        r, pids, report_path = self.run_child("watchdog")
        self.assertEqual(r.returncode, 2)  # VPS の再起動ループの判定（0/10以外は再起動）は従来どおり
        self.assertEqual(len(pids), 2)
        self.assert_all_dead(pids)
        self.assertIsNone(self.outsider.poll())  # 無関係のプロセスは残る
        report = Path(report_path).read_text(encoding="utf-8")
        self.assertIn("exit_code=2", report)
        self.assertIn("SKU=2JYr9irHmhLmXi58BfJp4E", report)
        self.assertIn("'account': '川島'", report)
        self.assertIn("--- thread MainThread", report)
        main_stack = report.split("--- thread MainThread", 1)[1].split("--- thread", 1)[0]
        self.assertIn("stuck_in_shops_detail", main_stack)  # 止まっていた関数が分かる

    def test_renderer_exit_kills_own_descendants_only(self):
        r, pids, report_path = self.run_child("renderer")
        self.assertEqual(r.returncode, 1)
        self.assert_all_dead(pids)  # 以前は chromedriver・Chrome 役が残っていた
        self.assertIsNone(self.outsider.poll())
        self.assertIn("exit_code=1 reason=Renderer異常", Path(report_path).read_text(encoding="utf-8"))

    def test_without_exit_report_nothing_is_written(self):
        # check_remaining_ebay など、記録先を設定しないプロセスは従来どおり
        r, pids, _ = self.run_child("renderer", report=False)
        self.assertEqual(r.returncode, 1)
        self.assert_all_dead(pids)
        self.assertEqual(os.listdir(self.tmp.name), [])


class PublishEbayTest(unittest.TestCase):
    def test_console_log_keeps_lines_before_os_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "publish_ebay_local_20261031.log"
            script = textwrap.dedent(f"""
                import os, sys
                sys.path.insert(0, {str(PROJECT_ROOT)!r})
                from apps.publish.publish_ebay import tee_console_to_file
                tee_console_to_file({str(log_path)!r})
                print("🚀 アカウント開始: 川島 (Target: 350)")
                print("[DEBUG] before goto (shops)", file=sys.stderr)
                os._exit(2)  # flush せずに終える（監視による自己終了と同じ）
            """)
            r = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                               encoding="utf-8", timeout=120, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
            self.assertEqual(r.returncode, 2)
            text = log_path.read_text(encoding="utf-8")
            self.assertIn("publish_ebay 開始", text)
            self.assertIn("アカウント開始: 川島", text)
            self.assertIn("before goto (shops)", text)

    def test_console_output_still_reaches_console(self):
        with tempfile.TemporaryDirectory() as tmp:
            script = textwrap.dedent(f"""
                import sys
                sys.path.insert(0, {str(PROJECT_ROOT)!r})
                from apps.publish.publish_ebay import tee_console_to_file
                tee_console_to_file({str(Path(tmp) / "x.log")!r})
                print("🚀 アカウント開始: 川島 (Target: 350)")
                sys.exit(10)
            """)
            r = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                               encoding="utf-8", timeout=120, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
            self.assertEqual(r.returncode, 10)
            self.assertIn("アカウント開始: 川島", r.stdout)

    def test_fatal_renderer_paths_use_descendant_kill(self):
        from apps.publish import publish_ebay as pe
        src = inspect.getsource(pe.main)
        code_lines = [l for l in src.splitlines() if not l.strip().startswith("#")]
        self.assertFalse([l for l in code_lines if "os._exit(" in l])
        self.assertEqual(src.count("kill_own_descendants_and_exit("), 2)
        self.assertIn("set_exit_report(EXIT_REPORT_PATH)", src)


if __name__ == "__main__":
    unittest.main()
