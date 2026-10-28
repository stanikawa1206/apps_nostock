# -*- coding: utf-8 -*-
"""
ワーカーのハング対策と watchdog のジョブ回収のテスト（モックのみ。DB・ブラウザ・メルカリには接続しない）。
  - watchdog: 終了したワーカーの statusファイルによる担当の確定・回収、ブラウザ応答待ちの見張り、起動時のワーカー数
  - scrape_worker: ブラウザ応答待ちファイルの作成・削除、ブラウザ起動設定の統一

実行: python -m unittest apps.tests.test_watchdog_recovery -v
"""

import os
import sys
import tempfile
import time
import unittest
from datetime import datetime
from unittest import mock

# watchdog・scrape_worker は import 時にログ・状態ファイルの置き場所を作るため、一時フォルダに向けて import する
_TMP = tempfile.TemporaryDirectory()
_stdout, _stderr = sys.stdout, sys.stderr
with mock.patch.dict(os.environ, {"WORKER_STATUS_DIR": _TMP.name, "WORKER_NAME": "test-worker"}):
    from apps.inventory import watchdog as wd
    from apps.inventory import scrape_worker as sw
sys.stdout, sys.stderr = _stdout, _stderr
sw._log_file.close()

HOST = wd.HOST_WORKER_NAME
LOCKED_AT = datetime(2026, 10, 28, 12, 57, 59)
T_LOCK = wd.locked_at_epoch(LOCKED_AT)


class FakeCursor:
    def __init__(self, row):
        self.row = row

    def execute(self, *args):
        pass

    def fetchone(self):
        return self.row

    def close(self):
        pass


class FakeConn:
    def __init__(self, row):
        self.row = row

    def cursor(self):
        return FakeCursor(self.row)

    def commit(self):
        pass

    def close(self):
        pass


class FakeProc:
    def __init__(self, pid, create_time, name="python", children=()):
        self.pid = pid
        self._create_time = create_time
        self._name = name
        self._children = list(children)
        self.killed = False

    def create_time(self):
        return self._create_time

    def name(self):
        return self._name

    def children(self, recursive=False):
        return self._children

    def kill(self):
        self.killed = True


def write_status_file(pid, job_id, page, mtime):
    path = os.path.join(_TMP.name, f"worker_status_{pid}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"pid={pid}\njob_id={job_id}\npage={page}\nupdated_at=x\n")
    os.utime(path, (mtime, mtime))
    return path


# ============================================================
# 終了したワーカーの statusファイルによる回収（process_file）
# ============================================================
class ProcessFileTest(unittest.TestCase):
    """
    今回の事例（2026-10-28 job=4374660）: 固まった1183064 を kill した後、同じホストの待機中ワーカー（statusファイルなし）が
    unknown のため回収が16分保留された。書き手のファイルが現在のロック後にこのジョブを指していれば、すぐ回収する。
    """

    HUNG, IDLE, NEW = 1183064, 1243452, 1373417

    def run_process_file(self, *, row, file_mtime, live, files=None, kill_ok=True, writer_alive=True):
        path = write_status_file(self.HUNG, 4374660, 1, file_mtime)
        hung = FakeProc(self.HUNG, file_mtime - 3600)
        state = {"killed": False}

        def get_proc(pid):
            if pid == self.HUNG and writer_alive and not state["killed"]:
                return hung
            return None

        def kill(proc):
            state["killed"] = kill_ok
            return kill_ok

        requeued = []
        with mock.patch.object(wd, "get_live_worker_process", side_effect=get_proc), \
                mock.patch.object(wd, "kill_and_confirm", side_effect=kill), \
                mock.patch.object(wd, "list_live_workers", return_value=live), \
                mock.patch.object(wd, "read_status_files", return_value=files or {}), \
                mock.patch.object(wd, "open_db", return_value=FakeConn(row)), \
                mock.patch.object(wd, "requeue_job", side_effect=lambda c, j, l, p: requeued.append((j, l, p)) or 1), \
                mock.patch.object(wd, "rlog"):
            wd.process_file(path)
        return requeued, os.path.exists(path), state["killed"]

    def running_row(self, worker=HOST):
        return ("running", worker, LOCKED_AT)

    def test_hung_writer_killed_and_idle_unknown_worker_is_requeued(self):
        # 待機中ワーカー（ロック前から起動・statusファイルなし）= unknown、新ワーカー（ロック後起動）= not_owner
        live = {self.IDLE: T_LOCK - 50000, self.NEW: T_LOCK + 600}
        requeued, exists, killed = self.run_process_file(
            row=self.running_row(), file_mtime=T_LOCK + 2, live=live)
        self.assertTrue(killed)
        self.assertEqual(requeued, [(4374660, LOCKED_AT, 1)])
        self.assertFalse(exists)

    def test_writer_already_gone_is_requeued(self):
        live = {self.IDLE: T_LOCK - 50000}
        requeued, _, killed = self.run_process_file(
            row=self.running_row(), file_mtime=T_LOCK + 2, live=live, writer_alive=False)
        self.assertFalse(killed)
        self.assertEqual(len(requeued), 1)

    def test_live_owner_blocks_requeue(self):
        # 別の生存ワーカーの statusファイルがこのジョブを指している（担当中）→ 戻さない
        live = {self.IDLE: T_LOCK - 50000}
        files = {self.IDLE: {"pid": self.IDLE, "job_id": 4374660, "page": 2, "mtime": time.time()}}
        requeued, exists, _ = self.run_process_file(
            row=self.running_row(), file_mtime=T_LOCK + 2, live=live, files=files)
        self.assertEqual(requeued, [])
        self.assertFalse(exists)  # 従来どおり statusファイルのみ削除

    def test_file_written_before_current_lock_is_held(self):
        # ファイルは前回のロックのもの（ジョブはその後に取り直された）→ 書き手は担当と確定できない → 従来どおり保留
        live = {self.IDLE: T_LOCK - 50000}
        requeued, exists, _ = self.run_process_file(
            row=self.running_row(), file_mtime=T_LOCK - 10, live=live)
        self.assertEqual(requeued, [])
        self.assertTrue(exists)

    def test_kill_not_confirmed_is_held(self):
        live = {self.IDLE: T_LOCK - 50000, self.HUNG: T_LOCK - 3600}
        requeued, exists, _ = self.run_process_file(
            row=self.running_row(), file_mtime=T_LOCK + 2, live=live, kill_ok=False)
        self.assertEqual(requeued, [])
        self.assertTrue(exists)

    def test_other_host_or_not_running_is_untouched(self):
        live = {self.IDLE: T_LOCK - 50000}
        for row in (self.running_row(worker="x-other-host"), ("done", HOST, LOCKED_AT), ("pending", None, None), None):
            requeued, exists, _ = self.run_process_file(row=row, file_mtime=T_LOCK + 2, live=live)
            self.assertEqual(requeued, [], row)
            self.assertFalse(exists, row)

    def test_all_not_owner_still_requeued_as_before(self):
        live = {self.NEW: T_LOCK + 600}
        requeued, _, _ = self.run_process_file(row=self.running_row(), file_mtime=T_LOCK + 2, live=live)
        self.assertEqual(len(requeued), 1)


class OwnerProvedByFileTest(unittest.TestCase):
    def info(self, mtime):
        return {"pid": 1, "job_id": 9, "page": 1, "mtime": mtime}

    def test_conditions(self):
        f = wd.owner_proved_by_file
        self.assertTrue(f(self.info(T_LOCK), T_LOCK, True, {2: "unknown"}))
        self.assertTrue(f(self.info(T_LOCK - 1.5), T_LOCK, True, {}))       # locked_at の秒丸め分は許容
        self.assertFalse(f(self.info(T_LOCK - 3), T_LOCK, True, {}))         # ロックより前のファイル
        self.assertFalse(f(self.info(T_LOCK), T_LOCK, False, {}))            # 書き手がまだ生きている
        self.assertFalse(f(self.info(T_LOCK), T_LOCK, True, {2: "owner"}))   # 担当中の生存ワーカーがいる


# ============================================================
# ブラウザ応答待ちの見張り（check_browser_waits）
# ============================================================
class CheckBrowserWaitsTest(unittest.TestCase):
    def setUp(self):
        wd._browser_killed.clear()
        for name in os.listdir(_TMP.name):
            if name.startswith("worker_browserwait_"):
                os.remove(os.path.join(_TMP.name, name))

    def write_wait(self, pid, started):
        path = os.path.join(_TMP.name, f"worker_browserwait_{pid}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"pid={pid}\njob_id=77\nstarted={started:.3f}\n")
        return path

    def run_check(self, procs):
        with mock.patch.object(wd, "get_live_worker_process", side_effect=lambda pid: procs.get(pid)), \
                mock.patch.object(wd.psutil, "wait_procs", side_effect=lambda ps, timeout: (ps, [])), \
                mock.patch.object(wd, "rlog"):
            wd.check_browser_waits()

    def worker(self, pid, started):
        chrome = FakeProc(pid + 1, started, "chrome-headless-shell")
        renderer = FakeProc(pid + 2, started, "chrome-headless-shell")
        driver = FakeProc(pid + 3, started - 10, "node")
        return FakeProc(pid, started - 100, "python3", [driver, chrome, renderer]), chrome, renderer, driver

    def test_hang_kills_only_that_workers_browser_once(self):
        now = time.time()
        hung, chrome, renderer, driver = self.worker(100, now - 95)
        ok, ok_chrome, ok_renderer, ok_driver = self.worker(200, now - 5)
        self.write_wait(100, now - 95)
        self.write_wait(200, now - 5)
        procs = {100: hung, 200: ok}
        self.run_check(procs)
        self.assertTrue(chrome.killed and renderer.killed)
        self.assertFalse(driver.killed)                      # Playwright の driver は終了させない
        self.assertFalse(hung.killed)                        # ワーカー本体も終了させない
        self.assertFalse(ok_chrome.killed or ok_renderer.killed or ok_driver.killed or ok.killed)

        # 同じ応答待ちに対しては繰り返さない（戻らなければ statusファイル更新停止で従来どおりワーカーごと終了）
        chrome.killed = renderer.killed = False
        self.run_check(procs)
        self.assertFalse(chrome.killed or renderer.killed)

    def test_below_threshold_is_untouched(self):
        now = time.time()
        proc, chrome, _, _ = self.worker(100, now - 89)
        self.write_wait(100, now - 89)
        self.run_check({100: proc})
        self.assertFalse(chrome.killed)

    def test_file_of_dead_or_reused_pid_is_removed_without_kill(self):
        now = time.time()
        dead = self.write_wait(300, now - 500)
        reused_proc, reused_chrome, _, _ = self.worker(400, now)  # ファイルより後に起動した同じ pid
        reused = self.write_wait(400, now - 500)
        self.run_check({400: reused_proc})
        self.assertFalse(os.path.exists(dead))
        self.assertFalse(os.path.exists(reused))
        self.assertFalse(reused_chrome.killed)

    def test_worker_browser_processes_filters_chrome(self):
        proc, chrome, renderer, driver = self.worker(100, time.time())
        self.assertEqual(wd.worker_browser_processes(proc), [chrome, renderer])


# ============================================================
# 起動時のワーカー数（watchdog だけを再起動した場合）
# ============================================================
class StartupTest(unittest.TestCase):
    class Stop(Exception):
        pass

    def run_main(self, running):
        with mock.patch.object(wd, "count_process", return_value=running), \
                mock.patch.object(wd, "start_worker") as start, \
                mock.patch.object(wd.time, "sleep"), \
                mock.patch.object(wd.glob, "glob", side_effect=self.Stop):
            with self.assertRaises(self.Stop):
                wd.main()
        return start.call_count

    def test_starts_only_missing_workers(self):
        self.assertEqual(self.run_main(0), 2)
        self.assertEqual(self.run_main(1), 1)
        self.assertEqual(self.run_main(2), 0)


# ============================================================
# scrape_worker: ブラウザ応答待ちファイル・起動設定
# ============================================================
class FakeResp:
    status = 200

    def __init__(self, json_fn):
        self.request = "req"
        self._json = json_fn

    def json(self):
        return self._json()


class FakeExpect:
    def __init__(self, resp):
        self.value = resp

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakePage:
    def __init__(self, json_fn, seen):
        self.resp = FakeResp(json_fn)
        self.seen = seen

    def expect_response(self, pred, timeout):
        return FakeExpect(self.resp)

    def goto(self, url, wait_until, timeout):
        # 取得中は応答待ちファイルがある
        self.seen.append(os.path.exists(sw.BROWSER_WAIT_FILE))


class BrowserWaitFileTest(unittest.TestCase):
    def test_file_exists_only_during_fetch(self):
        seen = []
        page = FakePage(lambda: {"items": []}, seen)
        out = sw._fetch_page_json_attempts(page, "u", {"req"}, job_id=5)
        self.assertEqual(out, {"items": []})
        self.assertEqual(seen, [True])
        self.assertFalse(os.path.exists(sw.BROWSER_WAIT_FILE))

    def test_file_removed_after_browser_broken(self):
        seen = []

        def broken():
            raise RuntimeError("Target page, context or browser has been closed")
        page = FakePage(broken, seen)
        with self.assertRaises(RuntimeError) as cm:
            sw._fetch_page_json_attempts(page, "u", {"req"}, job_id=5)
        self.assertEqual(str(cm.exception), "BROWSER_BROKEN")
        self.assertEqual(seen, [True, True])                 # 2回の試行それぞれでファイルを置く
        self.assertFalse(os.path.exists(sw.BROWSER_WAIT_FILE))

    def test_wait_file_format_read_by_watchdog(self):
        sw.begin_browser_wait(42)
        try:
            info = wd.parse_browser_wait_file(sw.BROWSER_WAIT_FILE)
        finally:
            sw.end_browser_wait()
        self.assertEqual(info["pid"], os.getpid())
        self.assertEqual(info["job_id"], "42")
        self.assertLess(abs(info["started"] - time.time()), 5)


class LaunchBrowserTest(unittest.TestCase):
    def test_same_args_every_launch(self):
        p = mock.Mock()
        sw.launch_browser(p)
        sw.launch_browser(p)
        for call in p.chromium.launch.call_args_list:
            self.assertEqual(call.kwargs, {"headless": True, "args": sw.BROWSER_LAUNCH_ARGS})
        self.assertIn("--disable-dev-shm-usage", sw.BROWSER_LAUNCH_ARGS)


def tearDownModule():
    # 一時フォルダを消せるよう、watchdog の回収ログを閉じる
    for h in list(wd._recovery_logger.handlers):
        h.close()
        wd._recovery_logger.removeHandler(h)


if __name__ == "__main__":
    unittest.main()
