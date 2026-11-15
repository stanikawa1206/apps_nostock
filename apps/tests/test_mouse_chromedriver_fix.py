# -*- coding: utf-8 -*-
"""
2026-11-15 の mouse の出品 Renderer異常（11/2〜15 の7回）の対策のテスト。
  原因: mouse の check_remaining_ebay.bat が、check_remaining の1回ごとに
        taskkill /F /IM chromedriver.exe /T を実行し、同じPCの出品(publish_ebay)の chromedriver も終了させていた。
  1. check_remaining は終了時に「この実行が起動したプロセスだけ」を片付ける（process_watchdog.kill_own_descendants）。
     bat の taskkill は廃止。
  2. 出品は chromedriver への接続拒否のとき、プロセスを終了せず Selenium の driver を作り直して続ける（安全策）。

本物のプロセスを使うテストは、python の子プロセス・名前を chromedriver.exe にした別プロセス（Windows のみ）を作り、
最後に自分で片付ける。DB は test_account_listing_block と同じテスト専用DB（本番DBには触れない）。eBay には接続しない。

実行: python -m unittest apps.tests.test_mouse_chromedriver_fix -v
"""

import contextlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import psutil

from apps.common import process_watchdog as pw
from apps.publish import publish_ebay as pe
from apps.tests.test_account_listing_block import DbTestBase, _fake_sync_playwright

# check_remaining_ebay は import 時に stdout をログへ差し替えるので、元に戻す（test_yahoo_fleamarket と同じ）
_o, _e = sys.stdout, sys.stderr
from apps.inventory import check_remaining_ebay as cr  # noqa: E402
sys.stdout, sys.stderr = _o, _e

REPO = Path(__file__).resolve().parents[2]
BAT = REPO / "apps" / "inventory" / "check_remaining_ebay.bat"

WIN_REFUSED = (
    "HTTPConnectionPool(host='localhost', port=63481): Max retries exceeded with url: "
    "/session/f26bc72a12e72e5f07abffa0f943b37f/url (Caused by NewConnectionError('<urllib3.connection."
    "HTTPConnection object at 0x000001D8ADBE3AD0>: Failed to establish a new connection: [WinError 10061] "
    "対象のコンピューターによって拒否されたため、接続できませんでした。'))"
)
LINUX_REFUSED = (
    "HTTPConnectionPool(host='localhost', port=40123): Max retries exceeded with url: /session/x/url "
    "(Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7f>: "
    "Failed to establish a new connection: [Errno 111] Connection refused'))"
)
READ_TIMEOUT = (
    "HTTPConnectionPool(host='localhost', port=32771): Max retries exceeded with url: /session/8f/source "
    "(Caused by ReadTimeoutError(\"HTTPConnectionPool(host='localhost', port=32771): Read timed out. "
    "(read timeout=120)\"))"
)


def _alive(pid):
    try:
        p = psutil.Process(pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _sleeper(seconds=120):
    return subprocess.Popen([sys.executable, "-c", f"import time; time.sleep({seconds})"])


# ============================================================
# 1. process_watchdog.kill_own_descendants（本物のプロセス）
# ============================================================
class KillOwnDescendantsTest(unittest.TestCase):
    def test_kills_only_own_tree(self):
        # 兄弟（このテストの子＝ helper から見て他人）。出品の publish_ebay に相当する。
        sibling = _sleeper()
        self.addCleanup(sibling.kill)
        helper_code = (
            "import subprocess, sys, time, json\n"
            "from apps.common.process_watchdog import kill_own_descendants\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import subprocess,sys,time;"
            "subprocess.Popen([sys.executable,\"-c\",\"import time;time.sleep(120)\"]);time.sleep(120)'])\n"
            "time.sleep(3)\n"
            "killed = kill_own_descendants()\n"
            "print(json.dumps(sorted(p.pid for p in killed)), flush=True)\n"
        )
        out = subprocess.run([sys.executable, "-c", helper_code], cwd=REPO, capture_output=True, text=True,
                             timeout=60, env={**os.environ, "PYTHONPATH": str(REPO)})
        self.assertEqual(out.returncode, 0, out.stderr)
        killed = [int(x) for x in out.stdout.strip().strip("[]").split(",") if x.strip()]
        self.assertEqual(len(killed), 2)  # helper の子と孫
        time.sleep(1)
        for pid in killed:
            self.assertFalse(_alive(pid), pid)
        self.assertTrue(_alive(sibling.pid))  # 他人のプロセスは残る

    def test_exit_variant_uses_same_scope(self):
        with mock.patch.object(pw, "kill_own_descendants", return_value=[]) as k, \
                mock.patch.object(pw.os, "_exit") as ex, mock.patch.object(pw, "write_exit_report"), \
                mock.patch("builtins.print"):
            pw.kill_own_descendants_and_exit("test", 2)
        k.assert_called_once()
        ex.assert_called_once_with(2)


# ============================================================
# 1. check_remaining_ebay: 終了時に自分の子孫だけ片付ける
# ============================================================
class CheckRemainingCleanupTest(unittest.TestCase):
    def test_cleanup_runs_on_normal_end(self):
        class _PW:
            chromium = mock.MagicMock()

        @contextlib.contextmanager
        def _fake_pw():
            yield _PW()

        with mock.patch.object(cr, "get_sql_server_connection"), \
                mock.patch.object(cr, "sync_playwright", _fake_pw), \
                mock.patch.object(cr, "playwright_driver_monitor", contextlib.nullcontext), \
                mock.patch.object(cr, "start_watchdog", return_value=mock.Mock()), \
                mock.patch.object(cr, "pull_remaining_targets", return_value=[]), \
                mock.patch.object(cr, "count_total_remaining", return_value=0), \
                mock.patch.object(cr, "kill_own_descendants", return_value=[]) as k, \
                mock.patch("builtins.print"):
            with self.assertRaises(SystemExit) as cm:
                cr.run_remaining_worker("test")
        self.assertEqual(cm.exception.code, 0)
        k.assert_called_once()

    def test_bat_has_no_taskkill_by_name(self):
        text = BAT.read_bytes().decode("utf-8")
        self.assertIn("\r\n", text)  # cmd 用に CRLF のまま
        for line in text.splitlines():
            stripped = line.strip().lower()
            if stripped.startswith("::") or stripped.startswith("rem "):
                continue
            self.assertNotIn("taskkill", stripped, line)


@unittest.skipUnless(os.name == "nt", "Windows の bat の実行テスト")
class BatRunTest(unittest.TestCase):
    """新しい bat を実際に動かし、名前が chromedriver.exe の他のプロセスが残ることを確かめる。"""

    def test_other_chromedriver_survives_bat_loop(self):
        tmp = Path(tempfile.mkdtemp(prefix="crbat_"))
        self.addCleanup(shutil.rmtree, tmp, True)
        # 他の処理の chromedriver の代わり（ping.exe を chromedriver.exe という名前で動かす）
        fake = tmp / "chromedriver.exe"
        shutil.copy(r"C:\Windows\System32\PING.EXE", fake)
        other = subprocess.Popen([str(fake), "-n", "120", "127.0.0.1"], stdout=subprocess.DEVNULL)
        self.addCleanup(other.kill)

        # bat が起動する check_remaining_ebay の代わり: 1回目は exit 1（100件ごとの再起動と同じ経路）、2回目は exit 0
        shutil.copy(BAT, tmp / "check_remaining_ebay.bat")
        (tmp / "check_remaining_ebay.py").write_text(
            "import os, sys\n"
            "p = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'count.txt')\n"
            "n = int(open(p).read()) if os.path.exists(p) else 0\n"
            "open(p, 'w').write(str(n + 1))\n"
            "print('dummy run', n + 1, flush=True)\n"
            "sys.exit(1 if n == 0 else 0)\n",
            encoding="utf-8",
        )
        # フルパスで呼ぶ（NoDefaultCurrentDirectoryInExePath が設定された環境では、カレントの bat を探さないため）
        res = subprocess.run(["cmd", "/c", str(tmp / "check_remaining_ebay.bat")], cwd=tmp, capture_output=True,
                             timeout=90, stdin=subprocess.DEVNULL)
        out = res.stdout.decode("cp932", errors="replace")
        self.assertEqual((tmp / "count.txt").read_text(), "2", out)  # 再起動してもう1回動いた
        self.assertIn("SUCCESS", out)
        self.assertTrue(_alive(other.pid), out)  # 以前の bat ならここで終了させられていた


# ============================================================
# 2. publish_ebay.heavy_check_detail（chromedriver の接続拒否）
# ============================================================
def call_heavy(vendor_name, conn=None):
    conn = conn or mock.MagicMock()
    return pe.heavy_check_detail(
        conn, mock.MagicMock(), "https://example/x", "SKU1", "preset", vendor_name, "mode",
        "brand", "1", "d", "t", "財布", {}, 0, 1, 2, mock.MagicMock(), 1,
    ), conn


class HeavyCheckSeleniumTest(unittest.TestCase):
    def setUp(self):
        self.upsert = mock.patch.object(pe, "upsert_vendor_item").start()
        mock.patch.object(pe, "playwright_driver_is_dead", return_value=False).start()
        mock.patch("builtins.print").start()
        self.addCleanup(mock.patch.stopall)

    def test_refused_records_parse_failure_and_signals(self):
        for msg in (WIN_REFUSED, LINUX_REFUSED):
            self.upsert.reset_mock()
            conn = mock.MagicMock()
            with mock.patch.object(pe, "parse_detail_shops", side_effect=Exception(msg)):
                with self.assertRaises(pe.SeleniumDriverDeadError):
                    call_heavy("メルカリshops", conn)
            self.assertEqual(self.upsert.call_args.args[1]["listing_head"], "解析失敗")
            conn.commit.assert_called()  # 記録を確定してから知らせる

    def test_other_fatal_cases_unchanged(self):
        for msg, pw_dead in ((READ_TIMEOUT, False), (WIN_REFUSED, True)):
            with self.subTest(msg=msg[:50], pw_dead=pw_dead), \
                    mock.patch.object(pe, "parse_detail_shops", side_effect=Exception(msg)), \
                    mock.patch.object(pe, "playwright_driver_is_dead", return_value=pw_dead):
                with self.assertRaises(pe.FatalRendererError):
                    call_heavy("メルカリshops")

    def test_normal_error_unchanged(self):
        with mock.patch.object(pe, "parse_detail_shops", side_effect=Exception("価格要素が見つかりません")):
            res, _ = call_heavy("メルカリshops")
        self.assertIsNone(res[0])
        self.assertEqual(self.upsert.call_args.args[1]["listing_head"], "解析失敗")

    def test_detection(self):
        self.assertTrue(pe.is_selenium_driver_connection_refused(Exception(WIN_REFUSED)))
        self.assertTrue(pe.is_selenium_driver_connection_refused(Exception(LINUX_REFUSED)))
        self.assertFalse(pe.is_selenium_driver_connection_refused(Exception(READ_TIMEOUT)))
        self.assertFalse(pe.is_selenium_driver_connection_refused(Exception("chrome not reachable")))
        self.assertTrue(pe.is_fatal_renderer_error(Exception(WIN_REFUSED)))  # 変更前はこれで exit 1


# ============================================================
# 2. publish_ebay.main: chromedriver が死んでも作り直して続ける
# ============================================================
class SeleniumRecoverMainTest(DbTestBase):
    PC = "testpc"

    def test_driver_recreated_and_run_finishes_normally(self):
        self.sql("INSERT INTO mst.ebay_accounts (account, preset_group, post_target, is_excluded, "
                 "fulfillment_policy_id, payment_policy_id, return_policy_id) VALUES (N'貴文', N'貴文', 1, 0, '1', '2', '3')")
        self.sql("INSERT INTO mst.execute_pcs (execute_pc, account, is_active) VALUES (?, NULL, 1)", self.PC)
        self.conn.commit()
        skus = iter(["S1", "S2"])

        def _take(conn, preset_group, processing_by, account):
            sku = next(skus, None)
            if sku is None:
                return None
            return {"vendor_item_id": sku, "price": 1, "shipping_days": None, "vendor_name": "メルカリshops",
                    "preset": "p", "mode": None, "default_brand_en": "b", "category_id_ebay": "1",
                    "department": "d", "type_ebay": "t", "category_group": "財布", "low_jpy_target": 1,
                    "high_jpy_target": 2, "item_condition_id": 1}

        drivers_seen = []

        def _heavy(conn, page, url, sku, *a, **k):
            drivers_seen.append(a[-2])  # driver 引数
            if sku == "S1":
                raise pe.SeleniumDriverDeadError(WIN_REFUSED)
            return ({"sku": sku}, None, 0, None, None)

        def _post(**kw):
            c = pe.get_sql_server_connection()
            try:
                c.cursor().execute("INSERT INTO trx.listings (account, start_time, is_deleted) VALUES (?, GETDATE(), 0)",
                                   kw["acct"])
                c.commit()
            finally:
                c.close()
            kw["acct_success"][kw["acct"]] += 1
            return (kw["acct_targets"], kw["acct_success"], kw["total_listings"], False, 0,
                    0, "NORMAL", 0, None, False)

        made = [mock.MagicMock(name="driver1"), mock.MagicMock(name="driver2")]
        made[0].quit.side_effect = Exception(WIN_REFUSED)  # 死んだ driver の quit は失敗する
        patches = [
            mock.patch.object(pe.socket, "gethostname", return_value=self.PC),
            mock.patch.object(pe, "set_exit_report"),
            mock.patch.object(pe, "set_exit_context"),
            mock.patch.object(pe, "start_watchdog", return_value=mock.Mock()),
            mock.patch.object(pe.boto3, "client"),
            mock.patch.object(pe, "fetch_active_presets", return_value=[]),
            mock.patch.object(pe, "sync_playwright", _fake_sync_playwright),
            mock.patch.object(pe, "playwright_driver_monitor", contextlib.nullcontext),
            mock.patch.object(pe, "take_one_vendor_item", side_effect=_take),
            mock.patch.object(pe, "heavy_check_detail", side_effect=_heavy),
            mock.patch.object(pe, "post_to_ebay", side_effect=_post),
            mock.patch.object(pe.webdriver, "Chrome", side_effect=made),
            mock.patch.object(pe, "kill_own_descendants_and_exit", side_effect=AssertionError("exit してはいけない")),
            mock.patch("builtins.print"),
        ]
        with contextlib.ExitStack() as st:
            for p in patches:
                st.enter_context(p)
            with self.assertRaises(SystemExit) as cm:
                pe.main()
        self.assertEqual(cm.exception.code, 10)  # 正常終了（変更前は exit 1）
        self.assertEqual(drivers_seen, [made[0], made[1]])  # 2件目は作り直した driver
        made[0].quit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
