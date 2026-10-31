# -*- coding: utf-8 -*-
"""
Playwright を使う常駐処理（publish_ebay / check_remaining_ebay）のゾンビ化防止。

処理を進められなくなったプロセスを、監視スレッドから確実に終了させる。
方式は x162-43-39-209 のカナリア(a305457, mercari_item_status._on_evaluate_timeout)と同じ:
このプロセスの子孫(Playwright driver・Chromium・chromedriver・Chrome)を kill してから
os._exit() する。メインスレッドは Playwright の同期呼び出しの中で空回り・ブロックしている
可能性があるため、その状態には依存しない。exit code は 0/10 以外なので、VPS の
publish_ebay_loop.sh / check_remaining_ebay.sh が自分の実行だけを片付けて再起動する。

・playwright_driver_monitor(): Playwright driver(node) が死んだら自己終了する
  （driver 死亡後は sync API の2回目以降の呼び出しが例外も出さず CPU100% で永久に空回りする。
   Playwright 1.58 で再現確認済み。2026-07-10 に x210-131-209-232 で81日間残ったゾンビの原因候補）
・start_watchdog(): 指定秒数以内に cancel() されなければ自己終了する（1件ごとの処理の上限）
"""
from __future__ import annotations

import os
import socket
import sys
import threading
import traceback
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import psutil

# Playwright driver(node) の生存確認間隔（秒）。driver 死亡から自己終了までの最大遅延になる。
PLAYWRIGHT_DRIVER_POLL_SEC = 5

# playwright_driver_monitor() が見つけた、このプロセスの Playwright driver(node) プロセス
_playwright_driver_proc: Optional[psutil.Process] = None

# 自己終了の記録先と、記録に添える「その時に処理していたもの」（set_exit_report / set_exit_context で設定）。
# 記録先を設定していないプロセス（check_remaining_ebay）は従来どおり何も書かない。
_exit_report_path: Optional[Path] = None
_exit_context: dict = {}


def set_exit_report(path) -> None:
    """自己終了の理由・終了コード・全スレッドのスタックを path に追記するようにする。"""
    global _exit_report_path
    _exit_report_path = Path(path)
    _exit_report_path.parent.mkdir(parents=True, exist_ok=True)


def set_exit_context(**fields) -> None:
    """自己終了の記録に添える処理中の情報（担当アカウント・SKU など）を差し替える。"""
    _exit_context.clear()
    _exit_context.update(fields)


def write_exit_report(reason: str, exit_code: int) -> None:
    """
    自己終了の直前に、理由・終了コード・処理中の情報と、全スレッドのスタックを記録する。
    スタックのうち MainThread が、監視が発動した時点でメインの処理が止まっていた場所になる。
    os._exit() は finally もログの後処理も通らないため、ここで書いて fsync まで済ませる。
    記録に失敗しても自己終了は妨げない。
    """
    if _exit_report_path is None:
        return
    try:
        names = {t.ident: t.name for t in threading.enumerate()}
        lines = [
            f"===== {datetime.now():%Y-%m-%d %H:%M:%S} host={socket.gethostname()} pid={os.getpid()} "
            f"exit_code={exit_code} reason={reason}",
            f"context: {_exit_context}",
        ]
        for ident, frame in sys._current_frames().items():
            lines.append(f"--- thread {names.get(ident, '?')} (ident={ident})")
            lines.append("".join(traceback.format_stack(frame)).rstrip("\n"))
        with open(_exit_report_path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        print(f"[WATCHDOG] 終了記録の書き込みに失敗しました: {e!r}", flush=True)


def kill_own_descendants_and_exit(reason: str, exit_code: int) -> None:
    write_exit_report(reason, exit_code)
    print(f"[WATCHDOG] {reason} → 子プロセスを強制終了して exit {exit_code}", flush=True)
    # 対象はこのプロセスから起動された子孫だけ（psutil は親より後に作られたプロセスだけを子とみなすため、
    # PID が再利用された無関係のプロセスや、利用者が開いている Chrome などは含まれない）。
    children = psutil.Process(os.getpid()).children(recursive=True)
    for child in children:
        try:
            child.kill()
        except psutil.NoSuchProcess:
            pass
    print(f"[WATCHDOG] 子プロセス{len(children)}件にkillを送信しました", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(exit_code)


def playwright_driver_is_dead() -> bool:
    proc = _playwright_driver_proc
    if proc is None:
        return False
    try:
        return proc.status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True


@contextmanager
def playwright_driver_monitor():
    """
    sync_playwright() の直後に入れ（with sync_playwright() as p, playwright_driver_monitor():）、
    このプロセスの Playwright driver(node) が死んだら PLAYWRIGHT_DRIVER_POLL_SEC 秒以内に
    exit 1 で自己終了させる。with を抜ける時（sync_playwright の正常終了で driver を止める前）に
    監視を止めるので、正常終了の経路では発火しない。
    """
    global _playwright_driver_proc
    drivers = [
        c for c in psutil.Process(os.getpid()).children()
        if "run-driver" in " ".join(c.cmdline())
    ]
    if len(drivers) != 1:
        raise RuntimeError(f"Playwright driver プロセスを特定できません: {[c.pid for c in drivers]}")
    _playwright_driver_proc = drivers[0]
    stop = threading.Event()

    def watch():
        while not stop.wait(PLAYWRIGHT_DRIVER_POLL_SEC):
            if playwright_driver_is_dead() and not stop.is_set():
                kill_own_descendants_and_exit(
                    f"Playwright driver(pid={_playwright_driver_proc.pid})が終了しました", 1
                )

    threading.Thread(target=watch, name="playwright-driver-monitor", daemon=True).start()
    try:
        yield
    finally:
        stop.set()
        _playwright_driver_proc = None


def start_watchdog(timeout_sec: float, reason: str, exit_code: int = 2) -> threading.Timer:
    """timeout_sec 秒以内に返り値の cancel() が呼ばれなければ、子孫を kill して exit_code で自己終了する。"""
    timer = threading.Timer(timeout_sec, kill_own_descendants_and_exit, args=(reason, exit_code))
    timer.daemon = True
    timer.start()
    return timer
