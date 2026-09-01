# apps/inventory/ebay_watchdog.py
# -*- coding: utf-8 -*-
#
# 【このプログラムの役割】
# EbayOrderFetch(fetch_orders_ebay.pyの常駐タスク)が実行中かどうかを確認し、
# 停止していれば Start-ScheduledTask で再起動する監視スクリプト。
#
# 💡 Windowsタスクスケジューラの「失敗時に再起動」設定(RestartOnFailure)は、
# 実機検証の結果、プロセスの強制終了・非ゼロ終了コードのいずれでも再起動が
# 発動しないことを確認した(2026-08-25検証)。そのため、このスクリプトを
# タスクスケジューラから2分間隔で起動し、代替の再起動機構として使う。
#
# これは「動いているか確認して、止まっていれば再起動するだけ」の最小限の
# 仕組みであり、停止が続いた場合のメール通知等(死活監視)はここには含まれない
# (別途の対応として保留中)。
#
import logging
import subprocess
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(r"D:\apps_nostock\logs")
LOG_FILE = LOG_DIR / "ebay_watchdog.log"
TASK_NAME = "EbayOrderFetch"

# 💡 subprocess.run()でpowershell.exeを呼ぶと、既定では青い背景のコンソール
# ウィンドウが一瞬表示されてしまう(2分毎に発生し目障りだったため2026-08-25に対応)。
# CREATE_NO_WINDOWを指定して子プロセスのウィンドウ自体を作らせないようにする。
_NO_WINDOW = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("ebay_watchdog")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            str(LOG_FILE), maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except Exception:
        logger.addHandler(logging.NullHandler())
    return logger


log = _setup_logging()


def get_task_state() -> str:
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command",
         f"(Get-ScheduledTask -TaskName '{TASK_NAME}').State"],
        capture_output=True, text=True, timeout=30,
        creationflags=_NO_WINDOW,
    )
    return result.stdout.strip()


def start_task() -> None:
    subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command",
         f"Start-ScheduledTask -TaskName '{TASK_NAME}'"],
        capture_output=True, text=True, timeout=30,
        creationflags=_NO_WINDOW,
    )


def main():
    state = get_task_state()
    if state == "Running":
        return  # 正常稼働中。2分毎に毎回記録するとログが肥大化するため何も残さない
    log.warning(f"{TASK_NAME} が実行中ではありません(状態: {state!r})。再起動します。")
    start_task()
    log.info(f"{TASK_NAME} の再起動を試みました。")


if __name__ == "__main__":
    main()
