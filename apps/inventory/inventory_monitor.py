# -*- coding: utf-8 -*-
"""
inventory_monitor.py

inventory_loop.py（在庫管理）の稼働状態を確認し、異常停止していればメールする。
常駐はしない。タスクスケジューラから10分ごとに1回ずつ起動する。

判定（inventory_loop の状態ファイル logs/inventory_loop_status.json とプロセスの有無）:
  - 稼働中のはずなのにプロセスが無い        → 通知（ウィンドウを閉じた・PC再起動など）
  - プロセスはあるが状態ファイルが更新されない → 通知（応答なし）
  - 異常終了したが inventory_loop 自身のメール送信に失敗していた → 通知
  - 停止フラグ・Ctrl+C による停止、未起動    → 通知しない
同じ異常は1回だけ通知する（logs/inventory_monitor_state.json に記録）。
異常が解消したら記録を消すため、その後にもう一度止まれば再び通知する。
進捗（残り件数）の停止は inventory_loop 自身が通知する。
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import send_mail
from apps.inventory.inventory_loop import find_running_scripts, read_status

# 状態ファイルは完了待ちの間30秒ごとに更新される。ジョブ投入スクリプト（数秒〜数分）より十分長くとる
HEARTBEAT_STALE_MINUTES = 30

MONITOR_STATE_PATH = PROJECT_ROOT / "logs" / "inventory_monitor_state.json"
LOG_PATH = PROJECT_ROOT / "logs" / "inventory_monitor.log"

_logger = logging.getLogger("inventory_monitor")
if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    _handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _logger.addHandler(_handler)
    _logger.propagate = False


def detect_incident(status, alive_pids, now: datetime):
    """
    異常があれば (異常のキー, 件名, 本文) を返す。キーは同じ異常を2回通知しないために使う。
    """
    if status is None or status.get("state") == "stopped":
        return None

    started = status.get("started_at")
    detail = (
        f"PID: {status.get('pid')}\n起動: {started}\nサイクル: {status.get('cycle_no')}\n"
        f"工程: {status.get('phase')}\n残り件数: {status.get('outstanding')}\n"
        f"状態ファイルの最終更新: {status.get('updated_at')}\n"
    )

    if status.get("state") == "error":
        if status.get("error_notified"):
            return None
        return (
            f"error:{started}",
            "【異常】在庫管理が異常終了しました",
            f"在庫管理が異常終了しています（inventory_loop からのメール送信は失敗していました）。\n"
            f"停止時刻: {status.get('stopped_at')}\n理由: {status.get('stop_reason')}\n\n{detail}",
        )

    pid = status.get("pid")
    if pid not in alive_pids:
        return (
            f"dead:{pid}:{started}",
            "【異常】在庫管理のプロセスが停止しています",
            "状態ファイルは稼働中のままですが、inventory_loop.py のプロセスがありません"
            "（ウィンドウを閉じた・PC再起動などで終了した可能性）。\n"
            "在庫管理は自動では再開しません。確認してから手動で起動してください。\n\n" + detail,
        )

    updated_at = datetime.fromisoformat(status["updated_at"])
    if (now - updated_at).total_seconds() >= HEARTBEAT_STALE_MINUTES * 60:
        return (
            f"stale:{pid}:{started}",
            "【要確認】在庫管理が応答していません",
            f"inventory_loop.py のプロセスはありますが、状態ファイルが {HEARTBEAT_STALE_MINUTES} 分以上"
            "更新されていません（ジョブ投入スクリプト等で止まっている可能性）。\n\n" + detail,
        )
    return None


def load_notified_key():
    if not MONITOR_STATE_PATH.exists():
        return None
    return json.loads(MONITOR_STATE_PATH.read_text(encoding="utf-8")).get("notified_key")


def save_notified_key(key):
    MONITOR_STATE_PATH.write_text(json.dumps({"notified_key": key}, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    alive_pids = {pid for pid, _, _ in find_running_scripts(("inventory_loop.py",))}
    incident = detect_incident(read_status(), alive_pids, datetime.now())
    notified_key = load_notified_key()

    if incident is None:
        if notified_key:
            _logger.info(f"異常が解消しました（{notified_key}）")
            save_notified_key(None)
        return 0

    key, subject, body = incident
    if key == notified_key:
        return 0

    _logger.info(f"{subject}（{key}）→ メール")
    if send_mail(subject, body):
        save_notified_key(key)
    else:
        # 送れなかった場合は記録せず、次回（10分後）に再送する
        _logger.info("メール送信に失敗しました。次回再送します")
    return 0


if __name__ == "__main__":
    sys.exit(main())
