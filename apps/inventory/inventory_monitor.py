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

あわせて、UNPUBLISHED再公開待ち（trx.unpublished_recovery）のうち再処理の時刻が来たものを再公開し、
未復旧が多い・複数アカウント・長期未復旧・期限切れの時だけ1日1回まとめて通知する（2026-11-09〜）。
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection, send_mail
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


def check_inventory_loop() -> None:
    alive_pids = {pid for pid, _, _ in find_running_scripts(("inventory_loop.py",))}
    incident = detect_incident(read_status(), alive_pids, datetime.now())
    notified_key = load_notified_key()

    if incident is None:
        if notified_key:
            _logger.info(f"異常が解消しました（{notified_key}）")
            save_notified_key(None)
        return

    key, subject, body = incident
    if key == notified_key:
        return

    _logger.info(f"{subject}（{key}）→ メール")
    if send_mail(subject, body):
        save_notified_key(key)
    else:
        # 送れなかった場合は記録せず、次回（10分後）に再送する
        _logger.info("メール送信に失敗しました。次回再送します")


# ===== UNPUBLISHED再公開待ち（trx.unpublished_recovery）の再処理と集約通知 =====
# 出品処理（毎日3:00〜）の時間外にも、再処理の時刻が来た保留を再公開する（取り出しは publish_ebay と共通で1か所だけ）。
# 一時的な失敗の個別メールは送らず、次のどれかに当たる時だけ1日1回まとめて通知する。
ORPHAN_NOTIFY_MIN_ROWS = 5            # 未復旧（pending・running・gave_up）がこの件数以上
ORPHAN_NOTIFY_MIN_ACCOUNTS = 2        # 未復旧のアカウントがこの数以上
ORPHAN_NOTIFY_LONG_DAYS = 3           # 保留にしてからこの日数以上 再公開できていない
ORPHAN_NOTIFY_STATE_PATH = PROJECT_ROOT / "logs" / "unpublished_recovery_notify.json"
# publish_ebay.ORPHAN_RETRY_STALE_MIN と同じ値（再処理の対象が無い時に publish_ebay を import しないため、ここにも持つ）
ORPHAN_RETRY_STALE_MIN = 30

SQL_ORPHAN_DUE_ACCOUNTS = """
SELECT DISTINCT account FROM trx.unpublished_recovery
 WHERE (status = 'pending' AND next_at <= SYSDATETIME())
    OR (status = 'running' AND updated_at < DATEADD(MINUTE, ?, SYSDATETIME()))
"""
SQL_ORPHAN_OPEN_ROWS = """
SELECT account, vendor_item_id, status, attempts, since_utc, next_at, last_error,
       DATEDIFF(MINUTE, since_utc, SYSUTCDATETIME()) / 1440.0 AS days
  FROM trx.unpublished_recovery
 WHERE status IN ('pending', 'running', 'gave_up')
 ORDER BY since_utc
"""


def retry_due_orphan_republishes(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SQL_ORPHAN_DUE_ACCOUNTS, -ORPHAN_RETRY_STALE_MIN)
        accounts = [r[0] for r in cur.fetchall()]
    conn.commit()
    if not accounts:
        return
    from apps.publish.publish_ebay import retry_pending_orphan_republishes
    for account in accounts:
        _logger.info(f"UNPUBLISHED再公開待ちを再処理します account={account}")
        retry_pending_orphan_republishes(conn, account)


def orphan_alert(rows) -> list:
    """未復旧の行から、通知する理由の一覧を返す（無ければ空）。"""
    reasons = []
    if len(rows) >= ORPHAN_NOTIFY_MIN_ROWS:
        reasons.append(f"未復旧が{len(rows)}件（{ORPHAN_NOTIFY_MIN_ROWS}件以上）")
    accounts = sorted({r["account"] for r in rows})
    if len(accounts) >= ORPHAN_NOTIFY_MIN_ACCOUNTS:
        reasons.append(f"複数アカウントで発生（{'・'.join(accounts)}）")
    long_rows = [r for r in rows if (r["days"] or 0) >= ORPHAN_NOTIFY_LONG_DAYS]
    if long_rows:
        reasons.append(f"{ORPHAN_NOTIFY_LONG_DAYS}日以上 未復旧が{len(long_rows)}件")
    gave_up = [r for r in rows if r["status"] == "gave_up"]
    if gave_up:
        reasons.append(f"再試行の期限切れ(gave_up)が{len(gave_up)}件")
    return reasons


def notify_orphan_alert(conn, today: str) -> None:
    with conn.cursor() as cur:
        cur.execute(SQL_ORPHAN_OPEN_ROWS)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.commit()
    reasons = orphan_alert(rows)
    if not reasons:
        return
    state = json.loads(ORPHAN_NOTIFY_STATE_PATH.read_text(encoding="utf-8")) if ORPHAN_NOTIFY_STATE_PATH.exists() else {}
    if state.get("notified_date") == today:
        return
    lines = [
        f"- {r['account']} {r['vendor_item_id']} status={r['status']} 試行{r['attempts']}回 "
        f"保留{float(r['days'] or 0):.1f}日 次回={r['next_at']:%m/%d %H:%M} 最終エラー={(r['last_error'] or '')[:150]}"
        for r in rows
    ]
    subject = f"【要確認】UNPUBLISHED再公開待ちが復旧していません（{len(rows)}件）"
    body = (
        "publishOffer のエラーで作り直した出品のうち、再公開できていないもの（eBayに出品なし）があります。\n"
        f"通知の理由: {' / '.join(reasons)}\n\n" + "\n".join(lines) + "\n\n"
        "再試行は 30分・2時間・6時間後、以後1日ごとに自動で続けます（同じ offer で 25001 が3回続くか、2日失敗したら offer を作り直す。"
        "14日で gave_up）。この通知は条件に当たる間、1日1回送ります。\n"
        "gave_up を再処理するには status='pending', next_at=SYSDATETIME() に戻してください。ログ: logs/unpublished_recovery.log\n"
    )
    _logger.info(f"{subject} 理由={reasons} → メール")
    if send_mail(subject, body):
        ORPHAN_NOTIFY_STATE_PATH.write_text(json.dumps({"notified_date": today}, ensure_ascii=False), encoding="utf-8")


def check_unpublished_recovery() -> None:
    conn = get_sql_server_connection()
    try:
        retry_due_orphan_republishes(conn)
        notify_orphan_alert(conn, datetime.now().strftime("%Y-%m-%d"))
    finally:
        conn.close()


def main() -> int:
    check_inventory_loop()
    # 在庫管理の監視（上）とは別の処理のため、こちらの失敗で上の監視を止めない（次回10分後に再実行）
    try:
        check_unpublished_recovery()
    except Exception:
        _logger.exception("UNPUBLISHED再公開待ちの確認でエラー")
    return 0


if __name__ == "__main__":
    sys.exit(main())
