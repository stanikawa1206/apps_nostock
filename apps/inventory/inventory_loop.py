# -*- coding: utf-8 -*-
"""
inventory_loop.py

在庫管理だけを連続実行する常駐スクリプト（出品処理とは独立。出品は publish_daily.py が毎日 3:00 に行う）。

1サイクル = 簡易: fetch_sold → 完全: fetch_active → fetch_sold → check_remaining
  - fetch_active は出品中SKUの vendor_item.status を NULL に戻してから検索するため、
    完全側の fetch_sold は必ず fetch_active の後に行う（旧 daily_check の在庫管理1回分と同じ順序）。
  - 各工程は「ジョブ（または remaining 対象）が0件になる」まで待ってから次へ進む。固定時間で打ち切らない。
    fetch_sold / fetch_active は pending だけでなく running と retry_wait（ラクマの503などで再試行待ち）の
    ジョブも終わるまで待つ。再試行は scrape_worker 側で回数の上限があるため、無期限には待たない。
  - サイクル開始時に presets_lookup を作り直す（ブランド・カテゴリーの変更を反映）。
  - サイクル終了時だけ mst.control『日次処理停止』を確認し、1 なら NULL に戻して正常終了する。
    自動再開はしない（再開は手動起動）。

監視:
  - 進捗（残りジョブ数・remaining 残件数）が STALL_ALERT_MINUTES 変化しなければメール（回復時も1通）。
    工程の所要時間そのものでは判定しない。
  - 工程の失敗・例外で終了する場合はメールしてから異常終了する。
  - サイクル終了時、このサイクルで投入したジョブに error（再試行でも回復しなかったもの）が
    1件でもあれば、件数と内訳をメールする（在庫管理は止めない）。
  - 状態を logs/inventory_loop_status.json に書き出す（inventory_monitor.py が10分ごとに確認し、
    プロセスの消失・応答なしを通知する）。
  - fetch_sold / fetch_active / check_remaining の正常終了メールは送らない（ログ logs/inventory_loop.log に記録）。
"""

import json
import logging
import os
import sys
import time
import traceback
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import psutil

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import send_mail
from apps.inventory.daily_check import (
    FETCH_ACTIVE,
    FETCH_SOLD,
    ReconnectingConnection,
    disable_console_quick_edit,
    format_child_error,
    launch_remaining_workers,
    notify_no_preset_listings,
    notify_undetermined_remaining,
    refresh_presets_lookup,
    reset_remaining_flags,
    run_script,
    should_stop_daily_check,
)

# 進捗（残件数）がこの時間変化しなければ停止の疑いとして通知する。
# 実測（2026-10-27）: ジョブ1件の最長は fetch_active 約35分、fetch_sold 約2分。
STALL_ALERT_MINUTES = 60
POLL_SECONDS = 30
# 残件数が変わらなくても、この間隔で待機状況をログに残す
LOG_EVERY_MINUTES = 10

STATUS_PATH = PROJECT_ROOT / "logs" / "inventory_loop_status.json"
LOG_PATH = PROJECT_ROOT / "logs" / "inventory_loop.log"

# 同時に動いてはいけないスクリプト（旧方式の daily_check は在庫管理を含むため）
INVENTORY_CONFLICT_SCRIPTS = ("inventory_loop.py", "daily_check.py")

SQL_COUNT_OUTSTANDING_JOBS = """
SELECT COUNT(*)
FROM trx.scrape_job
WHERE status IN ('pending', 'pending2', 'running', 'retry_wait')
"""

SQL_SELECT_ERROR_JOBS = """
SELECT job_id, job_kind, JSON_VALUE(job_payload, '$.vendor_name'), JSON_VALUE(job_payload, '$.preset'), error_message
FROM trx.scrape_job
WHERE status = 'error'
  AND created_at >= ?
ORDER BY job_id
"""

# エラー通知メールに載せるジョブの例の件数
ERROR_JOB_EXAMPLES = 20

_logger = logging.getLogger("inventory_loop")
if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    _handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _logger.addHandler(_handler)
    _logger.propagate = False


def log(msg: str):
    print(msg, flush=True)
    _logger.info(msg)


def _now() -> datetime:
    return datetime.now()


class StepFailed(Exception):
    """ジョブ投入スクリプトの異常終了。在庫管理を異常終了させる。"""


# ======================
# 二重起動の確認
# ======================
def find_running_scripts(script_names, exclude_pid=None):
    """
    指定したスクリプト名（ファイル名）で動いている python プロセスを返す。
    「python path\\daily_check.py」と「python -m apps.inventory.daily_check」の両方の起動方法を判定する。
    戻り値: [(pid, script_name, 起動時刻)]
    """
    names = {n.lower() for n in script_names}
    found = []
    for p in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
        if p.info["pid"] == exclude_pid:
            continue
        if not (p.info["name"] or "").lower().startswith("python"):
            continue
        cmdline = p.info["cmdline"] or []
        for i, arg in enumerate(cmdline):
            if i > 0 and cmdline[i - 1] == "-m":
                base = arg.rsplit(".", 1)[-1].lower() + ".py"
            else:
                base = os.path.basename(arg).lower()
            if base in names:
                found.append((p.info["pid"], base, datetime.fromtimestamp(p.info["create_time"])))
                break
    return found


# ======================
# 状態ファイル
# ======================
def write_status(**fields):
    """状態ファイルを更新する（渡した項目だけ上書きし、updated_at を付ける）。"""
    status = read_status() or {}
    status.update(fields)
    status["updated_at"] = _now().isoformat(timespec="seconds")
    tmp = STATUS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, STATUS_PATH)


def read_status():
    if not STATUS_PATH.exists():
        return None
    return json.loads(STATUS_PATH.read_text(encoding="utf-8"))


# ======================
# 完了待ち（進捗停止の検知つき）
# ======================
def count_outstanding_jobs(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(SQL_COUNT_OUTSTANDING_JOBS)
        return cur.fetchone()[0]


def count_remaining_targets(conn) -> int:
    # check_remaining のワーカーが「全件終了」と判断するのと同じ条件（ロック中の行も数える）。
    # check_remaining_ebay は Playwright 等を読み込むため、使う時だけ import する。
    from apps.inventory.check_remaining_ebay import count_total_remaining
    return count_total_remaining(conn)


def wait_until_zero(conn, phase: str, count_fn) -> int:
    """
    count_fn(conn) が0になるまで待つ。戻り値は待ち始めの件数。
    件数が STALL_ALERT_MINUTES 変化しなければ1回だけメールし、その後に件数が動いたら回復メールを送る。
    """
    initial = conn.run(count_fn)
    last_count = initial
    last_change = _now()
    last_log = last_change
    stall_notified = False
    log(f"[{phase}] 完了待ち開始: 残り {initial} 件")

    while True:
        count = conn.run(count_fn)
        now = _now()

        if count != last_count:
            if stall_notified:
                msg = (
                    f"在庫管理の工程「{phase}」の進捗が再開しました。\n"
                    f"停止していた時間: {last_change:%m/%d %H:%M} 〜 {now:%m/%d %H:%M}\n"
                    f"残り: {last_count} 件 → {count} 件\n"
                )
                log(f"[{phase}] 進捗再開: {last_count} → {count} 件")
                send_mail(f"【回復】在庫管理の進捗が再開しました（{phase}）", msg)
                stall_notified = False
            last_count = count
            last_change = now

        write_status(phase=phase, outstanding=count, last_progress_at=last_change.isoformat(timespec="seconds"))

        if count == 0:
            log(f"[{phase}] 完了（開始時 {initial} 件）")
            return initial

        if now - last_log >= timedelta(minutes=LOG_EVERY_MINUTES):
            log(f"[{phase}] 待機中: 残り {count} 件（最後に件数が動いたのは {last_change:%H:%M}）")
            last_log = now

        if not stall_notified and now - last_change >= timedelta(minutes=STALL_ALERT_MINUTES):
            msg = (
                f"在庫管理の工程「{phase}」で、残り件数が {STALL_ALERT_MINUTES} 分以上変化していません。\n"
                f"残り: {count} 件（{last_change:%m/%d %H:%M} から変化なし）\n"
                "scrape_worker / watchdog / check_remaining の各ワーカーが動いているか確認してください。\n"
                "在庫管理は待機を続けます（打ち切りません）。進捗が再開したら回復メールを送ります。\n"
            )
            log(f"[{phase}] 進捗停止の疑い: 残り {count} 件が {last_change:%H:%M} から変化なし → メール")
            send_mail(f"【要確認】在庫管理の進捗が停止しています（{phase}）", msg)
            stall_notified = True

        time.sleep(POLL_SECONDS)


# ======================
# 工程
# ======================
def run_scrape_step(conn, script: Path, phase: str):
    """ジョブを投入し、投入済みジョブ（pending / running）が無くなるまで待つ。"""
    started = _now()
    write_status(phase=phase, phase_started_at=started.isoformat(timespec="seconds"), outstanding=None)
    log(f"[{phase}] 開始: {script.name}")

    code, err = run_script(script)
    if code != 0:
        raise StepFailed(
            f"{script.name}（{phase}）のジョブ投入が異常終了しました（returncode={code}）\n\n"
            + format_child_error(err)
        )

    jobs = wait_until_zero(conn, phase, count_outstanding_jobs)
    log(f"[{phase}] 終了: ジョブ {jobs} 件 / 所要 {_now() - started}")


def run_remaining_step(conn, phase: str):
    """remaining フラグを初期化してワーカーを起動し、対象が無くなるまで待つ。"""
    started = _now()
    write_status(phase=phase, phase_started_at=started.isoformat(timespec="seconds"), outstanding=None)
    log(f"[{phase}] 開始: check_remaining")

    conn.run(reset_remaining_flags)
    launch_remaining_workers()

    # 待機中に例外で抜けた場合も、ここまでに出た判定不可は必ず通知する
    try:
        targets = wait_until_zero(conn, phase, count_remaining_targets)
    finally:
        conn.run(notify_undetermined_remaining, started)

    conn.run(notify_no_preset_listings)
    log(f"[{phase}] 終了: 対象 {targets} 件 / 所要 {_now() - started}")


def error_reason(error_message) -> str:
    """error_message（traceback や RAKUMA_HTTP の説明）から、内訳に使う短い理由を取り出す。"""
    lines = [ln.strip() for ln in (error_message or "").splitlines() if ln.strip()]
    if not lines:
        return "(エラー内容なし)"
    last = lines[-1]
    if " RAKUMA_HTTP status=" in last:
        # 例: "[再試行を使い切った retry=6/6] RAKUMA_HTTP status=503 retry_after=..." → 区分とステータスだけ
        head, rest = last.split(" RAKUMA_HTTP status=", 1)
        return f"{head} HTTP {rest.split()[0]}"
    return last.split(" for url:")[0][:150]


def notify_error_jobs(conn, cycle_no: int, cycle_started: datetime):
    """このサイクルで投入したジョブのうち error で終わったものがあれば、件数と内訳をメールする。"""
    with conn.cursor() as cur:
        cur.execute(SQL_SELECT_ERROR_JOBS, cycle_started)
        rows = cur.fetchall()
    if not rows:
        return

    breakdown = Counter((kind, vendor, error_reason(msg)) for _, kind, vendor, _, msg in rows)
    summary = "\n".join(
        f"  {n} 件  {kind} / {vendor} / {reason}" for (kind, vendor, reason), n in breakdown.most_common()
    )
    examples = "\n".join(
        f"  job_id={job_id} {kind} / {vendor} / {preset}" for job_id, kind, vendor, preset, _ in rows[:ERROR_JOB_EXAMPLES]
    )
    log(f"[サイクル {cycle_no}] error のジョブ {len(rows)} 件 → メール")
    send_mail(
        f"【要確認】在庫管理で取得できなかったジョブがあります（{len(rows)} 件）",
        f"サイクル {cycle_no}（{cycle_started:%m/%d %H:%M} 開始）で投入したジョブのうち、"
        f"{len(rows)} 件が error で終わりました（再試行でも回復しなかったもの）。\n"
        "これらのプリセットは今回のサイクルで検索結果を取得できていません。"
        "次のサイクルで改めて取得します（在庫管理は継続しています）。\n\n"
        f"内訳（件数 / ジョブ種別 / 仕入先 / 理由）:\n{summary}\n\n"
        f"例（先頭 {min(len(rows), ERROR_JOB_EXAMPLES)} 件）:\n{examples}\n\n"
        "詳細は trx.scrape_job の error_message を確認してください。\n",
    )


def refresh_presets_lookup_in_transaction(conn):
    """
    presets_lookup を作り直す（refresh_presets_lookup は daily_check.py のものをそのまま使う）。
    TRUNCATE から INSERT の commit までを1トランザクションにし、出品処理などの読み手に空の表を見せない
    （その間、読み手は TRUNCATE のスキーマロックで数秒待つ）。
    """
    conn.autocommit = False
    try:
        refresh_presets_lookup(conn)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True


def run_cycle(conn, cycle_no: int):
    started = _now()
    write_status(cycle_no=cycle_no, cycle_started_at=started.isoformat(timespec="seconds"), phase="presets_lookup 更新")
    log(f"===== サイクル {cycle_no} 開始 =====")

    # ブランド・カテゴリーの変更をサイクルごとに反映する（旧 daily_check は起動時の1回だけだった）
    conn.run(refresh_presets_lookup_in_transaction)

    run_scrape_step(conn, FETCH_SOLD, "簡易 fetch_sold")
    run_scrape_step(conn, FETCH_ACTIVE, "完全 fetch_active")
    run_scrape_step(conn, FETCH_SOLD, "完全 fetch_sold")
    run_remaining_step(conn, "完全 check_remaining")

    conn.run(notify_error_jobs, cycle_no, started)

    ended = _now()
    write_status(phase="サイクル終了", last_cycle_no=cycle_no, last_cycle_ended_at=ended.isoformat(timespec="seconds"))
    log(f"===== サイクル {cycle_no} 終了（所要 {ended - started}） =====")


# ======================
# メイン
# ======================
def main() -> int:
    disable_console_quick_edit()

    others = find_running_scripts(INVENTORY_CONFLICT_SCRIPTS, exclude_pid=os.getpid())
    if others:
        lines = "\n".join(f"  PID {pid} {name}（{started:%m/%d %H:%M} 起動）" for pid, name, started in others)
        log(f"在庫管理（または旧 daily_check）が既に動いているため起動を中止します\n{lines}")
        return 1

    STATUS_PATH.unlink(missing_ok=True)  # 前回起動分の状態は引き継がない
    write_status(state="running", pid=os.getpid(), started_at=_now().isoformat(timespec="seconds"), phase="初期化")
    log(f"=== inventory_loop 開始 PID={os.getpid()} ===")

    conn = ReconnectingConnection()
    conn.autocommit = True
    try:
        cycle_no = 1
        while True:
            run_cycle(conn, cycle_no)
            # 停止判定はサイクル終了時だけ行う
            if conn.run(should_stop_daily_check):
                break
            cycle_no += 1

        write_status(state="stopped", phase="停止", stopped_at=_now().isoformat(timespec="seconds"),
                     stop_reason="mst.control 日次処理停止")
        log(f"=== inventory_loop 正常終了（停止フラグ、{cycle_no} サイクル完了） ===")
        send_mail(
            "在庫管理を停止しました（停止フラグ）",
            f"mst.control『日次処理停止』を検知したため、サイクル {cycle_no} の終了後に在庫管理を停止しました。\n"
            "再開する場合は inventory_loop.py を手動で起動してください。\n",
        )
        return 0

    except KeyboardInterrupt:
        write_status(state="stopped", phase="停止", stopped_at=_now().isoformat(timespec="seconds"),
                     stop_reason="Ctrl+C")
        log("=== inventory_loop 手動停止（Ctrl+C） ===")
        return 130

    except Exception as e:
        status = read_status() or {}
        write_status(state="error", stopped_at=_now().isoformat(timespec="seconds"), stop_reason=str(e)[:500])
        detail = str(e) if isinstance(e, StepFailed) else traceback.format_exc()
        log(f"=== inventory_loop 異常終了: {e} ===")
        notified = send_mail(
            "【異常】在庫管理が異常終了しました",
            f"サイクル: {status.get('cycle_no')}\n"
            f"工程: {status.get('phase')}\n"
            f"時刻: {_now():%Y-%m-%d %H:%M:%S}\n\n"
            f"{detail}\n\n"
            "在庫管理は自動では再開しません。原因を確認してから inventory_loop.py を手動で起動してください。\n",
        )
        # 送れなかった場合は inventory_monitor.py が代わりに通知する
        write_status(error_notified=bool(notified))
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
