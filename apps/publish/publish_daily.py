# -*- coding: utf-8 -*-
"""
publish_daily.py

毎日 日本時間 3:00 にタスクスケジューラから起動する出品処理（在庫管理 inventory_loop.py とは独立）。

  1. 前回の出品（publish_daily / publish_manager / 旧 daily_check）が動いていれば起動せず異常通知して終了
  2. 米国太平洋時間で月が替わっていれば、出品枠の残数を初期化（listing_left=10,000 / additional_listing_left=50,000）
  3. 古い出品の削除（delete_ebay_daily.py）
  4. 出品処理のロック解除 → publish_manager.py（Active Listing取得・post_target計算・出品・LIMIT監視）→ 結果メール
  5. 出品中に太平洋時間の月をまたいだ場合は、枠の残数がずれるため要確認通知

出品枠のリセット月は mst.control『出品枠リセット月』（yyyymm）に記録し、
このキーの更新と残数の初期化を同じトランザクションで行う（同じ月に二重リセットしない・再実行しても安全）。
"""

import os
import sys
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import send_mail
from apps.inventory.daily_check import (
    ReconnectingConnection,
    disable_console_quick_edit,
    run_delete_ebay_daily,
    run_publish_manager,
)
from apps.inventory.inventory_loop import find_running_scripts

# 出品処理と同時に動いてはいけないスクリプト（旧 daily_check は出品処理を含むため）
PUBLISH_CONFLICT_SCRIPTS = ("publish_daily.py", "publish_manager.py", "daily_check.py")

# ======================
# 出品枠の月次リセット
# ======================
# eBay の出品枠は米国太平洋時間の毎月1日 0:00 に切り替わる（夏時間・冬時間は ZoneInfo が考慮する）
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
SERVER_TZ = ZoneInfo("Asia/Tokyo")

CONTROL_KEY_LISTING_RESET_MONTH = "出品枠リセット月"
MONTHLY_LISTING_LEFT = 10000
MONTHLY_ADDITIONAL_LISTING_LEFT = 50000


def pacific_month_key(now_utc: datetime) -> int:
    """太平洋時間での年月を yyyymm の整数で返す（now_utc はタイムゾーン付き）。"""
    d = now_utc.astimezone(PACIFIC_TZ)
    return d.year * 100 + d.month


def pacific_month_start_server_time(month_key: int) -> datetime:
    """太平洋時間の月初 0:00 を、サーバー時刻（JST、naive）で返す。trx.listings.start_time との比較用。"""
    start = datetime(month_key // 100, month_key % 100, 1, tzinfo=PACIFIC_TZ)
    return start.astimezone(SERVER_TZ).replace(tzinfo=None)


def reset_listing_quota_if_new_month(conn, now_utc: datetime):
    """
    mst.control『出品枠リセット月』が太平洋時間の当月より前なら、出品枠の残数を初期化して当月を記録する。
    キーの確認・残数の初期化・当月の記録を1つのトランザクションで行い、キーの行を UPDLOCK で押さえるため、
    同時に2回実行されても初期化は1回だけになる。
    キーの行が無い・値が NULL の場合は、どの月まで初期化済みか判断できないため例外にする（初期設定が必要）。

    戻り値: 初期化した場合は {"month": 当月, "prev_month": 前回, "before": [(account, listing_left, additional_listing_left)]}、
            初期化不要なら None
    """
    month = pacific_month_key(now_utc)
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT param_value FROM mst.control WITH (UPDLOCK, HOLDLOCK) WHERE param_key = ?",
            CONTROL_KEY_LISTING_RESET_MONTH,
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            raise RuntimeError(
                f"mst.control『{CONTROL_KEY_LISTING_RESET_MONTH}』が未設定です。"
                f"出品枠を初期化済みの太平洋時間の年月（例: {month}）を yyyymm で登録してください。"
            )
        prev_month = row[0]
        if prev_month >= month:
            conn.rollback()
            return None

        cur.execute("""
            SELECT account, listing_left, additional_listing_left
            FROM mst.ebay_accounts
            WHERE ISNULL(is_excluded, 0) = 0
            ORDER BY account
        """)
        before = [tuple(r) for r in cur.fetchall()]

        cur.execute(
            """
            UPDATE mst.ebay_accounts
            SET listing_left = ?, additional_listing_left = ?
            WHERE ISNULL(is_excluded, 0) = 0
            """,
            MONTHLY_LISTING_LEFT, MONTHLY_ADDITIONAL_LISTING_LEFT,
        )
        cur.execute(
            "UPDATE mst.control SET param_value = ? WHERE param_key = ?",
            month, CONTROL_KEY_LISTING_RESET_MONTH,
        )
        conn.commit()
        return {"month": month, "prev_month": prev_month, "before": before}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True


def notify_listing_quota_reset(result):
    lines = "\n".join(
        f"  {account}: listing_left {left} → {MONTHLY_LISTING_LEFT} / "
        f"additional_listing_left {add_left} → {MONTHLY_ADDITIONAL_LISTING_LEFT}"
        for account, left, add_left in result["before"]
    )
    body = (
        f"米国太平洋時間の月が替わったため（{result['prev_month']} → {result['month']}）、"
        "出品処理の開始前に出品枠の残数を初期化しました。\n\n"
        f"{lines}\n"
    )
    print(body)
    send_mail(f"出品枠を初期化しました（{result['month']}）", body)


def count_listings_since(conn, since: datetime):
    """since（サーバー時刻）以降に出品した件数をアカウント別に返す。"""
    cur = conn.cursor()
    cur.execute("""
        SELECT account, COUNT(*)
        FROM trx.listings
        WHERE start_time >= ?
        GROUP BY account
        ORDER BY account
    """, since)
    return [tuple(r) for r in cur.fetchall()]


def notify_if_publish_crossed_pacific_month(conn, start_utc: datetime, end_utc: datetime):
    """
    出品中に太平洋時間の月が替わった場合、新しい月の枠で出品した分が旧月の残数から引かれており、
    次回の初期化で 10,000 / 50,000 に戻すと、その分だけ残数が実際より多くなるため通知する。
    """
    end_month = pacific_month_key(end_utc)
    if pacific_month_key(start_utc) == end_month:
        return False

    boundary = pacific_month_start_server_time(end_month)
    rows = conn.run(count_listings_since, boundary)
    lines = "\n".join(f"  {account}: {n} 件" for account, n in rows) or "  （0件）"
    body = (
        f"出品処理の実行中に米国太平洋時間の月が替わりました（{boundary:%Y-%m-%d %H:%M} JST）。\n"
        "月の切り替え後の出品は新しい月の枠を使っていますが、DB の残数は旧月の値から引かれています。\n"
        "次回の出品開始前に残数を 10,000 / 50,000 に初期化するため、下記の件数分だけ残数が実際より多くなります。\n"
        "Seller Hub の残数と照合して補正してください。\n\n"
        f"切り替え後の出品件数（trx.listings、通常枠・追加枠の合計）:\n{lines}\n"
    )
    print(body)
    send_mail("【要確認】出品中に米国太平洋時間の月が替わりました", body)
    return True


# ======================
# メイン
# ======================
def main() -> int:
    disable_console_quick_edit()
    print(f"=== publish_daily 開始 {datetime.now():%Y-%m-%d %H:%M:%S} PID={os.getpid()} ===")

    others = find_running_scripts(PUBLISH_CONFLICT_SCRIPTS, exclude_pid=os.getpid())
    if others:
        lines = "\n".join(f"  PID {pid} {name}（{started:%m/%d %H:%M} 起動）" for pid, name, started in others)
        body = (
            "前回の出品処理（または旧方式の daily_check）がまだ動いているため、今回の出品処理は開始しません。\n\n"
            f"{lines}\n\n"
            "前回分が終わらない原因を確認してください。出品枠の初期化・古い出品の削除も今回は行っていません。\n"
        )
        print(body)
        send_mail("【異常】前回の出品処理が継続中のため本日の出品を開始しません", body)
        return 1

    conn = ReconnectingConnection()
    conn.autocommit = True
    try:
        reset = conn.run(reset_listing_quota_if_new_month, datetime.now(timezone.utc))
        if reset:
            notify_listing_quota_reset(reset)
        else:
            print("ℹ 出品枠の初期化は不要です（太平洋時間の当月は初期化済み）")

        # 29日以上経過した古い出品を削除（結果メールは従来どおり）
        run_delete_ebay_daily(conn)

        # 出品（ロック解除 → publish_manager → 結果メールは従来どおり）
        pub_start_utc = datetime.now(timezone.utc)
        pub_code = run_publish_manager(conn)
        notify_if_publish_crossed_pacific_month(conn, pub_start_utc, datetime.now(timezone.utc))

        print(f"=== publish_daily 終了 publish_manager returncode={pub_code} ===")
        return 0 if pub_code == 0 else 1

    except Exception:
        tb = traceback.format_exc()
        print(tb)
        send_mail(
            "【異常】出品処理（publish_daily）が異常終了しました",
            f"時刻: {datetime.now():%Y-%m-%d %H:%M:%S}\n\n{tb}",
        )
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
