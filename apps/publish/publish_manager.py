# -*- coding: utf-8 -*-
"""
publish_manager.py

eBay出品全体を管理する司令塔(Orchestrator)。

publish (publish_ebay.py) はVPS・ローカルそれぞれで常駐し、出品を継続する。
publish_manager.py は publish を繰り返し起動するのではなく、

    1. 起動時に一度だけ、前日のLIMIT状態をクリアしてVPS・ローカルのpublishを起動する
    2. その後はLIMITになったアカウントを監視し、必要なら出品枠を確保する

という「起動」と「LIMIT監視」だけを担当する。publish の完了を待つ必要はない。

publish_ebay.py は「出品だけ」を行い、
make_listing_space.py は「出品枠を確保するだけ」を行う。
"""

import subprocess
import sys
import os
import time
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# ======================
# パス設定
# ======================
PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection

# 出品枠確保は make_listing_space.py の責務。
# publish_manager は「1アカウント分だけ確保する」公開関数を直接importして呼び出す。
from apps.publish.make_listing_space import run_make_listing_space

BASE_DIR = PROJECT_ROOT
PYTHON = sys.executable

PUBLISH_SCRIPT = BASE_DIR / "apps" / "publish" / "publish_ebay.py"

# ======================
# VPS一覧
# ======================
VPS_LIST = [
    "162.43.42.135",
    "162.43.15.160",
    "162.43.29.154",
    "210.131.209.103",
    "162.43.39.209",
    "85.131.251.127",
    "210.131.209.232",
]

# LIMIT監視の間隔（秒）
LIMIT_CHECK_INTERVAL_SECONDS = 5


# ======================
# 前日状態のリセット
# ======================
SQL_RESET_CLOSE_STATUS = """
UPDATE mst.ebay_accounts
SET
    is_closed_today = 0,
    close_reason = NULL
WHERE
    is_closed_today = 1
    OR close_reason IS NOT NULL
"""


def reset_close_status():
    """
    前日までのLIMIT/当日終了状態を全アカウント分クリアする。

    publish_manager 起動時に一度だけ呼び出す。
    """

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_RESET_CLOSE_STATUS)
        conn.commit()
    finally:
        conn.close()


SQL_CLEAR_CLOSE_STATUS_FOR_ACCOUNT = """
UPDATE mst.ebay_accounts
SET
    close_reason = NULL,
    is_closed_today = 0
WHERE
    account = ?
"""


def clear_close_status(account):
    """
    指定アカウントだけ、LIMIT/当日終了状態をクリアする。

    出品枠確保が完了し、そのアカウントの出品を再開してよくなったタイミングで呼び出す。
    他のアカウントの状態には影響しない。
    """

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_CLEAR_CLOSE_STATUS_FOR_ACCOUNT, account)
        conn.commit()
    finally:
        conn.close()


# ======================
# publish 起動
# ======================
def run_local():
    """
    ローカル publish (publish_ebay.py) を起動する（起動のみ、完了は待たない）。

    publish はローカルでも常駐して出品を続けるため、publish_manager 側はここで待機しない。
    """

    print("🟢 LOCAL publish 開始")

    subprocess.Popen(
        [PYTHON, str(PUBLISH_SCRIPT)],
        cwd=str(BASE_DIR),
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    print("🚀 LOCAL publish 起動完了")


def run_vps():
    """VPS版 publish を全台起動する（起動のみ、完了は待たない）。"""

    print("🟢 VPS publish 開始")

    for ip in VPS_LIST:
        cmd = (
            f'start "{ip}" '
            f'ssh -tt root@{ip} '
            '"cd /opt/apps_nostock && '
            'git pull && '
            'cd /opt/apps_nostock/apps/publish && '
            'chmod +x publish_ebay_loop.sh && '
            './publish_ebay_loop.sh"'
        )

        subprocess.Popen(cmd, shell=True)

    print("🚀 VPS全台で publish 起動完了")


# ======================
# LIMIT監視
# ======================
SQL_SELECT_LIMIT_ACCOUNTS = """
SELECT
    account,
    post_target
FROM mst.ebay_accounts
WHERE close_reason = 'LIMIT'
"""


def get_limit_accounts():
    """
    LIMITになったアカウントを取得する。

    post_target は mst.ebay_accounts.post_target の値をそのまま使用する。
    publish_manager.py 側に post_target の固定値は持たせない
    （値の調整は mst.ebay_accounts を直接更新して行う運用のため）。

    戻り値の構造:
        [
            {
                "account": "BUZZ②",
                "post_target": 320,
            },
            ...
        ]
    """

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_LIMIT_ACCOUNTS)
            rows = cur.fetchall()

        limit_accounts = [
            {"account": row[0], "post_target": row[1]}
            for row in rows
        ]

        return limit_accounts

    finally:
        conn.close()


SQL_SELECT_TODAY_POSTED_COUNT = """
SELECT COUNT(*)
FROM trx.listings
WHERE account = ?
  AND CAST(start_time AS DATE) = CAST(GETDATE() AS DATE)
  AND is_deleted = 0
"""


def get_today_posted_count(account):
    """
    指定アカウントの本日の出品成功件数を取得する。

    publish_ebay.py が出品停止判定に使っているものと同じSQL・同じロジック
    （trx.listings を start_time の日付(SQL Server側の当日)で絞り込み、
    is_deleted = 0 で削除済みを除外）を使用する。
    """

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_TODAY_POSTED_COUNT, account)
            today_posted = cur.fetchone()[0]

        return today_posted

    finally:
        conn.close()


def calculate_remaining_count(account_info):
    """post_target と本日の出品実績から、不足件数(remaining)を計算する。"""

    account = account_info["account"]
    post_target = account_info["post_target"]

    today_posted = get_today_posted_count(account)
    remaining = post_target - today_posted

    return remaining


# ======================
# 出品枠確保
# ======================
def request_listing_space(account, remaining):
    """
    必要な件数分だけ古い出品を削除して空きを作る（make_listing_space.py への依頼）。

    remaining > 0 の場合のみ呼び出される想定。
    戻り値は実際に確保できた件数（0件の場合や、remainingに届かない場合もある）。
    make_listing_space.py 側で起きた例外はそのまま呼び出し元に伝播させる
    （LIMIT状態を解除してよいかの判断は呼び出し元(handle_limit_account)が行う）。
    """

    print(f"🟡 {account} {remaining}件分の出品枠を確保します")

    actual_count = run_make_listing_space(account, remaining)

    if actual_count > 0:
        print(f"🟢 {account} {actual_count}件分の出品枠を確保しました")

    return actual_count


def handle_limit_account(account_info):
    """
    LIMITになった1アカウント分の対応を行う。

    流れ:
      1. LIMITになったアカウントについて、今日あと何件出品したいか計算する
      2. remaining <= 0 なら、削除はせずLIMIT状態だけ解除する
         （単に目標達成後の状態が残っているだけの可能性があるため）
      3. remaining > 0 なら、必要な件数分だけ古い出品を削除して空きを作る
      4. 空きができた場合（1件以上確保できた場合）だけLIMIT状態を解除する
         （publish_ebayは待機中なので、解除後に自動で出品を再開する）
      5. 途中のどこで何が起きても（DBエラー・make_listing_space側の例外など）、
         この関数の中で例外を吸収し、LIMIT状態は維持したまま次の監視サイクルに委ねる。
         1件の処理で失敗しても、他のアカウントの監視は止めない
         （この関数を呼び出す handle_limit_accounts / monitor_limit_accounts 側には
         例外を伝播させない）。
    """

    account = account_info["account"]

    try:
        remaining = calculate_remaining_count(account_info)

        print(f"🟡 {account} LIMITを検知しました")
        print(f"   post_target={account_info['post_target']}")
        print(f"   today_posted={account_info['post_target'] - remaining}")
        print(f"   remaining={remaining}")

        # すでに当日の目標数に達しているだけなら、削除は不要。LIMIT状態だけ解除する。
        if remaining <= 0:
            clear_close_status(account)
            print(f"🟢 {account} 目標達成済みのためLIMIT状態を解除しました")
            return

        # 必要な件数分だけ古い出品を削除して空きを作る
        actual_count = request_listing_space(account, remaining)

        if actual_count <= 0:
            # 空きができていないので、LIMIT状態はそのまま維持して次の監視へ進む。
            print(f"⚠️ {account} 出品枠を確保できませんでした。LIMIT状態を維持します。")
            return

        # 1件以上確保できていれば（要求件数に届かなくても）LIMIT状態を解除してよい。
        # まだ足りない分は、publish_ebay再開後に再びLIMITになった時点で
        # 次の監視サイクルが改めてremainingを計算して対応する。
        # publish_ebayは待機中なので、解除後に自動で出品を再開する。
        clear_close_status(account)
        print(f"🟢 {account} LIMIT状態を解除しました")

    except Exception as e:
        # remaining計算・出品枠確保・LIMIT解除のどこで例外が起きても、ここで吸収する。
        # LIMIT状態はクリアせず（＝安全側）維持し、他アカウントの監視は継続させる。
        print(f"❌ {account} 出品枠確保処理中に例外が発生しました: error={e}")


def handle_limit_accounts(limit_accounts):
    """LIMITになった各アカウントを1件ずつ処理する（1件の失敗が他に波及しない単位で処理する）。"""

    for account_info in limit_accounts:
        handle_limit_account(account_info)


# ======================
# LIMIT監視ループ
# ======================
def monitor_limit_accounts():
    """
    LIMITになったアカウントを一定間隔で監視し続けるループ。

    publish (VPS・ローカル) は常駐して出品を続けているため、
    publish_manager はここで publish の完了を待つ必要はない。
    LIMITになったアカウントを見つけ次第、都度その場で対応する
    （他アカウントの publish が終わるのを待つ必要もない）。
    """

    while True:
        # LIMITになったアカウントがあるか確認する
        limit_accounts = get_limit_accounts()

        if limit_accounts:
            # 対象アカウントごとに、必要なら出品枠を確保してLIMITを解除する。
            # publish_ebayは待機中なので、解除すれば自動で出品を再開する。
            # 1件の処理で失敗しても、他のアカウントの監視は続ける
            # （例外は handle_limit_account 側で処理済みで、ここには伝播してこない）。
            handle_limit_accounts(limit_accounts)

        # 少し待ってからもう一度確認する
        time.sleep(LIMIT_CHECK_INTERVAL_SECONDS)


def main():
    # 前日のLIMIT情報をクリアする
    reset_close_status()

    # 出品プログラムを起動する（VPS・ローカルとも常駐して出品を続ける）
    run_vps()
    run_local()

    # ここから先はLIMIT監視だけを行う（publish を再度起動することはない）。
    # LIMIT検知 → 必要件数の計算 → 出品枠確保 → LIMIT状態解除、という一連の対応を
    # monitor_limit_accounts() が繰り返し行う。publish_ebay側は待機しているだけなので、
    # LIMIT状態を解除すれば自動的に出品を再開する。
    monitor_limit_accounts()


if __name__ == "__main__":
    main()
