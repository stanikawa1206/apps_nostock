# -*- coding: utf-8 -*-
"""
publish_manager.py

eBay出品全体を管理する司令塔(Orchestrator)。

publish (publish_ebay.py) はVPS・ローカルそれぞれで常駐し、出品を継続する。
publish_manager.py は publish を繰り返し起動するのではなく、

    1. 起動時に一度だけ、前日のLIMIT状態をクリアする
    2. 最新のActive Listingを取得する（get_active_listings.py、全VPS・ローカルへ分散実行）
    3. 全VPS・ローカルの取得完了を待つ
    4. VPS・ローカルのpublishを起動する
    5. その後はLIMITになったアカウントを監視し、必要なら出品枠を確保する

という「前処理」「起動」「LIMIT監視」を担当する。publish の完了を待つ必要はない。

get_active_listings.py は「古い出品を削除するための前提データ取得」を行い、
publish_ebay.py は「出品だけ」を行い、
make_listing_space.py は「出品枠を確保するだけ」を行う。
いずれもpublish_manager.pyが全体の流れを制御する。
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
GET_ACTIVE_LISTINGS_SCRIPT = BASE_DIR / "apps" / "publish" / "get_active_listings.py"

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

# get_active_listings の完了待ちポーリング間隔（秒）
ACTIVE_LISTINGS_WAIT_SECONDS = 5


# ======================
# 前日状態のリセット
# ======================
SQL_RESET_CLOSE_STATUS = """
UPDATE mst.ebay_accounts
SET
    close_reason = NULL
WHERE
    close_reason IS NOT NULL
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
    close_reason = NULL
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
# get_active_listings 起動・完了待ち
# ======================
def run_get_active_listings_local():
    """
    ローカルで get_active_listings.py を起動する（起動のみ、完了は待たない）。

    publish同様、常駐ではなく担当アカウントを取得し終えたら自然に終了する
    一度きりの処理。完了確認は is_active_listings_fetch_done() 側で行う。
    """

    print("🟢 LOCAL get_active_listings 開始")

    subprocess.Popen(
        [
            PYTHON,
            "-m",
            "apps.publish.get_active_listings",
        ],
        cwd=str(BASE_DIR),
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    print("🚀 LOCAL get_active_listings 起動完了")


def run_get_active_listings_vps():
    """VPS版 get_active_listings を全台起動する（起動のみ、完了は待たない）。"""

    print("🟢 VPS get_active_listings 開始")

    for ip in VPS_LIST:
        cmd = (
            f'start "{ip}" '
            f'ssh -tt root@{ip} '
            '"cd /opt/apps_nostock && '
            'git pull && '
            'cd /opt/apps_nostock && '
            'python3 -m apps.publish.get_active_listings"'
        )

        subprocess.Popen(cmd, shell=True)

    print("🚀 VPS全台で get_active_listings 起動完了")


SQL_SELECT_HAS_PENDING_LISTING_FETCH = """
SELECT TOP 1 A.account
FROM mst.ebay_accounts A
WHERE ISNULL(A.is_excluded, 0) = 0
"""

SQL_SELECT_HAS_ACTIVE_LISTING_FETCH_WORKER = """
SELECT TOP 1 execute_pc
FROM mst.execute_pcs
WHERE account IS NOT NULL
"""


def is_active_listings_fetch_done():
    """
    全VPS・ローカルの get_active_listings が完了したかどうかを判定する。

    get_active_listings.py は publish_ebay.py と同じ mst.execute_pcs を使って
    アカウントを分散取得するため、判定も同じ考え方を流用する。
    以下の両方を満たしたときだけ「完了」とみなす:
      1. is_excluded=0（削除対象外でない）アカウントのうち、
         本日分のActive Listingが未取得のものが1件も無い
      2. mst.execute_pcs 上で、現在アカウントを処理中(account IS NOT NULL)の
         PCが1件も無い
    1.だけだと「取得済み件数はゼロだが、まだどのPCも取得を始めていない」
    瞬間を誤って「完了」と判定しかねないため、2.と組み合わせて判定する。
    """

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_HAS_PENDING_LISTING_FETCH)
            pending = cur.fetchone()

            cur.execute(SQL_SELECT_HAS_ACTIVE_LISTING_FETCH_WORKER)
            working = cur.fetchone()

        return pending is None and working is None

    finally:
        conn.close()


def wait_for_active_listings_completion():
    """
    全VPS・ローカルの get_active_listings が終わるまで待つ。

    get_active_listings は常駐プロセスではないため、publish のように
    「起動したら待たない」のではなく、ここでは意図的に完了を待つ。
    削除処理（make_listing_space.py）や出品処理はこのデータが揃っている
    前提のため、is_active_listings_fetch_done() が真になるまでポーリングする。
    """

    while not is_active_listings_fetch_done():
        print("🕒 get_active_listings の完了を待機しています…")
        time.sleep(ACTIVE_LISTINGS_WAIT_SECONDS)

    print("✅ 全VPS・ローカルの get_active_listings が完了しました")


# ======================
# LIMIT監視
# ======================
SQL_SELECT_LIMIT_ACCOUNTS = """
SELECT
    A.account,
    A.post_target,
    ISNULL(W.active_workers, 0) AS active_workers
FROM mst.ebay_accounts A
LEFT JOIN (
    SELECT account, COUNT(*) AS active_workers
    FROM mst.execute_pcs
    WHERE account IS NOT NULL
    GROUP BY account
) W ON A.account = W.account
WHERE A.close_reason = 'LIMIT'
"""


def get_limit_accounts():
    """
    LIMITになったアカウントを取得する。

    post_target は mst.ebay_accounts.post_target の値をそのまま使用する。
    publish_manager.py 側に post_target の固定値は持たせない
    （値の調整は mst.ebay_accounts を直接更新して行う運用のため）。

    active_workers は mst.execute_pcs（publish_ebay.py の fetch_next_account_and_lock()
    が使っているのと同じテーブル）を集計し、そのアカウントを現在何台のworker
    (VPS/ローカル)が処理中かを表す。複数workerが同一アカウントを並行処理している場合、
    全workerがLIMITで止まりきる前に出品枠確保を始めると二重削除になり得るため、
    呼び出し側(handle_limit_account)で active_workers==0 を待ってから処理する。

    戻り値の構造:
        [
            {
                "account": "BUZZ②",
                "post_target": 320,
                "active_workers": 0,
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
            {"account": row[0], "post_target": row[1], "active_workers": row[2]}
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
      0. まず active_workers（このアカウントを今処理中のworker数）を確認する。
         1台以上残っている場合は、まだ他のVPS/ローカルがこのアカウントを処理中で
         あり、これからLIMITで止まる可能性があるため、ここでは何もせず
         次回の監視サイクルに委ねる（二重削除防止。詳細は下記コメント参照）。
      1. active_workers == 0 になって初めて、今日あと何件出品したいか計算する
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
    active_workers = account_info["active_workers"]

    # ===== 二重削除防止: 全workerが終了するまで待つ =====
    # 同一アカウントは複数VPS(最大MAX_PARALLEL_PC台)が並行して処理し得る。
    # ある1台がLIMITでclose_reason='LIMIT'を書いた直後は、まだ他のVPSが
    # そのアカウントを処理中で、少し遅れて同じくLIMITになることがある。
    # ここでactive_workers>0のまま出品枠確保を始めてしまうと、
    # 「1台目のLIMITで確保→2台目が遅れてLIMITを検知して再度確保」という
    # 二重削除が起こり得るため、全workerがこのアカウントの処理を終えて
    # active_workers==0になるまでは何もせず待つ。
    if active_workers > 0:
        print(f"🕒 {account} はまだ{active_workers}台が処理中のため、出品枠確保を待機します")
        return

    print(f"🟢 {account} の全worker終了を確認しました。出品枠確保を開始します")

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

    ただし「LIMITを検知したらすぐ削除処理を始める」わけではない。
    同一アカウントは複数のworker(VPS/ローカル)が並行して処理していることがあり、
    1台がLIMITになった直後は、他のworkerがまだ同じアカウントを処理中で、
    少し遅れて同様にLIMITになる可能性がある。そこですぐ出品枠確保を実行すると、
    「1台目の分で確保→2台目が遅れて検知してまた確保」という二重削除が起こり得る。
    そのため handle_limit_account 側で active_workers（そのアカウントを処理中の
    worker数）を確認し、全workerの処理が終わって active_workers == 0 になって
    初めて出品枠確保を行う。active_workers > 0 の間は何もせず、次のこの監視サイクルに委ねる。
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

    # 最新のActive Listingを取得する（全VPS・ローカルへ分散実行、起動のみ）
    run_get_active_listings_vps()
    run_get_active_listings_local()

    # 全VPSの取得完了を待つ
    # （古い出品の削除・出品処理は、このデータが揃っている前提のため）
    wait_for_active_listings_completion()

    # 出品処理を開始する（VPS・ローカルとも常駐して出品を続ける）
    run_vps()
    run_local()

    # LIMITを監視する（publish を再度起動することはない）。
    # LIMIT検知 → 必要件数の計算 → 出品枠確保 → LIMIT状態解除、という一連の対応を
    # monitor_limit_accounts() が繰り返し行う。publish_ebay側は待機しているだけなので、
    # LIMIT状態を解除すれば自動的に出品を再開する。
    monitor_limit_accounts()


if __name__ == "__main__":
    main()
