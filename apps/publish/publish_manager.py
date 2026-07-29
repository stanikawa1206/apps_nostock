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

import calendar
import logging
import math
import subprocess
import sys
import os
import time
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# ======================
# パス設定
# ======================
PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection

# ======================
# 停止地点特定用トレースログ（次回再現時の切り分け専用。挙動は一切変更しない）
# ======================
_PM_TRACE_LOG_PATH = PROJECT_ROOT / "logs" / "publish_manager.log"
_pm_trace_logger = logging.getLogger("publish_manager_trace")
if not _pm_trace_logger.handlers:
    _pm_trace_logger.setLevel(logging.DEBUG)
    _pm_trace_handler = logging.FileHandler(_PM_TRACE_LOG_PATH, encoding="utf-8")
    _pm_trace_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _pm_trace_logger.addHandler(_pm_trace_handler)
    _pm_trace_logger.propagate = False


def _pm_log(msg: str) -> None:
    """[トレース専用] logs/publish_manager.log にタイムスタンプ付きで記録する。"""
    _pm_trace_logger.debug(msg)

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

    _pm_log(f"[clear_close_status] start account={account}")
    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_CLEAR_CLOSE_STATUS_FOR_ACCOUNT, account)
            _pm_log(f"[clear_close_status] rowcount={cur.rowcount} account={account}")
        conn.commit()
        _pm_log(f"[clear_close_status] commit success account={account}")
    finally:
        conn.close()
        _pm_log(f"[clear_close_status] end account={account}")


SQL_MARK_ACCOUNT_DONE = """
UPDATE mst.ebay_accounts
SET
    close_reason = 'DONE'
WHERE
    account = ?
"""


def mark_account_done(account):
    """
    指定アカウントを、当日の目標件数に到達済みとしてDONE確定させる。

    LIMIT中に複数worker(VPS/ローカル)が並行投稿した結果、post_targetを
    超過した状態でeBay側の出品制限にも該当することがある
    （目標超過とLIMIT検知がほぼ同時に起きるケース）。この場合、
    publish_ebay.py側はループ先頭のsent_now>=post_targetチェックへ
    戻る前にLIMITでbreakしてしまうため、DONEが一度も書き込まれない。

    そのためこの関数は、呼び出し元(handle_limit_account)がすでに
    「今日の投稿数 >= post_target」を確認済みの時点で、close_reasonを
    NULLへ戻すのではなく直接DONEへ確定させる。以後どのworkerが
    このアカウントを再取得するのを待つ必要もない。
    """

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_MARK_ACCOUNT_DONE, account)
        conn.commit()
    finally:
        conn.close()


# ======================
# post_target自動計算
# ======================
# 月間目標件数・1日あたりのpost_target上限・基準タイムゾーン（米国東部時間）。
MONTHLY_TARGET = 10000
MAX_DAILY_POST_TARGET = 500
US_EASTERN_TZ = ZoneInfo("America/New_York")

SQL_SELECT_POST_TARGET_ACCOUNTS = """
SELECT account, listing_left, post_target
FROM mst.ebay_accounts
WHERE ISNULL(is_excluded, 0) = 0
"""

SQL_UPDATE_POST_TARGET = """
UPDATE mst.ebay_accounts
SET post_target = ?
WHERE account = ?
"""


def calculate_post_target(
    listing_left,
    us_date,
    monthly_target=MONTHLY_TARGET,
    max_daily_post_target=MAX_DAILY_POST_TARGET,
):
    """
    月間目標(monthly_target)件に対し、us_date(米国東部の「本日」の日付)時点で
    必要なpost_targetを計算する。

    戻り値(dict):
        days_in_month:         当月の日数
        daily_quota:           1日あたりの出品ノルマ (monthly_target / days_in_month)
        target_progress:       本日の目標進行数 (daily_quota * us_date.day)
        actual_progress:       実際進行数 (monthly_target - listing_left)
        required_count:        必要出品数 (target_progress - actual_progress)
        calculated_post_target: 最終post_target
        cap_applied:           500件上限（max_daily_post_target）が適用されたかどうか
        is_last_day_of_month:  当月最終日として特別処理を適用したかどうか

    通常日: calculated_post_target = max(0, min(max_daily_post_target, ceil(required_count)))

    当月最終日(us_date.day == days_in_month)だけは特別処理とする。
    毎日ceil()した端数が月末までに累積し、月間目標(10,000件)を超えてしまう
    可能性があるため、最終日は通常計算(required_countのceil)を使わず、
    その時点の残り出品可能数(listing_left)をそのまま採用する
    （1日500件上限のみ維持する）:
        calculated_post_target = min(max_daily_post_target, listing_left)

    副作用は一切ない（DBアクセス・日時取得を行わない）純粋関数。
    """

    _, days_in_month = calendar.monthrange(us_date.year, us_date.month)

    daily_quota = monthly_target / days_in_month
    target_progress = daily_quota * us_date.day
    actual_progress = monthly_target - listing_left
    required_count = target_progress - actual_progress

    is_last_day_of_month = (us_date.day == days_in_month)

    if is_last_day_of_month:
        # 最終日特別処理: ceil()の累積誤差で月間目標を超えないよう、
        # 通常計算(required_countのceil)は使わず、残り出品可能数(listing_left)を
        # そのまま採用する（1日500件上限のみ維持する）。
        calculated_post_target = min(max_daily_post_target, listing_left)
        cap_applied = listing_left > max_daily_post_target
    else:
        raw_post_target = math.ceil(required_count)
        calculated_post_target = max(0, min(max_daily_post_target, raw_post_target))
        cap_applied = raw_post_target > max_daily_post_target

    return {
        "days_in_month": days_in_month,
        "daily_quota": daily_quota,
        "target_progress": target_progress,
        "actual_progress": actual_progress,
        "required_count": required_count,
        "calculated_post_target": calculated_post_target,
        "cap_applied": cap_applied,
        "is_last_day_of_month": is_last_day_of_month,
    }


def validate_listing_left(listing_left):
    """
    listing_left が計算可能な値かどうかを判定する。

    戻り値: (ok: bool, error_message: str or None)
    """

    if listing_left is None:
        return False, "listing_left が NULL のため計算できません"

    if listing_left < 0:
        return False, f"listing_left が異常値です（{listing_left} < 0）"

    if listing_left > MONTHLY_TARGET:
        return False, f"listing_left が異常値です（{listing_left} > {MONTHLY_TARGET}）"

    return True, None


def update_post_targets():
    """
    is_excluded=0 の全アカウントについて、米国東部時間の本日時点で必要な
    post_targetを自動計算し、mst.ebay_accounts.post_target へ反映する。

    publish_manager起動直後（reset_close_status()の後、get_active_listings
    開始前）に一度だけ実行する。listing_leftが異常値(NULL・0未満・monthly_target超)
    のアカウントはWARNINGを出してスキップし、post_targetは更新しない。
    """

    now_et = datetime.now(US_EASTERN_TZ)
    us_date = now_et.date()

    print("=" * 40)
    print("post_target自動計算開始")
    print("=" * 40)
    print()

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_POST_TARGET_ACCOUNTS)
            rows = cur.fetchall()

        accounts = [
            {"account": row[0], "listing_left": row[1], "post_target": row[2]}
            for row in rows
        ]

        updated_count = 0

        for account_info in accounts:
            account = account_info["account"]
            listing_left = account_info["listing_left"]
            current_post_target = account_info["post_target"]

            ok, error_message = validate_listing_left(listing_left)
            if not ok:
                print(f"WARNING {account} : {error_message}のためスキップします")
                continue

            result = calculate_post_target(listing_left, us_date)
            new_post_target = result["calculated_post_target"]

            with conn.cursor() as cur:
                cur.execute(SQL_UPDATE_POST_TARGET, new_post_target, account)
            conn.commit()

            updated_count += 1
            print(f"{account:<10}: {current_post_target} → {new_post_target}")

        print()
        print("=" * 40)
        print("post_target更新完了")
        print(f"対象アカウント : {len(accounts)}")
        print(f"更新件数       : {updated_count}")
        print("=" * 40)

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
    ローカルで get_active_listings.py を起動する（Popenで起動のみ、完了待ちは呼び出し元で行う）。

    起動した Popen オブジェクトを返す。完了確認は is_active_listings_fetch_done() ではなく、
    呼び出し元(main)がこの Popen を wait() することで行う。
    """

    print("🟢 LOCAL get_active_listings 開始")

    proc = subprocess.Popen(
        [
            PYTHON,
            "-m",
            "apps.publish.get_active_listings",
        ],
        cwd=str(BASE_DIR),
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    print("🚀 LOCAL get_active_listings 起動完了")

    return proc


def run_get_active_listings_vps():
    """
    VPS版 get_active_listings を全台同時に起動する（Popenで起動のみ、完了待ちは呼び出し元で行う）。

    各VPSは ssh を直接起動し(start/cmd.exeは経由しない)、CREATE_NEW_CONSOLE で
    個別のコンソールウィンドウにログをリアルタイム表示する。start経由にしないのは、
    startは起動直後に detach してしまい、返る Popen が実際の ssh プロセスと
    一致しなくなる（= proc.wait() で本当の完了を待てなくなる）ため。
    起動した Popen オブジェクトのリストを返し、完了確認は呼び出し元(main)が
    これらを wait() することで行う（is_active_listings_fetch_done() は使用しない）。
    """

    print("🟢 VPS get_active_listings 開始")

    processes = []

    for ip in VPS_LIST:
        proc = subprocess.Popen(
            [
                "ssh",
                f"root@{ip}",
                (
                    "cd /opt/apps_nostock && "
                    "git pull && "
                    "python3 -u -m apps.publish.get_active_listings"
                ),
            ],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
        processes.append(proc)

    print("🚀 VPS全台で get_active_listings 起動完了")

    return processes


SQL_TRUNCATE_ACTIVE_LISTINGS = "TRUNCATE TABLE ext.ebay_active_download"


def truncate_active_listings():
    """
    ext.ebay_active_download を1回だけ全件削除する。

    LOCAL・VPS7台が同時にアカウント単位でDELETEするとロック競合するため、
    get_active_listings起動前にここで1回だけ全件削除する
    （get_active_listings側はDELETEを行わず、INSERTのみ行う）。
    FK・インデックス付きビュー・レプリケーション/CDC・トリガーが無いことを確認済みのため
    TRUNCATE TABLEを使用する。
    """

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_TRUNCATE_ACTIVE_LISTINGS)
        conn.commit()
    finally:
        conn.close()


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

        _pm_log(f"[get_limit_accounts] result={limit_accounts}")

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

    _pm_log(f"[request_listing_space] start account={account} remaining={remaining}")

    print(f"🟡 {account} {remaining}件分の出品枠を確保します")

    actual_count = run_make_listing_space(account, remaining)

    if actual_count > 0:
        print(f"🟢 {account} {actual_count}件分の出品枠を確保しました")

    _pm_log(f"[request_listing_space] end account={account} actual_count={actual_count}")

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
      2. remaining <= 0 なら、すでに目標達成済みなので削除はせずDONEに確定させる
         （NULLに戻してworkerの再取得に委ねると、複数worker並行投稿による
         target超過とLIMIT検知が重なった場合にLIMITのまま固まり得るため、
         ここで直接DONEにする。万一ここを通らなくても
         auto_fix_done_accounts() が同じ条件で補正する）
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

    _pm_log(f"[handle_limit_account] start account={account} active_workers={active_workers}")

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

        _pm_log(f"[calculate_remaining_count] account={account} remaining={remaining} post_target={account_info['post_target']}")

        print(f"🟡 {account} LIMITを検知しました")
        print(f"   post_target={account_info['post_target']}")
        print(f"   today_posted={account_info['post_target'] - remaining}")
        print(f"   remaining={remaining}")

        # すでに当日の目標数に達している（remaining<=0）なら、削除は不要。
        # ここでNULLに戻してworkerの再取得に委ねると、複数workerの並行投稿で
        # post_targetを超過しつつeBay側の実制限にも同時にヒットした場合、
        # workerがループ先頭のsent_now>=post_targetチェックへ戻れず
        # LIMITのまま固まる恐れがある（再度LIMITでbreakする可能性があるため）。
        # そのためworkerの再取得を待たず、ここで直接DONEに確定させる。
        if remaining <= 0:
            mark_account_done(account)
            print(f"🟢 {account} 目標達成済みのためDONEにしました")
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
        _pm_log(f"[handle_limit_account] clear_close_status returned account={account}")
        print(f"🟢 {account} LIMIT状態を解除しました")

    except Exception as e:
        # remaining計算・出品枠確保・LIMIT解除のどこで例外が起きても、ここで吸収する。
        # LIMIT状態はクリアせず（＝安全側）維持し、他アカウントの監視は継続させる。
        _pm_log(f"[handle_limit_account] EXCEPTION account={account} error={e}")
        _pm_log(traceback.format_exc())
        print(f"❌ {account} 出品枠確保処理中に例外が発生しました: error={e}")


def handle_limit_accounts(limit_accounts):
    """LIMITになった各アカウントを1件ずつ処理する（1件の失敗が他に波及しない単位で処理する）。"""

    for account_info in limit_accounts:
        handle_limit_account(account_info)


# ======================
# DONE補正・終了判定
# ======================
SQL_AUTO_FIX_DONE_ACCOUNTS = """
UPDATE A
SET A.close_reason = 'DONE'
OUTPUT inserted.account
FROM mst.ebay_accounts A
CROSS APPLY (
    SELECT COUNT(*) AS today_posted
    FROM trx.listings L
    WHERE L.account = A.account
      AND CAST(L.start_time AS DATE) = CAST(GETDATE() AS DATE)
      AND L.is_deleted = 0
) T
WHERE ISNULL(A.is_excluded, 0) = 0
  AND (A.close_reason IS NULL OR A.close_reason = 'LIMIT')
  AND T.today_posted >= A.post_target
"""


def auto_fix_done_accounts():
    """
    目標件数(post_target)にすでに到達しているのに、DONEになっていない
    アカウントを自己修復する。

    is_excluded=0 の全アカウントについて、today_posted >= post_target なのに
    close_reason が NULL または LIMIT のままのものを 'DONE' に補正する。
    LIMITも対象に含めているのは、複数worker(VPS/ローカル)が同一アカウントを
    並行投稿した際、post_targetを超過した投稿とeBay側の実際の出品制限検知が
    ほぼ同時に起きると、publish_ebay.py側はループ先頭のsent_now>=post_target
    チェックへ戻る前にLIMITでbreakしてしまい、DONEが一度も書き込まれないまま
    LIMIT状態だけが残ることがあるため。この関数はactive_workersの状態に
    関係なく「today_posted >= post_target」という事実だけで判定するため、
    workerがこのアカウントをもう一周拾うのを待つ必要がない。
    終了判定(is_all_accounts_finished)は close_reason だけを信用せず、
    必ずこの補正を先に行ってから行う。
    """

    _pm_log("[auto_fix_done_accounts] start")

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_AUTO_FIX_DONE_ACCOUNTS)
            fixed_accounts = [row[0] for row in cur.fetchall()]
        conn.commit()
    finally:
        conn.close()

    for account in fixed_accounts:
        print("INFO")
        print(f"Auto fixed DONE account: {account}")

    _pm_log(f"[auto_fix_done_accounts] end fixed_accounts={fixed_accounts}")

    return fixed_accounts


SQL_SELECT_UNFINISHED_ACCOUNT = """
SELECT TOP 1 account
FROM mst.ebay_accounts
WHERE ISNULL(is_excluded, 0) = 0
  AND (close_reason IS NULL OR close_reason NOT IN ('DONE', 'EMPTY'))
"""


def is_all_accounts_finished():
    """
    is_excluded=0 の全アカウントについて、close_reason が DONE か EMPTY だけに
    なっているかどうかを判定する。

    close_reason が NULL（未処理）または LIMIT のアカウントが1件でも残っていれば
    False。auto_fix_done_accounts() による補正の後に呼び出すことで、
    close_reason だけを信用せず、今日の実績を反映した状態で判定する。
    """

    _pm_log("[is_all_accounts_finished] start")

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_UNFINISHED_ACCOUNT)
            row = cur.fetchone()
        result = row is None
        _pm_log(f"[is_all_accounts_finished] result={result}")
        return result
    finally:
        conn.close()


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

    このループには終了条件がある。DONE補正のあと、is_excluded=0の全アカウントの
    close_reasonがDONE/EMPTYだけになっていれば（＝LIMIT・未処理が1件も残って
    いなければ）、publish_managerの役目は終わったとみなしループを抜ける。
    """

    _pm_log("[monitor_limit_accounts] start")

    loop_no = 0

    while True:
        loop_no += 1
        _pm_log(f"[monitor_limit_accounts] loop start loop_no={loop_no}")

        # LIMITアカウント取得
        limit_accounts = get_limit_accounts()

        # LIMIT処理
        if limit_accounts:
            # 対象アカウントごとに、必要なら出品枠を確保してLIMITを解除する。
            # publish_ebayは待機中なので、解除すれば自動で出品を再開する。
            # 1件の処理で失敗しても、他のアカウントの監視は続ける
            # （例外は handle_limit_account 側で処理済みで、ここには伝播してこない）。
            handle_limit_accounts(limit_accounts)

        # DONE補正（publish_ebay側が書き込めなかったDONEを自己修復する）
        auto_fix_done_accounts()

        # 終了判定（close_reasonだけを信用せず、DONE補正の後に行う）
        if is_all_accounts_finished():
            print("INFO")
            print("All publish accounts finished.")
            print("publish_manager exiting.")
            _pm_log(f"[monitor_limit_accounts] all accounts finished, breaking loop loop_no={loop_no}")
            break

        # 少し待ってからもう一度確認する
        _pm_log(f"[monitor_limit_accounts] sleep before loop_no={loop_no} seconds={LIMIT_CHECK_INTERVAL_SECONDS}")
        time.sleep(LIMIT_CHECK_INTERVAL_SECONDS)


def main():
    print("=" * 40)
    print("=== publish_manager.py 実行開始 ===")
    print("=" * 40)

    # 前日のLIMIT情報をクリアする
    reset_close_status()

    # 各アカウントの本日時点のpost_targetを自動計算してmst.ebay_accountsへ反映する
    update_post_targets()

    # 最新のActive Listingを取得する（ローカル1プロセスのみ・直列実行）。
    # VPSへの分散取得は、一部アカウントだけ取得失敗するケースが発生したため
    # 現在は無効化している。原因調査後に必要であれば以下のコメントを外して復活させる。
    #
    # processes = run_get_active_listings_vps()
    # for proc in processes:
    #     proc.wait()
    #
    # get_active_listings.py 側が現在アカウントごとにDELETE→INSERTを行うため、
    # ここでの全件事前TRUNCATEは行わない（途中で失敗した場合に他アカウントの
    # 既存データまで消えてしまうことを避けるため）。
    # truncate_active_listings()

    local_proc = run_get_active_listings_local()
    local_proc.wait()

    # 出品処理を開始する（VPS・ローカルとも常駐して出品を続ける）
    run_vps()
    run_local()

    # LIMITを監視する（publish を再度起動することはない）。
    # LIMIT検知 → 必要件数の計算 → 出品枠確保 → LIMIT状態解除、という一連の対応を
    # monitor_limit_accounts() が繰り返し行う。publish_ebay側は待機しているだけなので、
    # LIMIT状態を解除すれば自動的に出品を再開する。
    monitor_limit_accounts()

    _pm_log("[main] completed normally")

    print("=" * 40)
    print("=== publish_manager.py 正常終了 ===")
    print("=" * 40)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        _pm_log(f"[__main__] UNCAUGHT EXCEPTION: {e}")
        _pm_log(traceback.format_exc())
        print("=" * 40)
        print("=== publish_manager.py 異常終了 ===")
        print(f"例外内容: {e}")
        print("=" * 40)
        raise
