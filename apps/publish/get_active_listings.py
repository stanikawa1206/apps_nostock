# -*- coding: utf-8 -*-
"""
get_active_listings.py

eBayの現在の出品一覧(Active Listing)を取得し、ext.ebay_active_download へ保存する。

古い出品を削除する処理（make_listing_space.py 等）の前提データを作る処理であり、
「在庫を確認する」処理ではなく「削除のための下準備」という性格が強いため、
apps/inventory ではなく apps/publish 配下に置き、publish_manager.py の
前処理として扱う。

対象アカウントは mst.ebay_accounts.is_excluded で判定する（削除対象外のアカウント
だけを除外する）。DONE/EMPTY/LIMIT（close_reason）は一切関係ない。
close_reasonがLIMIT中のアカウントであっても、古い出品の削除対象にはなり得るため、
Active Listingは取得しておく必要がある。

現在の運用（安定運用優先）:
    ローカル1プロセスが、is_excluded=0 の全アカウントを1件ずつ順番に処理する
    （run_serial_local()）。VPSへの分散は行わない。

旧方式（VPS/ローカルへの分散取得、mst.execute_pcs使用）は、一部アカウントだけ
取得できないケースが発生したため現在は使用していないが、原因調査後に
再度必要になる可能性があるため、関連関数(fetch_next_account_for_listing_fetch /
release_listing_fetch_pc / run_for_this_pc)は削除せずそのまま残してある。
"""

import sys

# print内の絵文字等がWindowsの既定コンソールエンコーディング(cp932)で
# UnicodeEncodeErrorになり、そのアカウントの処理全体が異常終了する事象が
# 確認されたため、他の本番スクリプト（publish_manager.py等）と同様に
# stdout/stderrをUTF-8へ固定する。
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import datetime
import socket

import requests
import xml.etree.ElementTree as ET

from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import get_sql_server_connection


# =========================
# Trading API取得
# =========================
def get_active_listings(account: str):

    token = get_access_token_new(account)
    if not token:
        print(f"❌ token取得失敗: {account}")
        return []

    url = "https://api.ebay.com/ws/api.dll"

    headers = {
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml"
    }

    all_items = []
    page = 1
    per_page = 200

    while True:
        print(f"  → page {page}")

        body = f"""
        <?xml version="1.0" encoding="utf-8"?>
        <GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
            <ActiveList>
                <Pagination>
                    <EntriesPerPage>{per_page}</EntriesPerPage>
                    <PageNumber>{page}</PageNumber>
                </Pagination>
            </ActiveList>
        </GetMyeBaySellingRequest>
        """

        res = requests.post(url, headers=headers, data=body)
        root = ET.fromstring(res.text)

        ns = {"e": "urn:ebay:apis:eBLBaseComponents"}

        items = root.findall(".//e:Item", ns)
        count = len(items)

        print(f"    件数: {count}")

        if count == 0:
            break

        for item in items:
            item_id = item.findtext("e:ItemID", default="", namespaces=ns)
            title = item.findtext("e:Title", default="", namespaces=ns)

            price = item.findtext(".//e:CurrentPrice", default="0", namespaces=ns)
            price = float(price)

            watch = item.findtext("e:WatchCount", default="0", namespaces=ns)
            watch = int(watch)

            start_time = item.findtext(".//e:StartTime", default=None, namespaces=ns)

            sku = item.findtext(".//e:SKU", default="", namespaces=ns)

            all_items.append({
                "item_id": item_id,
                "sku": sku,
                "title": title,
                "price": price,
                "watch_count": watch,
                "start_time": start_time
            })

        # ★ ここが修正ポイント
        if count < per_page:
            break

        page += 1

    print(f"  ✔ 合計: {len(all_items)}")
    return all_items


# =========================
# DB INSERT
# =========================
def insert_items(cursor, account, items, fetched_at):

    if not items:
        return

    params = [
        (
            account,
            item["sku"],
            item["item_id"],
            item["title"],
            item["watch_count"],
            item["price"],
            item["start_time"],
            fetched_at
        )
        for item in items
    ]

    cursor.executemany("""
        INSERT INTO ext.ebay_active_download
        (
            account,
            vendor_item_id,
            listing_id,
            title_en,
            watchers,
            [Start price],
            start_time,
            fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, params)


# =========================
# execute_pcs を使った分散取得
# =========================
# is_excluded=0（削除対象外でない）アカウントを1件だけ、このPC(execute_pc)に割り当てる。
# 「本日既に取得済みかどうか」は一切見ない。get_active_listings は1日1回しか
# 起動しない運用を前提にしないため、手動再実行や障害復旧でも毎回全アカウントを
# 取得し直せるようにしている。
# NOT EXISTS (mst.execute_pcs側) により、他のPCが既に処理中のアカウントは
# 対象から外れる（同じアカウントを複数PCが同時に取得することはない）。
def fetch_next_account_for_listing_fetch(conn, current_pc, already_processed):
    """
    このPCが次に取得すべきアカウントを1件確保する。

    publish_ebay.py の fetch_next_account_and_lock() と同じ
    「mst.execute_pcs.account をUPDATEして自分に割り当てる」方式。
    対象は is_excluded=0 のアカウントのみで、DONE/EMPTY/LIMIT(close_reason)は問わない。
    取得済みかどうかの判定にDB側の値（fetched_at等）は一切使用しない。

    already_processed: このPCが今回のrunの中で既に処理し終えたaccountの集合。
    これが無いと、解放した直後の同じアカウントを毎回また選んでしまい、
    このPCの取得ループが終わらなくなるため、run単位・プロセス内メモリだけで
    重複取得を防ぐために使う（DBへの永続化は行わない）。

    取得すべきアカウントが無ければ None を返す。
    """

    exclude_clause = ""
    params = []

    if already_processed:
        placeholders = ", ".join("?" for _ in already_processed)
        exclude_clause = f"AND A.account NOT IN ({placeholders})"
        params.extend(already_processed)

    sql = f"""
    UPDATE TOP (1) mst.execute_pcs
    SET account = Target.account
    OUTPUT inserted.account
    FROM mst.execute_pcs AS P
    CROSS APPLY (
        SELECT TOP 1 A.account
        FROM mst.ebay_accounts A
        WHERE ISNULL(A.is_excluded, 0) = 0
          {exclude_clause}
          AND NOT EXISTS (
              SELECT 1
              FROM mst.execute_pcs E2
              WHERE E2.account = A.account
          )
        ORDER BY A.account ASC
    ) AS Target
    WHERE P.execute_pc = ? AND P.is_active = 1 AND P.account IS NULL
    """

    params.append(current_pc)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        conn.commit()

    return row[0].strip() if row else None


def release_listing_fetch_pc(conn, current_pc):
    """このPCの execute_pcs 占有を解除する（担当アカウントをNULLに戻す）。"""

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE mst.execute_pcs
            SET account = NULL
            WHERE execute_pc = ?
        """, current_pc)
    conn.commit()


def fetch_and_store_active_listings_for_account(conn, account, fetched_at):
    """
    1アカウント分のActive Listingを取得し、ext.ebay_active_download へ保存する。

    流れ: トークン取得 → eBay API全ページ取得 → 取得成功確認
          → DELETE → INSERT → COMMIT。

    トークン取得失敗・API取得失敗・0件取得の場合は、DELETE・INSERTのどちらも
    行わずFalseを返す（既存データを誤って消さないため）。
    成功時はTrueを返す。例外はこの関数の中で吸収し、呼び出し元へは伝播させない。
    """

    print(f"[START] account={account}")

    # --- トークン取得 ---
    try:
        token = get_access_token_new(account)
    except Exception as e:
        print(f"[ERROR] account={account} トークン取得中に例外が発生しました: {e}")
        return False

    if not token:
        print(f"[ERROR] account={account} トークン取得に失敗したためDB更新を中止")
        return False

    print(f"[TOKEN] account={account} success")

    # --- eBay APIから全ページ取得 ---
    try:
        items = get_active_listings(account)
    except Exception as e:
        print(f"[ERROR] account={account} Active Listing取得中に例外が発生しました: {e}")
        return False

    total_count = len(items)
    print(f"[FETCH] account={account} total_count={total_count}")

    # --- 0件は異常として扱い、DELETE/INSERTは行わない ---
    if total_count == 0:
        print(f"[ERROR] account={account} Active Listing取得結果が0件のためDB更新を中止")
        return False

    # --- DELETE → INSERT → COMMIT ---
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ext.ebay_active_download WHERE account = ?", account)
            insert_items(cur, account, items, fetched_at)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] account={account} DB保存中に例外が発生しました: {e}")
        return False

    print(f"[SAVE] account={account} insert_count={total_count}")

    # --- 保存後の確認 ---
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS row_count, MAX(fetched_at) AS latest_fetched_at
            FROM ext.ebay_active_download
            WHERE account = ?
        """, account)
        row = cur.fetchone()

    print(f"[VERIFY] account={account} db_count={row[0]} latest_fetched_at={row[1]}")
    print(f"[END] account={account} success")

    return True


def run_for_this_pc():
    """
    【現在は未使用・将来のVPS分散再開用に保持】

    このPC(execute_pc)に割り当てられた1アカウントだけ、Active Listingを取得する。
    execute_pcs は担当アカウントを1件決定するためだけに使用し、
    1プロセス=1アカウントの処理で終了する（処理後に次のアカウントは取得しない）。
    """

    current_pc = socket.gethostname().strip()
    conn = get_sql_server_connection()

    fetched_at = datetime.datetime.now()

    try:
        account = fetch_next_account_for_listing_fetch(conn, current_pc, set())

        if not account:
            print("[INFO] 取得対象アカウントがありません。終了します。")
            return

        try:
            fetch_and_store_active_listings_for_account(conn, account, fetched_at)
        finally:
            release_listing_fetch_pc(conn, current_pc)

    finally:
        conn.close()


# =========================
# ローカル1プロセスによる直列実行（現在の標準実行経路）
# =========================
SQL_SELECT_ALL_TARGET_ACCOUNTS = """
SELECT account
FROM mst.ebay_accounts
WHERE ISNULL(is_excluded, 0) = 0
ORDER BY account
"""


def get_target_accounts():
    """is_excluded=0（削除対象外でない）の全アカウントを account 昇順で取得する。"""

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_ALL_TARGET_ACCOUNTS)
            rows = cur.fetchall()
        return [row[0].strip() for row in rows]
    finally:
        conn.close()


def run_serial_local():
    """
    ローカル1プロセスで、is_excluded=0 の全アカウントを1件ずつ順番に処理する。

    VPSへの分散・execute_pcsによるアカウント割当は行わない。
    1アカウントの失敗（トークン取得失敗・API取得失敗・0件取得）は、
    そのアカウントのDB更新をスキップするだけで、残りのアカウントの処理は継続する。
    """

    start_time = datetime.datetime.now()

    accounts = get_target_accounts()
    fetched_at = datetime.datetime.now()

    conn = get_sql_server_connection()

    results = {}

    try:
        for account in accounts:
            try:
                results[account] = fetch_and_store_active_listings_for_account(conn, account, fetched_at)
            except Exception as e:
                # 1アカウントで想定外の例外が起きても、残りのアカウントの処理は続ける
                print(f"[ERROR] account={account} 想定外の例外が発生しました: {e}")
                results[account] = False
    finally:
        conn.close()

    elapsed = datetime.datetime.now() - start_time

    print("\n=== 結果サマリ ===")
    for account, ok in results.items():
        print(f"account={account}: {'success' if ok else 'failed'}")

    print(f"Total elapsed: {elapsed}")


if __name__ == "__main__":
    run_serial_local()
