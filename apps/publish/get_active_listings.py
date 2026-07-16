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

publish_ebay.py の fetch_next_account_and_lock() と同じ考え方で、
mst.execute_pcs を使って「1PCにつき1アカウントずつ確保して処理する」方式を取り、
VPS・ローカルで分散して取得できるようにしている。
"""

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
    print("A", flush=True)
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

    先にeBay側から取得してから、DBの削除・挿入を行う。
    取得に失敗した場合はDBを一切変更しない（古いデータを誤って消さないため）。
    """

    items = get_active_listings(account)
    print("B", flush=True)

    with conn.cursor() as cur:
        cur.execute("DELETE FROM ext.ebay_active_download WHERE account = ?", account)
        print("C", flush=True)
        insert_items(cur, account, items, fetched_at)
        print("D", flush=True)
    conn.commit()
    print("E", flush=True)

    print("F", flush=True)
    return len(items)


def run_for_this_pc():
    """
    このPC(execute_pc)に割り当てられた分のアカウントについて、
    Active Listingを取得し終えるまで繰り返す。

    publish_ebay.py のメインループと同じ「1件確保→処理→解放」を、
    担当できるアカウントが無くなるまで繰り返す構成。
    VPS・ローカルそれぞれでこの関数を実行することで、
    全アカウント分の取得が自然に分散される。
    """

    current_pc = socket.gethostname().strip()
    conn = get_sql_server_connection()

    fetched_at = datetime.datetime.now()

    # このPCが今回のrunで既に処理したaccountの集合（DBには保存しない、プロセス内メモリのみ）。
    # これが無いと、release_listing_fetch_pc()で解放した直後の同じアカウントを
    # fetch_next_account_for_listing_fetch()が毎回また返してしまい、ループが終わらない。
    already_processed = set()

    try:
        while True:
            account = fetch_next_account_for_listing_fetch(conn, current_pc, already_processed)

            if not account:
                print("[INFO] 取得対象アカウントがありません。終了します。")
                print("K0", flush=True)
                break

            print(f"▶ 開始: {account}")

            try:
                count = fetch_and_store_active_listings_for_account(conn, account, fetched_at)
                print("G", flush=True)
                print(f"  件数: {count}")
                print("H", flush=True)
            except Exception as e:
                # 1アカウントの取得失敗で全体を止めない。
                # このPCの already_processed には加えるため、このPC自身は
                # 同じアカウントを無限に再試行しない。ただし他のPCの
                # already_processed には影響しないため、空いたPCがあれば
                # そちらで改めて取得を試みることはできる。
                print(f"❌ {account} のActive Listing取得に失敗しました: error={e}")
            finally:
                already_processed.add(account)
                print("I", flush=True)
                release_listing_fetch_pc(conn, current_pc)
                print("J", flush=True)

        print("K", flush=True)
        print("✅ 完了")
        print("L", flush=True)

    finally:
        print("M", flush=True)
        conn.close()
        print("N", flush=True)


if __name__ == "__main__":
    run_for_this_pc()
