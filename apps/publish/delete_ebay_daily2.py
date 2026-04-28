# -*- coding: utf-8 -*-

from concurrent.futures import ThreadPoolExecutor, as_completed

from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import delete_items_from_ebay_batch


# ===== 設定 =====
DELETE_CONFIG = {
    "川島": 705,
    "谷川②": 735,
    "谷川③": 470,
}

MAX_WORKERS = 2
BATCH_SIZE  = 10


# ===== 削除対象取得 =====
SQL_SELECT = """
SELECT TOP (?) listing_id
FROM ext.ebay_active_download
WHERE
    account = ?
    AND LEN(vendor_item_id) >= 6
    AND watchers = 0
ORDER BY start_time
"""


def get_delete_targets(account, limit):

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT, limit, account)
            rows = cur.fetchall()

        item_ids = [str(row[0]) for row in rows]

        print(f"=== {account} 取得件数: {len(item_ids)} ===")
        for iid in item_ids:
            print(f"  取得 item_id={iid}")

        return item_ids

    finally:
        conn.close()


# ===== 削除処理 =====
def delete_for_account(account, item_ids):

    if not item_ids:
        return account, 0

    conn = get_sql_server_connection()
    deleted = 0

    try:
        idx = 0
        while idx < len(item_ids):

            batch = item_ids[idx: idx + BATCH_SIZE]

            result = delete_items_from_ebay_batch(account, batch)


            if not isinstance(result, dict):
                print(f"⚠️ {account}: API異常")
                break

            res_list = result.get("results") or []

            ok_ids = []
            for r in res_list:
                if r.get("success"):
                    ok_ids.append(str(r.get("item_id")))

            # ===== DB更新 =====
            if ok_ids:
                with conn.cursor() as cur:
                    for iid in ok_ids:
                        cur.execute("""
                            UPDATE trx.listings
                               SET is_deleted = 1,
                                   deleted_at = SYSDATETIME(),
                                   delete_reason = ?
                             WHERE account = ?
                               AND listing_id = ?
                               AND ISNULL(is_deleted, 0) = 0
                        """, ("定期削除(watch0)", account, iid))

                conn.commit()

                deleted += len(ok_ids)

            idx += BATCH_SIZE

        return account, deleted

    finally:
        conn.close()


# ===== メイン =====
def run():

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for account, delete_limit in DELETE_CONFIG.items():

            print(f"\n▶ {account}: {delete_limit}件削除対象取得")

            item_ids = get_delete_targets(account, delete_limit)

            futures.append(
                executor.submit(delete_for_account, account, item_ids)
            )

        for f in as_completed(futures):
            results.append(f.result())

    print("\n=== 削除結果 ===")
    total = 0
    for account, cnt in results:
        print(f"{account}: {cnt}件削除")
        total += cnt

    print(f"合計削除: {total}")


if __name__ == "__main__":
    run()