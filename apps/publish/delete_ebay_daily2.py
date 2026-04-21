# -*- coding: utf-8 -*-

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import delete_items_from_ebay_batch


# ===== 設定 =====
TARGET_ACCOUNT = "谷川②"
DELETE_LIMIT   = 1
MAX_WORKERS    = 2
BATCH_SIZE     = 10


# ===== 取得 =====
SQL_SELECT = """
SELECT
    account,
    listing_id
FROM ext.active_download_new
WHERE
    account = ?
    AND LEN(vendor_item_id) >= 6
    AND ISNULL(watchers, 0) = 0
ORDER BY start_time ASC
"""


def fetch_targets():
    with get_sql_server_connection() as conn:
        cur = conn.cursor()
        cur.execute(SQL_SELECT, (TARGET_ACCOUNT,))
        rows = cur.fetchall()

    return [(str(r[0]), str(r[1])) for r in rows]


# ===== 削除 =====
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
def main():

    pairs = fetch_targets()

    if not pairs:
        print("対象なし")
        return

    # ===== accountごとに分割 =====
    by_account = defaultdict(list)
    for acc, iid in pairs:
        by_account[acc].append(iid)

    # ===== 削除数制限 =====
    for acc in by_account:
        by_account[acc] = by_account[acc][:DELETE_LIMIT]

    total = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(delete_for_account, acc, ids): acc
            for acc, ids in by_account.items()
        }

        for fut in as_completed(futures):
            acc = futures[fut]
            a, cnt = fut.result()
            print(f"{a}: {cnt}件削除")
            total += cnt

    print(f"合計削除: {total}")


if __name__ == "__main__":
    main()