# -*- coding: utf-8 -*-
"""
出品枠確保処理

指定したアカウントについて、出品枠を確保する。
現時点では「watchers=0 の出品を削除する」ことで枠を確保しているが、
将来的には --count 指定・必要枠数だけ確保する仕様・LIMIT判定などを
追加していく前提の構成にしている。
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import delete_items_from_ebay_batch


# ===== 設定 =====
# account -> 今回確保したい出品枠数
LISTING_SPACE_CONFIG = {
    "BUZZ②": 12,
}


MAX_WORKERS = 2
BATCH_SIZE  = 10


# ===== 出品枠確保候補取得 =====
SQL_SELECT_CANDIDATES = """
SELECT TOP (?) listing_id
FROM ext.ebay_active_download
WHERE
    account = ?
    AND LEN(vendor_item_id) >= 6
    AND watchers = 0
ORDER BY start_time
"""


def get_listing_space_candidates(account, limit):

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_CANDIDATES, limit, account)
            rows = cur.fetchall()

        item_ids = [str(row[0]) for row in rows]

        print(f"=== {account} 出品枠確保候補件数: {len(item_ids)} ===")
        for iid in item_ids:
            print(f"  候補 item_id={iid}")

        return item_ids

    finally:
        conn.close()


# ===== 出品枠確保処理 =====
def secure_listing_space_for_account(account, item_ids):

    if not item_ids:
        return account, 0

    conn = get_sql_server_connection()
    secured = 0

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

                        cur.execute("""
                            DELETE FROM ext.ebay_active_download
                            WHERE account = ?
                            AND listing_id = ?
                        """, (account, iid))


                conn.commit()

                secured += len(ok_ids)

            idx += BATCH_SIZE

        return account, secured

    finally:
        conn.close()


# ===== メイン =====
def make_listing_space(config=None):
    """
    指定アカウント分の出品枠を確保する。

    config: {account: 確保したい件数} の辞書。
            省略時は LISTING_SPACE_CONFIG を使う。
            将来 daily_check_publish.py から必要件数を渡せるようにするための拡張ポイント。
    """

    target_config = config if config is not None else LISTING_SPACE_CONFIG

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for account, space_count in target_config.items():

            print(f"\n▶ {account}: {space_count}件分の出品枠確保対象取得")

            item_ids = get_listing_space_candidates(account, space_count)

            futures.append(
                executor.submit(secure_listing_space_for_account, account, item_ids)
            )

        for f in as_completed(futures):
            results.append(f.result())

    print("\n=== 出品枠確保結果 ===")
    total = 0
    for account, cnt in results:
        print(f"{account}: {cnt}件分の出品枠を確保")
        total += cnt

    print(f"合計確保: {total}")

    return dict(results)


def run_make_listing_space(account, count):
    """
    他モジュール（publish_manager.py 等）から呼び出すための、
    「1アカウント分だけ出品枠を確保する」シンプルな公開関数。

    内部的には既存の make_listing_space() にそのまま委譲するだけで、
    削除ロジック自体（候補の選び方・削除の実行方法）は一切変更しない。

    account: 出品枠を確保したいアカウント名
    count:   確保したい件数（呼び出し側で計算したremaining等）
    戻り値:  実際に確保できた件数（0件の場合や、countに届かない場合もある）
    """

    results = make_listing_space(config={account: count})
    return results.get(account, 0)


if __name__ == "__main__":
    make_listing_space()
