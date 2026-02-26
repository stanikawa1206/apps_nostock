import socket
import time
from typing import Any, Dict, Optional

from apps.common.utils import (
    get_sql_server_connection,
    compute_start_price_usd,
)

from apps.adapters.ebay_api import (
    update_ebay_price_rest,
)


from apps.adapters.mercari_item_status import (
    _is_transient_inventory_error,
)

def main():

    conn = get_sql_server_connection()

    sql = """
    SELECT
        ext.account,
        ext.listing_id,
        ext.vendor_item_id,
        ext.[Start price] AS start_price_usd,
        trx.price AS cost_jpy
    FROM ext.ebay_active_download ext
    INNER JOIN trx.vendor_item trx
        ON ext.vendor_item_id = trx.vendor_item_id
    WHERE
        trx.preset NOT LIKE N'%スカーフ%'
        AND ext.[Start price] < 500
    ORDER BY ext.account;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    results = []

    for row in rows:
        account = row[0]
        listing_id = row[1]
        sku = row[2]
        start_price_usd = float(row[3])
        cost_jpy = float(row[4])

        # 手数料差引
        net_usd = start_price_usd - start_price_usd * 0.17 - start_price_usd * 0.15

        # 円換算
        revenue_jpy = 155 * net_usd

        # 粗利
        gross_profit = revenue_jpy - 3300 - cost_jpy

        # FLOOR相当
        gross_profit = int(gross_profit)

        # 利益率
        profit_rate = round(gross_profit / (start_price_usd * 155), 2)

        if profit_rate < 0.09:
            results.append(row)

    print(f"対象件数: {len(results)}")


    # 固定条件
    mode = "DDP"
    low_usd_target = 80
    high_usd_target = 1000

    for row in results:

        account = row[0]
        listing_id = row[1]
        vendor_item_id = row[2]
        cost_jpy = row[4]

        new_price_usd = compute_start_price_usd(
            cost_jpy=cost_jpy,
            mode=mode,
            low_usd_target=low_usd_target,
            high_usd_target=high_usd_target
        )

        if new_price_usd is None:
            print(f"[SKIP] レンジ外 sku={vendor_item_id}")
            continue


        print(f"[START] account={account} sku={vendor_item_id} listing_id={listing_id} new_price={new_price_usd}")

        did_update = False
        resp: Optional[Dict[str, Any]] = None

        for wait in [0, 2, 6, 15]:
            if wait:
                time.sleep(wait)
           
            resp = update_ebay_price_rest(
                account=account,
                sku=vendor_item_id,
                new_price_usd=new_price_usd
            )

            if resp and resp.get("success"):
                did_update = True
                break

            if not _is_transient_inventory_error(resp or {}):
                break

        if not did_update:
            print(
                f"[WARN] 価格更新失敗 sku={vendor_item_id} resp={resp}",
                flush=True
            )
            continue

        print(f"[OK] 更新成功 sku={vendor_item_id}")

        

    conn.close()


if __name__ == "__main__":
    print(f"=== 利益率調整バッチ開始 host={socket.gethostname()} ===")
    main()
    print("=== 完了 ===")