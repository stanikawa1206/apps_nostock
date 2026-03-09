import time
import csv
from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import update_ebay_price
from apps.adapters.mercari_item_status import handle_listing_delete

MODE = "API"   # "API" or "CSV"


SQL_SELECT_UNDERPRICED = """
SELECT
    ext.ebay_active_download.listing_id,
    ext.ebay_active_download.vendor_item_id,
    ext.ebay_active_download.account,
    trx.listings.vendor_name,
    trx.listings.is_deleted,
    ext.ebay_active_download.[Start price] AS actual_price_usd,
    trx.listings.start_price as expected_price_usd,
    ext.ebay_active_download.[Start price] - trx.listings.start_price AS price_diff,
    trx.vendor_item.price AS cost_jpy
FROM ext.ebay_active_download

INNER JOIN trx.listings
    ON ext.ebay_active_download.listing_id = trx.listings.listing_id

INNER JOIN trx.vendor_item
    ON trx.listings.vendor_item_id = trx.vendor_item.vendor_item_id

WHERE
    trx.listings.is_deleted = 0
    AND ext.ebay_active_download.[Start price] <> trx.listings.start_price
"""


def main():

    conn = get_sql_server_connection()
    cur = conn.cursor()

    cur.execute(SQL_SELECT_UNDERPRICED)
    rows = cur.fetchall()

    print("対象件数:", len(rows))



    # =========================
    # CSV生成モード
    # =========================
    if MODE == "CSV":

        files = {}

        for row in rows:

            account = row.account
            listing_id = row.listing_id
            price = float(row.expected_price_usd)

            if account not in files:
                f = open(f"price_update_{account}.csv", "w", newline="", encoding="utf-8")
                writer = csv.writer(f)
                writer.writerow(["Action", "ItemID", "StartPrice"])
                files[account] = (f, writer)

            writer = files[account][1]
            writer.writerow(["Revise", listing_id, price])

        for f, _ in files.values():
            f.close()

        print("CSV作成完了")
        return

    # =========================
    # APIモード
    # =========================
    for row in rows:

        account = row.account
        listing_id = row.listing_id
        sku = row.vendor_item_id
        vendor_name = row.vendor_name
        new_price = float(row.expected_price_usd)

        print(f"price update: listing_id={listing_id} sku={sku} price={new_price}")

        res = update_ebay_price(
            account=account,
            ebay_item_id=listing_id,
            new_price_usd=new_price,
            sku=sku
        )

        if not res.get("success"):

            if res.get("error") == "inventory_put_failed":
                raw = res.get("raw", {})
                errors = raw.get("putOffer", {}).get("errors", [])

                if errors:
                    error_id = errors[0].get("errorId")

                    # small image / duplicate扱い
                    if error_id == 25002:
                        print(
                            "SMALL IMAGE → DELETE:",
                            "account=", account,
                            "listing_id=", listing_id,
                            "sku=", sku
                        )

                        handle_listing_delete(conn, sku, vendor_name)

                    # ebay internal error
                    if error_id == 25001:
                        print("RETRY (EBAY INTERNAL ERROR)")
                        time.sleep(2)
                        continue

            print("ERROR:", res)

        print(res)
        time.sleep(1) 



    conn.close()


if __name__ == "__main__":
    main()