from apps.common.utils import (
    get_sql_server_connection,
    compute_start_price_usd
)


SQL_SELECT = """
SELECT
    ext.ebay_active_download.vendor_item_id,
    ext.ebay_active_download.account,
    trx.vendor_item.price AS cost_jpy,
    trx.vendor_item.preset,
    mst.presets_lookup.mode
FROM ext.ebay_active_download

INNER JOIN trx.vendor_item
ON trx.vendor_item.vendor_item_id = ext.ebay_active_download.vendor_item_id

INNER JOIN mst.presets_lookup
ON mst.presets_lookup.preset = trx.vendor_item.preset
"""


SQL_UPDATE = """
UPDATE trx.listings
SET start_price = ?
WHERE account = ?
AND vendor_item_id = ?
AND is_deleted = 0
"""


def main():

    conn = get_sql_server_connection()
    cur = conn.cursor()

    cur.execute(SQL_SELECT)
    rows = cur.fetchall()

    print("対象件数:", len(rows))

    updated = 0
    skipped = 0

    for row in rows:

        vendor_item_id = row.vendor_item_id
        account = row.account
        cost_jpy = row.cost_jpy
        mode = row.mode

        price = compute_start_price_usd(
            cost_jpy=cost_jpy,
            mode=mode,
            low_usd_target=None,
            high_usd_target=None
        )

        if price is None:
            skipped += 1
            continue

        cur.execute(
            SQL_UPDATE,
            float(price),
            account,
            vendor_item_id
        )

        if cur.rowcount == 0:
            skipped += 1
            continue

        updated += 1

    conn.commit()
    conn.close()

    print("更新:", updated)
    print("スキップ:", skipped)


if __name__ == "__main__":
    main()