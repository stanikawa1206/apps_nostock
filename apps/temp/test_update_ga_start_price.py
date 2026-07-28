"""
GA（Authenticity Guarantee）start_price 一括更新のテスト用スクリプト（Dry Run）。

update_ga_start_price.py と同じ対象抽出条件・同じ compute_start_price_usd 呼び出しを使い、
先頭10件のみを対象に新価格を計算して表示する。

・eBayへの ReviseFixedPriceItem は実行しない
・trx.listings の UPDATE も実行しない
・DB/eBay に一切変更を加えない完全な Dry Run
"""

from decimal import Decimal

from apps.common.utils import get_sql_server_connection, compute_start_price_usd


TEST_LIMIT = 10

SQL_SELECT = """
SELECT
    l.listing_id,
    l.account,
    l.vendor_item_id,
    v.price AS cost_jpy,
    l.start_price AS current_start_price
FROM trx.listings AS l
INNER JOIN trx.vendor_item AS v
    ON l.vendor_item_id = v.vendor_item_id
INNER JOIN mst.presets_lookup AS p
    ON v.preset = p.preset
WHERE
    l.is_deleted = 0
    AND l.start_price >= 500
    AND p.mode = 'GA'
"""


def main():

    conn = get_sql_server_connection()
    cur = conn.cursor()

    cur.execute(SQL_SELECT)
    rows = cur.fetchall()

    conn.close()

    target_count = len(rows)
    test_rows = rows[:TEST_LIMIT]

    print(f"対象件数: {target_count}")
    print("=" * 60)

    for row in test_rows:

        item_id = row.listing_id
        vendor_item_id = row.vendor_item_id
        cost_jpy = row.cost_jpy
        current_price = Decimal(str(row.current_start_price))

        new_price_str = compute_start_price_usd(
            cost_jpy=cost_jpy,
            mode="GA",
            low_usd_target=1,
            high_usd_target=3000,
        )

        print(
            f"item_id={item_id} vendor_item_id={vendor_item_id} "
            f"仕入価格={cost_jpy} 現在価格={current_price} 新価格={new_price_str}"
        )

    print("=" * 60)
    print("対象件数:", target_count)
    print("テスト実行件数:", len(test_rows))


if __name__ == "__main__":
    main()
