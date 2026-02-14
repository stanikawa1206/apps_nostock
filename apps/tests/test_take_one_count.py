from datetime import datetime
from apps.common.utils import get_sql_server_connection
from apps.publish.publish_ebay import debug_render_sql_access

PRESET = "ヴィトン長財布M"
LOW_COST = 43817
HIGH_COST = 97947

TEST_SQL = """
SELECT COUNT(*)
FROM trx.vendor_item v
LEFT JOIN mst.seller s
    ON s.vendor_name = v.vendor_name
   AND s.seller_id   = v.seller_id
WHERE
    v.preset = ?
    AND v.status = N'販売中'
    AND ISNULL(v.出品不可flg, 0) = 0
    AND ISNULL(v.[出品状況], N'') <> N'配送条件NG'
    AND ISNULL(v.[出品状況], N'') <> N'NG(危険素材)'
    AND ISNULL(v.[出品状況], N'') <> N'NG(GA補色)'

    AND (
        v.last_updated_str IS NULL
        OR NOT (
            v.last_updated_str LIKE N'%ヶ月前%'
            OR v.last_updated_str LIKE N'%か月前%'
            OR v.last_updated_str LIKE N'%半年以上前%'
        )
    )

    AND v.price BETWEEN ? AND ?

    AND (
        v.processing_at IS NULL
        OR v.processing_at < ?
        OR (
            v.processing_by = ?
            AND v.processing_at < ?
        )
    )

    AND NOT EXISTS (
        SELECT 1
        FROM trx.listings l
        WHERE l.vendor_name = v.vendor_name
          AND l.vendor_item_id = v.vendor_item_id
    )

    AND (
        s.seller_id IS NULL
        OR
        (
            (v.vendor_name = N'メルカリ'
             AND (
                 s.rating_count >= 50
                 OR (
                     s.rating_count < 50
                     AND (
                         v.last_ng_at IS NULL
                         OR DATEADD(
                             day,
                             CASE
                                 WHEN s.rating_count >= 45 THEN 1
                                 WHEN s.rating_count >= 30 THEN 7
                                 WHEN s.rating_count >= 10 THEN 14
                                 ELSE 30
                             END,
                             v.last_ng_at
                         ) <= SYSDATETIME()
                     )
                 )
             )
            )
            OR
            (v.vendor_name = N'メルカリshops'
             AND (
                 s.rating_count >= 20
                 OR (
                     s.rating_count < 20
                     AND (
                         v.last_ng_at IS NULL
                         OR DATEADD(
                             day,
                             CASE
                                 WHEN s.rating_count >= 18 THEN 1
                                 WHEN s.rating_count >= 12 THEN 7
                                 WHEN s.rating_count >= 5  THEN 14
                                 ELSE 30
                             END,
                             v.last_ng_at
                         ) <= SYSDATETIME()
                     )
                 )
             )
            )
        )
    );
"""



def main():
    conn = get_sql_server_connection()
    start_time = datetime.now()
    processing_by = "TEST_WORKER"

    params = [
        PRESET,
        LOW_COST,
        HIGH_COST,
        start_time,
        processing_by,
        start_time,
    ]

    print("\n" + "="*100)
    print(debug_render_sql_access(TEST_SQL, params))
    print("="*100 + "\n")

    with conn.cursor() as cur:
        cur.execute(TEST_SQL, params)
        count = cur.fetchone()[0]

    print("preset:", PRESET)
    print("start_time:", start_time)
    print("候補件数:", count)

    conn.close()

if __name__ == "__main__":
    main()
