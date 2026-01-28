# -*- coding: utf-8 -*-
"""
英訳（タイトル/説明）生成 → trx.vendor_item に反映するテスト（抽出SQL版）

- 抽出SQLで vendor_item_id を複数取得
- 各 vendor_item_id を input にして
  title_jp / description / preset → default_brand_en → 翻訳 → UPDATE(title_en, description_en)
"""

from __future__ import annotations

import sys
import utils

N = 200

SQL_PICK_TARGETS = f"""
SELECT TOP ({N})
    v.vendor_item_id
FROM trx.vendor_item AS v
LEFT JOIN trx.listings AS l
    ON v.vendor_item_id = l.vendor_item_id
WHERE
    v.preset NOT LIKE N'エルメス%'
    AND v.title_en LIKE N'Hermes%'
    AND v.status = N'販売中'
ORDER BY
    l.account,
    v.preset;
"""



def main() -> int:
    cn = utils.get_sql_server_connection()
    try:
        cur = cn.cursor()

        # ① 対象 vendor_item_id を取得
        cur.execute(SQL_PICK_TARGETS)
        vendor_item_ids = [r[0] for r in cur.fetchall()]

        print(f"TARGET COUNT: {len(vendor_item_ids)}")

        # ② 1件ずつ更新
        for vendor_item_id in vendor_item_ids:
            # trx.vendor_item から取得
            cur.execute(
                """
                SELECT
                    title_jp,
                    description,
                    preset
                FROM trx.vendor_item
                WHERE vendor_item_id = ?
                """,
                (vendor_item_id,),
            )
            row = cur.fetchone()

            title_jp = row[0]
            description_jp = row[1] or ""
            preset = row[2]

            # mst.v_presets から default_brand_en
            cur.execute(
                """
                SELECT
                    default_brand_en
                FROM mst.v_presets
                WHERE preset = ?
                """,
                (preset,),
            )
            row2 = cur.fetchone()
            default_brand_en = row2[0] if row2 else None

            # 翻訳
            title_en = utils.translate_to_english(
                title_jp,
                description_jp,
                expected_brand_en=default_brand_en,
            )

            description_en = utils.generate_ebay_description(
                title_en=title_en,
                description_jp=description_jp,
                expected_brand_en=default_brand_en,
            )

            # UPDATE
            cur.execute(
                """
                UPDATE trx.vendor_item
                SET
                    title_en = ?,
                    description_en = ?
                WHERE vendor_item_id = ?
                """,
                (title_en, description_en, vendor_item_id),
            )

            cn.commit()

            print("----------------------------------------")
            print(f"UPDATED: {vendor_item_id}")
            print(f"preset           : {preset}")
            print(f"default_brand_en : {default_brand_en}")
            print("[title_en]")
            print(title_en)

        print("DONE")
        return 0

    finally:
        try:
            cn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
