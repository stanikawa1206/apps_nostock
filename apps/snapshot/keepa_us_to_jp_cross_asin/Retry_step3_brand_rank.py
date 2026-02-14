# -*- coding: utf-8 -*-
from my_utils import get_sql_server_connection
import wakarunda_utils
import time

# ==========================================
# SQL定義
# ==========================================

# 1. マスタの 'E' 判定をブランド単位で抽出
SQL_SELECT_MASTER_E = """
SELECT m.brand, MIN(t.asin) as sample_asin
FROM mst.amazon_brand m WITH (NOLOCK)
INNER JOIN trx.amazon_cross_market_asin t WITH (NOLOCK) ON m.brand = t.jp_brand
WHERE m.[rank] = 'E'
GROUP BY m.brand
"""

# 2. マスタ更新 (ブランド指定)
SQL_UPDATE_MASTER = "UPDATE mst.amazon_brand SET [rank] = ?, last_seen_at = SYSDATETIME() WHERE brand = ?"

# 3. マスタからトランザクションへの一括反映
SQL_SYNC_MASTER_TO_TRX = """
UPDATE t
SET t.wakarunda = m.[rank], t.last_seen_at = SYSDATETIME()
FROM trx.amazon_cross_market_asin t
INNER JOIN mst.amazon_brand m ON t.jp_brand = m.brand
WHERE (t.wakarunda = 'E' OR t.wakarunda IS NULL) AND m.[rank] <> 'E'
"""

# 4. トランザクションの 'N' 判定を抽出
SQL_SELECT_TRX_N = """
SELECT TOP 100 asin, jp_brand
FROM trx.amazon_cross_market_asin
WHERE wakarunda = 'N'
"""

# 5. UPSERTマスタ (Nの救済成功時用)
SQL_UPSERT_MASTER = """
MERGE mst.amazon_brand AS tgt
USING (SELECT ? AS brand, ? AS [rank]) AS src
ON tgt.brand = src.brand
WHEN MATCHED THEN
    UPDATE SET [rank] = src.[rank], last_seen_at = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (brand, [rank], last_seen_at) VALUES (src.brand, src.[rank], SYSDATETIME());
"""

# 6. トランザクション更新
SQL_UPDATE_TRX = "UPDATE trx.amazon_cross_market_asin SET wakarunda = ?, last_seen_at = SYSDATETIME() WHERE asin = ?"

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    wakarunda_utils.launch_fresh_chrome()

    # --- STEP 1 & 2: ブランドマスタのE再取得と訂正 ---
    print("\n[Step 1 & 2] ブランドマスタ 'E' の再判定を開始...")
    cursor.execute(SQL_SELECT_MASTER_E)
    master_targets = cursor.fetchall()

    for row in master_targets:
        brand, asin = row.brand, row.sample_asin
        print(f"  再判定中: {brand}")
        rank, page_brand = wakarunda_utils.fetch_rank_and_brand_by_asin(asin)
        
        if rank not in ["判定不能", "E"]:
            cursor.execute(SQL_UPDATE_MASTER, [rank, brand])
            conn.commit()
            print(f"  -> マスタ訂正完了: {rank}")
        time.sleep(1)

    # --- STEP 3: トランザクションへ反映 ---
    print("\n[Step 3] マスタの最新ランクをトランザクションへ一括反映中...")
    cursor.execute(SQL_SYNC_MASTER_TO_TRX)
    print(f"  -> {cursor.rowcount} 件のトランザクションを更新しました。")
    conn.commit()

    # --- STEP 4, 5 & 6: トランザクションNの再取得と更新 ---
    print("\n[Step 4-6] トランザクション 'N' の再判定を開始...")
    cursor.execute(SQL_SELECT_TRX_N)
    trx_n_targets = cursor.fetchall()

    for row in trx_n_targets:
        asin, db_brand = row.asin, row.jp_brand
        print(f"  N判定リトライ: {asin} ({db_brand})")
        rank, page_brand = wakarunda_utils.fetch_rank_and_brand_by_asin(asin)

        # 空白除去のみで完全一致（大文字小文字区別あり）をチェック
        if db_brand.replace(" ", "") == page_brand.replace(" ", ""):
            new_rank = rank if rank not in ["判定不能", "E"] else "E"
            print(f"  -> [救済成功] ブランド一致: {new_rank}")
            
            # トランザクションとマスタの両方を更新
            cursor.execute(SQL_UPDATE_TRX, [new_rank, asin])
            cursor.execute(SQL_UPSERT_MASTER, [db_brand, new_rank])
            conn.commit()
        else:
            print(f"  -> [維持] ブランド不一致継続: {page_brand}")
        time.sleep(1)

    conn.close()
    print("\n=== 全工程が完了しました ===")

if __name__ == "__main__":
    main()