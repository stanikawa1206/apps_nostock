# -*- coding: utf-8 -*-
from my_utils import get_sql_server_connection
import wakarunda_utils
import os
import time
import csv

# ==========================================
# 設定: 出力パス
# ==========================================
OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin\brand"
CSV_FILE = os.path.join(OUTPUT_DIR, "brand_mismatch_check.csv")

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

# 5. UPSERTマスタ (救済成功時用)
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

    # 目視チェック用データの保持 (一意性確保のため set を使用)
    csv_data_set = set()

    # --- STEP 1 & 2: ブランドマスタのE再取得と訂正 ---
    print("\n[Step 1 & 2] ブランドマスタ 'E' の再判定を開始...")
    cursor.execute(SQL_SELECT_MASTER_E)
    master_targets = cursor.fetchall()

    for row in master_targets:
        brand, asin = row.brand, row.sample_asin
        print(f"  再判定中: {brand}")
        rank, page_brand = wakarunda_utils.fetch_rank_and_brand_by_asin(asin)
        
        # 完全一致確認
        if brand == page_brand:
            if rank not in ["判定不能", "E"]:
                cursor.execute(SQL_UPDATE_MASTER, [rank, brand])
                conn.commit()
                print(f"  -> マスタ訂正完了: {rank}")
        else:
            # 不一致（N）の場合のみCSV用データに追加
            print(f"  -> [Mismatch] 不一致につき記録対象: {page_brand}")
            csv_data_set.add((brand, page_brand, rank))
            
        time.sleep(1)

    # --- STEP 3: トランザクションへ反映 ---
    print("\n[Step 3] マスタの最新ランクをトランザクションへ一括反映中...")
    cursor.execute(SQL_SYNC_MASTER_TO_TRX)
    conn.commit()

    # --- STEP 4, 5 & 6: トランザクションNの再取得と更新 ---
    print("\n[Step 4-6] トランザクション 'N' の再判定を開始...")
    cursor.execute(SQL_SELECT_TRX_N)
    trx_n_targets = cursor.fetchall()

    for row in trx_n_targets:
        asin, db_brand = row.asin, row.jp_brand
        print(f"  N判定リトライ: {asin} ({db_brand})")
        rank, page_brand = wakarunda_utils.fetch_rank_and_brand_by_asin(asin)

        # 完全一致確認
        if db_brand == page_brand:
            new_rank = rank if rank not in ["判定不能", "E"] else "E"
            print(f"  -> [救済成功] 完全一致を確認: {new_rank}")
            
            cursor.execute(SQL_UPDATE_TRX, [new_rank, asin])
            cursor.execute(SQL_UPSERT_MASTER, [db_brand, new_rank])
            conn.commit()
        else:
            # 不一致継続（N）の場合のみCSV用データに追加
            print(f"  -> [維持] 不一致継続(目視対象): {page_brand}")
            csv_data_set.add((db_brand, page_brand, rank))
            
        time.sleep(1)

    # --- CSV出力処理 ---
    if csv_data_set:
        # ディレクトリの存在確認と作成
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            print(f"ディレクトリを作成しました: {OUTPUT_DIR}")

        print(f"\n[CSV] {CSV_FILE} を作成しています...")
        # input brand (0番目の要素) で昇順ソート
        sorted_list = sorted(list(csv_data_set), key=lambda x: x[0] if x[0] else "")
        
        try:
            with open(CSV_FILE, mode='w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["input brand", "wakarunda brand", "rank"])
                writer.writerows(sorted_list)
            print(f"  -> 出力完了: {len(sorted_list)} 件の不一致を保存しました。")
        except Exception as e:
            print(f"  [CSV Error] ファイルの書き込みに失敗しました: {e}")
    else:
        print("\n[CSV] 不一致（N）のデータがなかったため、出力は行いませんでした。")

    conn.close()
    print("\n=== 全工程が完了しました ===")

if __name__ == "__main__":
    main()