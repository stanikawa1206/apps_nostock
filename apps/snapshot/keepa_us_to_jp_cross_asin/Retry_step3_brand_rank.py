# -*- coding: utf-8 -*-
from my_utils import get_sql_server_connection
import wakarunda_utils
import os
import time
import pandas as pd

# ==========================================
# 設定: 出力パス
# ==========================================
OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin\brand"
EXCEL_FILE = os.path.join(OUTPUT_DIR, "brand_recheck_all.xlsx")

# SQL定義（元のコードから継承）
SQL_SELECT_MASTER_E = """
SELECT m.brand, MIN(t.asin) as sample_asin
FROM mst.amazon_brand m WITH (NOLOCK)
INNER JOIN trx.amazon_cross_market_asin t WITH (NOLOCK) ON m.brand = t.jp_brand
WHERE m.[rank] = 'E'
GROUP BY m.brand
"""
SQL_UPDATE_MASTER = "UPDATE mst.amazon_brand SET [rank] = ?, last_seen_at = SYSDATETIME() WHERE brand = ?"
SQL_SYNC_MASTER_TO_TRX = """
UPDATE t
SET t.wakarunda = m.[rank], t.last_seen_at = SYSDATETIME()
FROM trx.amazon_cross_market_asin t
INNER JOIN mst.amazon_brand m ON t.jp_brand = m.brand
WHERE (t.wakarunda = 'E' OR t.wakarunda IS NULL) AND m.[rank] <> 'E'
"""
SQL_SELECT_TRX_N = "SELECT asin, jp_brand FROM trx.amazon_cross_market_asin WHERE wakarunda = 'N'"
SQL_UPDATE_TRX = "UPDATE trx.amazon_cross_market_asin SET wakarunda = ?, last_seen_at = SYSDATETIME() WHERE asin = ?"
SQL_UPSERT_MASTER = """
MERGE mst.amazon_brand AS tgt
USING (SELECT ? AS brand, ? AS [rank]) AS src
ON tgt.brand = src.brand
WHEN MATCHED THEN
    UPDATE SET [rank] = src.[rank], last_seen_at = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (brand, [rank], last_seen_at) VALUES (src.brand, src.[rank], SYSDATETIME());
"""

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # --- STEP 1: 全対象（E & N）の抽出とExcel土台作成 ---
    print("\n[Step 1] 調査対象を抽出しています...")
    
    # Master E 抽出
    cursor.execute(SQL_SELECT_MASTER_E)
    list_e = [{"type": "MASTER_E", "id_val": r.brand, "asin": r.sample_asin} for r in cursor.fetchall()]
    
    # Transaction N 抽出
    cursor.execute(SQL_SELECT_TRX_N)
    list_n = [{"type": "TRX_N", "id_val": r.jp_brand, "asin": r.asin} for r in cursor.fetchall()]
    
    all_targets = list_e + list_n
    if not all_targets:
        print("対象データがありません。")
        return

    df = pd.DataFrame(all_targets)
    df["page_brand"] = ""
    df["new_rank"] = ""
    df["status"] = "waiting"

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    df.to_excel(EXCEL_FILE, index=False)
    print(f"-> Excel作成完了: {EXCEL_FILE} (全 {len(df)} 件)")

    # --- STEP 2: 判定処理の実行 ---
    wakarunda_utils.launch_fresh_chrome()

    for index, row in df.iterrows():
        t_type = row["type"]
        db_brand = row["id_val"]
        asin = row["asin"]

        print(f"[{index+1}/{len(df)}] {t_type} 判定中: {db_brand} ({asin})")
        
        try:
            rank, page_brand = wakarunda_utils.fetch_rank_and_brand_by_asin(asin)
            df.at[index, "page_brand"] = page_brand
            df.at[index, "new_rank"] = rank

            # --- 個別更新ロジック ---
            if t_type == "MASTER_E":
                if db_brand == page_brand and rank not in ["判定不能", "E"]:
                    cursor.execute(SQL_UPDATE_MASTER, [rank, db_brand])
                    conn.commit()
                    df.at[index, "status"] = "Master Updated"
                else:
                    df.at[index, "status"] = "Mismatch/E maintained"

            elif t_type == "TRX_N":
                if db_brand == page_brand:
                    new_rank = rank if rank not in ["判定不能", "E"] else "E"
                    cursor.execute(SQL_UPDATE_TRX, [new_rank, asin])
                    cursor.execute(SQL_UPSERT_MASTER, [db_brand, new_rank])
                    conn.commit()
                    df.at[index, "status"] = f"N Recovered ({new_rank})"
                else:
                    df.at[index, "status"] = "Mismatch (N maintained)"

        except Exception as e:
            print(f"  -> Error: {e}")
            df.at[index, "status"] = f"Error: {e}"

        # 1件ごとにExcelを保存
        df.to_excel(EXCEL_FILE, index=False)
        time.sleep(1)

    # --- STEP 3: 最後にマスタの修正をトランザクションへ一括反映 (E機能の仕上げ) ---
    print("\n[Step 3] マスタの最新ランクをトランザクションへ反映中...")
    cursor.execute(SQL_SYNC_MASTER_TO_TRX)
    conn.commit()

    conn.close()
    print(f"\n=== 全工程完了: {EXCEL_FILE} ===")

if __name__ == "__main__":
    main()