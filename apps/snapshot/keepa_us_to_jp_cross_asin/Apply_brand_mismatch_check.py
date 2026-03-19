# -*- coding: utf-8 -*-
from my_utils import get_sql_server_connection
import os
import csv

# ==========================================
# 設定: 入力パス
# ==========================================
OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin\brand"
CSV_FILE = os.path.join(OUTPUT_DIR, "brand_mismatch_check.csv")

# ==========================================
# SQL定義
# ==========================================

# 1. ブランドマスタの更新 (UPSERT)
SQL_UPSERT_MASTER = """
MERGE mst.amazon_brand AS tgt
USING (SELECT ? AS brand, ? AS [rank]) AS src
ON tgt.brand = src.brand
WHEN MATCHED THEN
    UPDATE SET [rank] = src.[rank], last_seen_at = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (brand, [rank], last_seen_at) VALUES (src.brand, src.[rank], SYSDATETIME());
"""

# 2. トランザクションのランク更新 (現在のブランド名のままランクのみ更新)
SQL_UPDATE_TRX_RANK = """
UPDATE trx.amazon_cross_market_asin
SET wakarunda = ?, last_seen_at = SYSDATETIME()
WHERE jp_brand = ?
"""

def main():
    if not os.path.exists(CSV_FILE):
        print(f"エラー: ファイルが見つかりません: {CSV_FILE}")
        return

    conn = get_sql_server_connection()
    cursor = conn.cursor()

    applied_count = 0
    print(f"\n[Start] CSVの反映を開始: {CSV_FILE}")

    try:
        with open(CSV_FILE, mode='r', encoding='cp932', newline='') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # 4列目の「OK」が '1' の場合のみ処理
                if row.get("OK") == "1":
                    input_brand = row["input brand"]
                    wakarunda_brand = row["wakarunda brand"]
                    rank = row["rank"]

                    print(f"反映対象: {input_brand} / {wakarunda_brand} -> Rank: {rank}")

                    try:
                        # --- A. マスターへの反映 ---
                        # 1. input brand をマスターに登録/更新
                        cursor.execute(SQL_UPSERT_MASTER, [input_brand, rank])
                        
                        # 2. wakarunda brand もマスターに登録/更新 (名称が異なる場合)
                        if input_brand != wakarunda_brand:
                            cursor.execute(SQL_UPSERT_MASTER, [wakarunda_brand, rank])

                        # --- B. トランザクションへの反映 ---
                        # 現在の jp_brand (input brand) に対してランクを書き込む
                        cursor.execute(SQL_UPDATE_TRX_RANK, [rank, input_brand])
                        
                        conn.commit()
                        applied_count += 1
                        print(f"  -> 成功")
                    except Exception as db_e:
                        print(f"  [DB Error] {input_brand} の更新に失敗: {db_e}")
                        conn.rollback()

        print(f"\n[Finish] 完了しました。反映件数: {applied_count} 行分 (マスターとトランザクションを更新)")

    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()