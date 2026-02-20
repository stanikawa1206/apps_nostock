# step1_brand_all_checker.py
# -*- coding: utf-8 -*-
import sys
from datetime import datetime
from my_utils import get_sql_server_connection, keepa_request

# ==========================================
# 1. SQL定義
# ==========================================

# seen_date が NULL のレコードをすべて取得
SQL_SELECT_NULL_BRANDS = """
SELECT brand, from_rank, to_rank
FROM trx.amazon_export_brand 
WHERE seen_date IS NULL
"""

# 個別のレコードを更新
SQL_UPDATE_BRAND = """
UPDATE trx.amazon_export_brand 
SET number_of_item = ?, seen_date = GETDATE() 
WHERE brand = ? AND from_rank = ? AND to_rank = ?
"""

# ==========================================
# 2. 処理ロジック
# ==========================================

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    try:
        # 1. 対象となる全レコードを取得
        cursor.execute(SQL_SELECT_NULL_BRANDS)
        rows = cursor.fetchall()
        
        total_targets = len(rows)
        if total_targets == 0:
            print("seen_date が NULL のレコードは見つかりませんでした。")
            return

        print(f"合計 {total_targets} 件のブランドを処理します。")

        # 2. ループで順次処理
        for i, row in enumerate(rows, 1):
            brand, f_rank, t_rank = row
            print(f"[{i}/{total_targets}] 処理中: {brand} (Rank: {f_rank} - {t_rank})")

            # Keepa API リクエスト (USドメイン: 1)
            # my_utils.keepa_request がトークン切れ時の待機を自動で行います
            selection = {
                "brand": brand,
                "avg90_SALES_gte": f_rank,
                "avg90_SALES_lte": t_rank,
                "productType": 0,
                "perPage": 10000 
            }
            
            try:
                # Keepa API実行
                res = keepa_request("query", params={"domain": 1}, data=selection)
                
                # 件数の取得
                asin_list = res.get("asinList", [])
                num_items = len(asin_list)
                
                # DB更新
                cursor.execute(SQL_UPDATE_BRAND, [num_items, brand, f_rank, t_rank])
                conn.commit()
                print(f" -> 完了: {num_items}件ヒット")
                
            except Exception as e:
                print(f" -> スキップ (エラー発生): {e}")
                conn.rollback()
                continue

        print("\nすべての処理が終了しました。")

    except Exception as e:
        print(f"致命的なエラー: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()