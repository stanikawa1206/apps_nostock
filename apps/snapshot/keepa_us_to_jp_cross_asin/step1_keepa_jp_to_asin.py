# step1_keepa_jp_to_asin.py
# -*- coding: utf-8 -*-
import sys
import os
from datetime import datetime
from my_utils import get_sql_server_connection, keepa_request
from step0_category_manager import get_next_category, update_category_status

# ==========================================
# 設定: 検索条件 & ログ設定
# ==========================================
QUERY_LIMIT = 10000

# --- ログ出力設定 ---
LOG_DIR = r"X:\apps\snapshot\keepa_us_to_jp_cross_asin\logs"
# ------------------

# 2. 価格フィルタ (新品価格 90日平均)
PRICE_MIN_JPY = 5000
PRICE_MAX_JPY = 200000

# 3. サイズ・重量フィルタ
MAX_EDGE_MM = 1600
MAX_WEIGHT_G = 30000

# ==========================================
# ログ出力用関数
# ==========================================
def write_execution_log(rank_range, cat_id, count):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(LOG_DIR, f"{today}.log")
    
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{now_str}] Rank: {rank_range} | CatID: {cat_id} | Saved: {count} items\n"
    
    with open(log_file, mode="a", encoding="utf-8") as f:
        f.write(log_line)

# ==========================================
# SQL定義
# ==========================================
SQL_UPSERT = """
MERGE trx.amazon_cross_market_asin WITH (ROWLOCK) AS tgt
USING (SELECT ? AS asin, ? AS jp_category_id) AS src
ON tgt.asin = src.asin
WHEN MATCHED THEN
    UPDATE SET 
        last_seen_at = SYSDATETIME(), 
        keepa_last_caught_at = SYSDATETIME(),  -- ←追加
        jp_category_id = src.jp_category_id
WHEN NOT MATCHED THEN
    INSERT (asin, last_seen_at, keepa_last_caught_at, jp_category_id, us_existence)
    VALUES (src.asin, SYSDATETIME(), SYSDATETIME(), src.jp_category_id, 0); -- ←追加
"""

def save_asins_to_db(cursor, asin_list, cat_id):
    if not asin_list:
        return 0
    
    count = 0
    for asin in asin_list:
        try:
            cursor.execute(SQL_UPSERT, [asin, cat_id])
            cursor.connection.commit() 
            count += 1
        except Exception as e:
            print(f"Error {asin}: {e}")
            cursor.connection.rollback()
    return count

def fetch_and_save_recursive(cat_id, min_rank, max_rank, cursor):
    print(f"Fetching Cat: {cat_id} | Rank(Avg180): {min_rank}-{max_rank}")
    
    selection = {
        "rootCategory": cat_id,
        "productType": 0,                           # 物理的な商品
        "avg365_SALES_gte": min_rank,               # 180日 → 365日に変更
        "avg365_SALES_lte": max_rank,               # 180日 → 365日に変更
        "avg90_NEW_gte": PRICE_MIN_JPY,
        "avg90_NEW_lte": PRICE_MAX_JPY,
        "fbaFees_lte": 650,                         # サイズ・重量を削除し、FBA料金(上限650円)を追加
        "perPage": QUERY_LIMIT
    }
    
    res = keepa_request("query", params={"domain": 5}, data=selection)
    
    total = res.get("totalResults", 0)
    asin_list = res.get("asinList", [])
    
    if total > QUERY_LIMIT and (max_rank - min_rank) > 1:
        print(f"   [Split] Hit {total} > Limit. Splitting range...")
        mid = (min_rank + max_rank) // 2
        
        count_1 = fetch_and_save_recursive(cat_id, min_rank, mid, cursor)
        count_2 = fetch_and_save_recursive(cat_id, mid + 1, max_rank, cursor)
        
        return count_1 + count_2
    else:
        print(f"   -> Got {len(asin_list)} items. Saving...")
        saved_count = save_asins_to_db(cursor, asin_list, cat_id)
        
        rank_range_str = f"{min_rank}-{max_rank}"
        write_execution_log(rank_range_str, cat_id, saved_count)
        
        return saved_count

# step1_keepa_jp_to_asin.py の main() 関数部分のみ抜粋

def main():
    if len(sys.argv) < 3:
        print("エラー: ランキング閾値が指定されていません。")
        print("使用法: python step1_keepa_jp_to_asin.py [min_rank] [max_rank]")
        sys.exit(1)
        
    try:
        target_min_rank = int(sys.argv[1])
        target_max_rank = int(sys.argv[2])
    except ValueError:
        print("エラー: ランクは数値で指定してください。")
        sys.exit(1)

    print(f"=== Step1: カテゴリー連続処理を開始 (指定ランク: {target_min_rank} - {target_max_rank}) ===")
    
    while True:
        # 1. DBから次のカテゴリーを取得
        category = get_next_category(target_min_rank, target_max_rank)
        
        if not category:
            print(f"\n=== 全ての対象カテゴリーの処理が完了しました (閾値 {target_min_rank}-{target_max_rank}) ===")
            break
            
        target_category_id = category["id"]
        target_category_name = category["name"]
        
        print(f"\n実行開始 - カテゴリー: {target_category_name} (ID: {target_category_id})")
        
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        
        try:
            # 2. 再帰取得と保存を実行
            total_saved = fetch_and_save_recursive(target_category_id, target_min_rank, target_max_rank, cursor)
            print(f"=== DB保存完了: 合計 {total_saved}件 (CatID: {target_category_id}) ===")
            
            # 3. 【変更箇所】1カテゴリーの処理が正常に完了した直後にマスタを更新する
            update_category_status(target_category_id, target_min_rank, target_max_rank)
            print(f"=== マスタ更新完了: 実行日と閾値を反映しました ===")
            
        except Exception as e:
            print(f"Fatal Error: {e}")
            conn.rollback()
            # エラー時はマスタが更新されないため、次回再実行の対象として残ります
        finally:
            conn.close()

if __name__ == "__main__":
    main()