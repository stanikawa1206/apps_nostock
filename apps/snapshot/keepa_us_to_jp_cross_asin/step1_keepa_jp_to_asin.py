# step1_keepa_jp_to_asin.py
# -*- coding: utf-8 -*-
import sys
import os
from datetime import datetime
from my_utils import get_sql_server_connection, keepa_request

# ==========================================
# 設定: 検索条件 & ログ設定
# ==========================================
TARGET_CATEGORY_ID = 2229202051
QUERY_LIMIT = 10000           # Keepaの1回あたりの取得上限

# --- ログ出力設定 ---
LOG_DIR = r"X:\apps\snapshot\keepa_us_to_jp_cross_asin\logs"
# ------------------

# 1. ランキング範囲 (180日平均)
RANK_MIN = 1
RANK_MAX = 50000

# 2. 価格フィルタ (新品価格 90日平均)
PRICE_MIN_JPY = 10000
PRICE_MAX_JPY = 300000

# 3. サイズ・重量フィルタ
MAX_EDGE_MM = 1600
MAX_WEIGHT_G = 30000

# ==========================================
# ログ出力用関数
# ==========================================
def write_execution_log(rank_range, cat_id, count):
    """
    指定のフォルダに実行ログを出力する
    内容: ランキング区間、カテゴリーID、書き込み数、書き込み日時
    """
    # フォルダが存在しない場合は作成
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    # ファイル名は日付（例: 2026-02-12.log）
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(LOG_DIR, f"{today}.log")
    
    # 書き込み日時の取得
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # ログ行の作成
    log_line = f"[{now_str}] Rank: {rank_range} | CatID: {cat_id} | Saved: {count} items\n"
    
    # 追記モード("a")で保存
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
        jp_category_id = src.jp_category_id
WHEN NOT MATCHED THEN
    INSERT (asin, last_seen_at, jp_category_id, us_existence)
    VALUES (src.asin, SYSDATETIME(), src.jp_category_id, NULL);
"""

# step1_keepa_jp_to_asin.py 内の修正

def save_asins_to_db(cursor, asin_list, cat_id):
    if not asin_list:
        return 0
    
    count = 0
    for asin in asin_list:
        try:
            cursor.execute(SQL_UPSERT, [asin, cat_id])
            # ★ 1件ごとに確定させてロックを即座に解放する
            cursor.connection.commit() 
            count += 1
        except Exception as e:
            print(f"Error {asin}: {e}")
            cursor.connection.rollback()
    return count

def fetch_and_save_recursive(cat_id, min_rank, max_rank, cursor):
    """
    再帰的にKeepaからASINを取得し、その都度DBに保存する
    """
    print(f"Fetching Cat: {cat_id} | Rank(Avg180): {min_rank}-{max_rank}")
    
    selection = {
        "rootCategory": cat_id,
        "productType": 0,
        "avg180_SALES_gte": min_rank,
        "avg180_SALES_lte": max_rank,
        "avg90_NEW_gte": PRICE_MIN_JPY,
        "avg90_NEW_lte": PRICE_MAX_JPY,
        "packageLength_lte": MAX_EDGE_MM,
        "packageWeight_lte": MAX_WEIGHT_G,
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
        
        # --- ログ出力の実行 ---
        rank_range_str = f"{min_rank}-{max_rank}"
        write_execution_log(rank_range_str, cat_id, saved_count)
        # --------------------
        
        return saved_count

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    try:
        total_saved = fetch_and_save_recursive(TARGET_CATEGORY_ID, RANK_MIN, RANK_MAX, cursor)
        conn.commit()
        print(f"=== DB保存完了: 合計 {total_saved}件 ===")
        
    except Exception as e:
        print(f"Fatal Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()