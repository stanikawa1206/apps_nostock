# step1_keepa_jp_to_asin.py
# -*- coding: utf-8 -*-
import sys
from my_utils import get_sql_server_connection, keepa_request

# ==========================================
# 設定: 検索条件
# ==========================================
TARGET_CATEGORY_ID = 14304371 # 例: 釣り具
QUERY_LIMIT = 10000           # Keepaの1回あたりの取得上限

# 1. ランキング範囲 (180日平均)
RANK_MIN = 1
RANK_MAX = 50000

# 2. 価格フィルタ (新品価格 90日平均)
#    10,000円 〜 300,000円
PRICE_MIN_JPY = 10000
PRICE_MAX_JPY = 300000

# 3. サイズ・重量フィルタ (Keepa指定用: mm単位, g単位)
#    160cm (1600mm) 以下、30kg (30000g) 以下
MAX_EDGE_MM = 1600
MAX_WEIGHT_G = 30000

# ==========================================
# SQL定義
# ==========================================
SQL_UPSERT = """
MERGE trx.amazon_cross_market_asin AS tgt
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

def save_asins_to_db(cursor, asin_list, cat_id):
    """リストを受け取ってDBに保存する"""
    if not asin_list:
        return 0
    
    count = 0
    for asin in asin_list:
        try:
            cursor.execute(SQL_UPSERT, [asin, cat_id])
            count += 1
        except Exception as e:
            print(f"Error {asin}: {e}")
    return count

def fetch_and_save_recursive(cat_id, min_rank, max_rank, cursor):
    """
    再帰的にKeepaからASINを取得し、その都度DBに保存する
    """
    print(f"Fetching Cat: {cat_id} | Rank(Avg180): {min_rank}-{max_rank}")
    
    selection = {
        "rootCategory": cat_id,
        
        # 4. 物理的な商品に限定 (0=Physical, 1=Digital)
        "productType": 0,
        
        # ランキング (180日平均)
        "avg180_SALES_gte": min_rank,
        "avg180_SALES_lte": max_rank,
        
        # 価格帯 (90日平均: 1万〜30万円)
        "avg90_NEW_gte": PRICE_MIN_JPY,
        "avg90_NEW_lte": PRICE_MAX_JPY,
        
        # サイズ・重量制限 (160cm, 30kg以下)
        "packageLength_lte": MAX_EDGE_MM,
        "packageWeight_lte": MAX_WEIGHT_G,
        
        # 新品在庫があるものに限定するなら以下も有効ですが、
        # "avg90_NEW" がある時点で価格履歴がある(=在庫があった)商品に絞られます。
        
        "perPage": QUERY_LIMIT
    }
    
    # APIリクエスト (my_utils内でトークン管理されます)
    res = keepa_request("query", params={"domain": 5}, data=selection)
    
    total = res.get("totalResults", 0)
    asin_list = res.get("asinList", [])
    
    # ヒット数が上限(10000)を超えており、かつ分割可能(幅が1より大きい)な場合
    if total > QUERY_LIMIT and (max_rank - min_rank) > 1:
        print(f"   [Split] Hit {total} > Limit. Splitting range...")
        
        mid = (min_rank + max_rank) // 2
        
        # 前半・後半に分けて再帰呼び出し
        count_1 = fetch_and_save_recursive(cat_id, min_rank, mid, cursor)
        count_2 = fetch_and_save_recursive(cat_id, mid + 1, max_rank, cursor)
        
        return count_1 + count_2
    else:
        # 上限以下なら保存実行
        print(f"   -> Got {len(asin_list)} items. Saving...")
        saved_count = save_asins_to_db(cursor, asin_list, cat_id)
        return saved_count

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    try:
        # 再帰処理の開始
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