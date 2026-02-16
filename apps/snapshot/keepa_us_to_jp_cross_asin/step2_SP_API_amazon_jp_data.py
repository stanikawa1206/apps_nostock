# step2_SP_API_amazon_jp_data.py
# -*- coding: utf-8 -*-
import time
import requests
import sys
from my_utils import get_sql_server_connection, get_spapi_access_token, get_spapi_items_batch

# ==========================================
# 設定
# ==========================================
BATCH_SIZE = 10
BASE_WAIT_TIME = 2.0

# 読み取り専用(NOLOCK)で抽出。空文字も対象に含めることで確実に拾う
SQL_SELECT = """
    SELECT TOP 500 asin 
    FROM trx.amazon_cross_market_asin WITH (NOLOCK) 
    WHERE (jp_title IS NULL OR jp_title = '')
    ORDER BY last_seen_at DESC
"""

# 行ロック(ROWLOCK)を指定して更新
SQL_UPDATE = """
UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK)
SET jp_title = ?, jp_price = ?, jp_brand = ?, last_seen_at = SYSDATETIME()
WHERE asin = ?
"""

def extract_price(item):
    """SP-APIのitemデータから価格(数値)を抽出する"""
    try:
        offers = item.get("offers", [])
        if offers:
            price_info = offers[0].get("price", {})
            amount = price_info.get("amount")
            if amount:
                return float(amount)
    except Exception:
        pass
    return None

def process_items(cursor, items):
    """取得したアイテムリストをDBに保存する共通処理"""
    count = 0
    for item in items:
        asin = item.get("asin")
        if not asin: continue

        summaries = item.get("summaries", [])
        if not summaries:
            print(f"  [Skip] No summaries for {asin}")
            continue
            
        summary = summaries[0]
        attr = item.get("attributes", {})
        
        title = summary.get("itemName")
        brand = summary.get("brand") or attr.get("brand", [{}])[0].get("value")
        price = extract_price(item)
        
        if brand:
            brand = str(brand).strip()
        
        # --- 修正ポイント：1件ごとにリトライとコミットを行う ---
        # これにより Step 1 とのデッドロックを回避し、成功分を確実に保存する
        for retry in range(3):
            try:
                cursor.execute(SQL_UPDATE, [title, price, brand, asin])
                # 実行直後にこの行のロックを解放するためにコミット
                cursor.connection.commit() 
                count += 1
                break 
            except Exception as e:
                # タイムアウトやデッドロック時は少し待機してリトライ
                if "timeout" in str(e).lower() or "deadlock" in str(e).lower():
                    time.sleep(1)
                    continue
                print(f"  [DB Update Error] {asin}: {e}")
                cursor.connection.rollback()
                break
    return count

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    # 1. 対象取得
    print("更新対象を検索中...")
    cursor.execute(SQL_SELECT)
    rows = cursor.fetchall()
    target_asins = [row.asin for row in rows]
    print(f"更新対象: {len(target_asins)}件")

    if not target_asins:
        print("処理対象のASINがありませんでした。")
        conn.close()
        return

    try:
        token = get_spapi_access_token("JP")
    except Exception as e:
        print(f"初期トークン取得失敗: {e}")
        conn.close()
        return

    # 2. バッチ処理ループ
    total_processed = 0
    
    for i in range(0, len(target_asins), BATCH_SIZE):
        batch = target_asins[i : i + BATCH_SIZE]
        current_retry = 0
        max_retries = 3
        batch_success = False
        
        while current_retry <= max_retries:
            try:
                print(f"Processing batch {i} - {i+len(batch)} (Retry: {current_retry})")
                
                # APIリクエスト
                items = get_spapi_items_batch(batch, "JP", token)
                
                if items:
                    # process_items内部で1件ずつコミットされる
                    process_items(cursor, items)
                    total_processed += len(batch)
                    batch_success = True
                    break 
                else:
                    raise requests.exceptions.RequestException("Empty response")

            except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
                print(f"  [Batch Error] 1件ずつ再試行します... ({e})")
                for single_asin in batch:
                    try:
                        time.sleep(0.5)
                        single_items = get_spapi_items_batch([single_asin], "JP", token)
                        if single_items:
                            process_items(cursor, single_items)
                    except Exception as single_e:
                        print(f"    [Single Error] {single_asin}: {single_e}")
                
                total_processed += len(batch)
                batch_success = True 
                break

            except Exception as e:
                print(f"  [Unexpected Error] {e}")
                current_retry += 1
                time.sleep(5)
        
        time.sleep(BASE_WAIT_TIME)

    conn.close()
    print(f"=== 完了: 合計 {total_processed}件 処理しました ===")

if __name__ == "__main__":
    main()