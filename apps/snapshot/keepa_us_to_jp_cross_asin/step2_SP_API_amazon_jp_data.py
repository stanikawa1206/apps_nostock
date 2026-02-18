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

SQL_SELECT = """
    SELECT asin 
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
    target_asins = [row[0] for row in rows] # row.asin がエラーになる場合はインデックス指定
    print(f"更新対象: {len(target_asins)}件")

    if not target_asins:
        print("処理対象のASINがありませんでした。")
        conn.close()
        return

    # トークン取得と取得時刻の記録
    try:
        token = get_spapi_access_token("JP")
        token_start_time = time.time() # トークン取得時刻を記録
    except Exception as e:
        print(f"初期トークン取得失敗: {e}")
        conn.close()
        return

# 2. バッチ処理ループ
    total_processed = 0
    
    for i in range(0, len(target_asins), BATCH_SIZE):
        batch = target_asins[i : i + BATCH_SIZE]
        current_retry = 0
        max_retries = 2 # バッチ全体のリトライ回数
        
        while current_retry <= max_retries:
            try:
                print(f"Processing batch {i} - {i+len(batch)} (Retry: {current_retry})")
                
                # APIリクエストを実行
                items = get_spapi_items_batch(batch, "JP", token)
                
                # 成功した場合（データがある場合）
                if items:
                    process_items(cursor, items)
                    total_processed += len(batch)
                    break 
                else:
                    # アイテムが空（404等）の場合も、正常終了として次へ
                    print(f"  [Info] No items found for this batch.")
                    total_processed += len(batch)
                    break

            except Exception as e:
                # --- ここでエラーが出たら一律トークンを再取得 ---
                print(f"  [Error Occurred] {e}")
                print("  トークンを再取得してリトライします...")
                
                try:
                    # トークンを更新して再試行カウントを増やす
                    token = get_spapi_access_token("JP")
                    current_retry += 1
                    time.sleep(2) # 少し待機してからリトライ
                    
                    if current_retry > max_retries:
                        print(f"  [Skip] 最大リトライ回数を超えたため、このバッチを1件ずつ処理に回します。")
                        # 1件ずつのリトライロジックへ（既存のコードを流用）
                        for single_asin in batch:
                            try:
                                single_items = get_spapi_items_batch([single_asin], "JP", token)
                                if single_items:
                                    process_items(cursor, single_items)
                            except Exception:
                                pass
                        total_processed += len(batch)
                        break
                except Exception as token_e:
                    print(f"  [Critical] トークンの再取得自体に失敗しました: {token_e}")
                    time.sleep(10)
                    current_retry += 1
        
        time.sleep(BASE_WAIT_TIME)

    conn.close()
    print(f"=== 完了: 合計 {total_processed}件 処理しました ===")

if __name__ == "__main__":
    main()