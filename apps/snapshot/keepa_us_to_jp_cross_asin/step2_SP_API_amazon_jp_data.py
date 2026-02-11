import time
import requests
from my_utils import get_sql_server_connection, get_spapi_access_token, get_spapi_items_batch

# 設定
BATCH_SIZE = 10
BASE_WAIT_TIME = 2.0

SQL_SELECT = "SELECT asin FROM trx.amazon_cross_market_asin WHERE jp_brand IS NULL"
SQL_UPDATE = """
UPDATE trx.amazon_cross_market_asin 
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

        # 【修正】summariesが空の場合のクラッシュ防止
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
        
        try:
            cursor.execute(SQL_UPDATE, [title, price, brand, asin])
            count += 1
        except Exception as e:
            print(f"  DB Error {asin}: {e}")
    return count

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    # 1. 対象取得
    cursor.execute(SQL_SELECT)
    rows = cursor.fetchall()
    target_asins = [row.asin for row in rows]
    print(f"更新対象: {len(target_asins)}件")

    if not target_asins:
        conn.close()
        return

    try:
        token = get_spapi_access_token("JP")
    except Exception as e:
        print(f"初期トークン取得失敗: {e}")
        return

    # 2. バッチ処理
    total_processed = 0
    
    for i in range(0, len(target_asins), BATCH_SIZE):
        batch = target_asins[i : i + BATCH_SIZE]
        
        current_retry = 0
        max_retries = 3  # リトライ回数
        batch_success = False
        
        while current_retry <= max_retries:
            try:
                print(f"Processing batch {i} - {i+len(batch)} (Retry: {current_retry})")
                
                # APIリクエスト
                items = get_spapi_items_batch(batch, "JP", token)
                
                # アイテムが空（エラー等）でなければ処理
                if items:
                    c = process_items(cursor, items)
                    conn.commit()
                    total_processed += len(batch)
                    batch_success = True
                    break # 成功したらループを抜ける
                else:
                    # itemsが空(400エラー等でmy_utilsが空を返した場合)
                    # ここで例外を発生させて下のexceptブロックに飛ばす
                    raise requests.exceptions.RequestException("Batch failed or empty")

            except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
                # 400エラー(Bad Request)やその他の失敗時 -> 1件ずつ処理に切り替え
                print(f"  [Batch Error] バッチ処理失敗。1件ずつ再試行します... ({e})")
                
                single_success_count = 0
                for single_asin in batch:
                    try:
                        time.sleep(1.0) # 連打防止
                        single_items = get_spapi_items_batch([single_asin], "JP", token)
                        if single_items:
                            process_items(cursor, single_items)
                            single_success_count += 1
                    except Exception as single_e:
                        print(f"    [Single Error] {single_asin}: {single_e}")
                
                conn.commit()
                print(f"  -> 個別処理完了: {single_success_count}/{len(batch)}件 成功")
                batch_success = True # 個別処理で進んだことにする
                break

            except Exception as e:
                print(f"  [Unexpected Error] {e}")
                break
        
        # もしバッチも個別も失敗したら、ログを出して次へ
        if not batch_success:
            print(f"  [Skip] Batch {i} failed completely.")

        time.sleep(BASE_WAIT_TIME)

    conn.close()
    print(f"=== 完了: {total_processed}件 処理しました ===")

if __name__ == "__main__":
    main()