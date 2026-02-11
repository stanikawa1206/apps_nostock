# step4_check_us.py
# -*- coding: utf-8 -*-
import time
import requests
from my_utils import get_sql_server_connection, get_spapi_access_token, get_spapi_items_batch, get_spapi_prices_batch

# 設定
BATCH_SIZE = 10
BASE_WAIT_TIME = 2.0

# 対象: ランクが D, -, E (仕入れ対象) で、US確認がまだ(NULL)のもの
SQL_SELECT = """
SELECT asin FROM trx.amazon_cross_market_asin 
WHERE (wakarunda IN ('D', '-', 'E')) 
  AND us_existence IS NULL
"""

# US情報更新
SQL_UPDATE_EXIST = """
UPDATE trx.amazon_cross_market_asin 
SET us_existence = 1, us_price = ?, last_seen_at = SYSDATETIME()
WHERE asin = ?
"""

SQL_UPDATE_NOT_EXIST = """
UPDATE trx.amazon_cross_market_asin 
SET us_existence = 0, us_price = NULL, last_seen_at = SYSDATETIME()
WHERE asin = ?
"""

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # 1. 対象取得
    cursor.execute(SQL_SELECT)
    rows = cursor.fetchall()
    target_asins = [row.asin for row in rows]
    print(f"US調査対象: {len(target_asins)}件")

    if not target_asins:
        conn.close()
        return

    # 初回トークン取得 (US)
    try:
        token = get_spapi_access_token("US")
    except Exception as e:
        print(f"初期トークン取得失敗: {e}")
        return

    total_processed = 0

    # 2. バッチ処理ループ
    for i in range(0, len(target_asins), BATCH_SIZE):
        batch = target_asins[i : i + BATCH_SIZE]
        
        current_retry = 0
        max_retries = 5
        
        # --- リトライループ (Step 2と同じ構造) ---
        while current_retry <= max_retries:
            try:
                print(f"Checking batch {i} - {i+len(batch)} (Retry: {current_retry})")
                
                # A. 存在確認 (Catalog API)
                # エラーが起きても my_utils 側で raise_for_status() されるので except に飛びます
                items = get_spapi_items_batch(batch, "US", token)
                
                # カタログにあったASINのリストを作成
                found_asins = [item["asin"] for item in items if "asin" in item]
                
                # B. 価格確認 (Pricing API)
                # 存在したASINについてのみ価格を聞く
                price_map = {}
                if found_asins:
                    # ここでもトークン切れの可能性があるため、エラー時は except に飛んでリトライさせます
                    price_map = get_spapi_prices_batch(found_asins, "US", token)

                # C. DB更新
                for asin in batch:
                    if asin in found_asins:
                        # 存在あり (us_existence = 1)
                        price = price_map.get(asin)
                        try:
                            cursor.execute(SQL_UPDATE_EXIST, [price, asin])
                        except Exception as e:
                            print(f"  DB Error {asin}: {e}")
                    else:
                        # 存在なし (us_existence = 0)
                        try:
                            cursor.execute(SQL_UPDATE_NOT_EXIST, [asin])
                        except Exception as e:
                            print(f"  DB Error {asin}: {e}")

                conn.commit()
                total_processed += len(batch)
                
                # 成功したらループを抜ける
                break 

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                
                # === エラーハンドリング (Step 2と同様) ===
                if status_code == 403:
                    print(f"  [403 Forbidden] トークン期限切れの可能性があります。10秒待機して再取得します...")
                    time.sleep(10)
                    try:
                        # USのトークンを再取得
                        token = get_spapi_access_token("US")
                        print("  -> トークン更新成功。リトライします。")
                    except Exception as token_err:
                        print(f"  -> トークン更新失敗: {token_err}")
                        break
                    current_retry += 1
                
                elif status_code == 429:
                    wait_sec = (2 ** current_retry) * 2
                    print(f"  [429 Rate Limit] {wait_sec}秒待機...")
                    time.sleep(wait_sec)
                    current_retry += 1
                    
                elif status_code >= 500:
                    print(f"  [Server Error {status_code}] Amazon側の不調。30秒待機...")
                    time.sleep(30)
                    current_retry += 1
                    
                else:
                    print(f"  [API Error {status_code}] スキップします: {e}")
                    break
                    
            except Exception as e:
                print(f"  [Unexpected Error] {e}")
                break
        
        # バッチ間の待機
        time.sleep(BASE_WAIT_TIME)

    conn.close()
    print(f"=== US確認完了: {total_processed}件 ===")

if __name__ == "__main__":
    main()