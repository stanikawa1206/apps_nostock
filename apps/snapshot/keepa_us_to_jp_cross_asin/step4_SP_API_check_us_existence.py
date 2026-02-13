# step4_check_us.py
# -*- coding: utf-8 -*-
import time
import requests
from my_utils import get_sql_server_connection, get_spapi_access_token, get_spapi_items_batch

# 設定
BATCH_SIZE = 10
BASE_WAIT_TIME = 2.0

# ステータス定義
ST_PENDING = 0   # 未処理
ST_EXIST = 1     # あり
ST_NOT_EXIST = 2 # なし

# 対象: ランクが D, - (仕入れ対象) で、US確認がまだ(0)のもの
SQL_SELECT = """
SELECT asin FROM trx.amazon_cross_market_asin 
WHERE (wakarunda IN ('D', '-')) 
  AND us_existence = 0
"""

# US情報更新 (存在あり: 1) ※価格は更新対象から除外
SQL_UPDATE_EXIST = """
UPDATE trx.amazon_cross_market_asin 
SET us_existence = 1, last_seen_at = SYSDATETIME()
WHERE asin = ?
"""

# US情報更新 (存在なし: 2)
SQL_UPDATE_NOT_EXIST = """
UPDATE trx.amazon_cross_market_asin 
SET us_existence = 2, last_seen_at = SYSDATETIME()
WHERE asin = ?
"""

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # 1. 対象取得
    cursor.execute(SQL_SELECT)
    rows = cursor.fetchall()
    target_asins = [row.asin for row in rows]
    print(f"US存在確認対象: {len(target_asins)}件")

    if not target_asins:
        print("処理対象のASINがありません。")
        conn.close()
        return

    # トークン取得 (US)
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
        
        while current_retry <= max_retries:
            try:
                print(f"Checking batch {i} - {i+len(batch)} (Retry: {current_retry})")
                
                # A. 存在確認 (Catalog API のみ実行)
                items = get_spapi_items_batch(batch, "US", token)
                
                # カタログにあったASINのリストを作成
                found_asins = [item["asin"] for item in items if "asin" in item]
                
                # B. DB更新
                for asin in batch:
                    if asin in found_asins:
                        # 存在あり
                        cursor.execute(SQL_UPDATE_EXIST, [asin])
                    else:
                        # 存在なし
                        cursor.execute(SQL_UPDATE_NOT_EXIST, [asin])

                conn.commit()
                total_processed += len(batch)
                break 

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code == 403:
                    print("  [403] トークン更新中...")
                    time.sleep(10)
                    try:
                        token = get_spapi_access_token("US")
                    except: break
                    current_retry += 1
                elif status_code == 429:
                    time.sleep((2 ** current_retry) * 2)
                    current_retry += 1
                elif status_code >= 500:
                    time.sleep(30)
                    current_retry += 1
                else:
                    print(f"  [API Error {status_code}] スキップ")
                    break
            except Exception as e:
                print(f"  [Unexpected Error] {e}")
                break
        
        time.sleep(BASE_WAIT_TIME)

    conn.close()
    print(f"=== US存在確認完了: {total_processed}件 ===")

if __name__ == "__main__":
    main()