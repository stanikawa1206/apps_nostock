# step4_check_us.py (価格取得統合版)
# -*- coding: utf-8 -*-
import time
import requests
import sys

# 変更点1: get_spapi_prices_batch をインポートに追加
from my_utils import (
    get_sql_server_connection, 
    get_spapi_access_token, 
    get_spapi_items_batch,
    get_spapi_prices_batch 
)

# 設定
BATCH_SIZE = 10
BASE_WAIT_TIME = 2.0

# ステータス定義
ST_PENDING = 0   # 未処理
ST_EXIST = 1     # あり
ST_NOT_EXIST = 2 # なし

SQL_SELECT = """
SELECT asin FROM trx.amazon_cross_market_asin WITH (NOLOCK)
WHERE (wakarunda IN ('D', '-')) 
  AND us_existence = 0
"""

# 変更点2: USに存在する場合、us_priceも更新するようにSQLを変更（カラム名は環境に合わせてください）
SQL_UPDATE_EXIST = """
UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK) 
SET us_existence = 1, us_lowest_price_d = ?, last_seen_at = SYSDATETIME() 
WHERE asin = ?
"""

SQL_UPDATE_NOT_EXIST = """
UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK) 
SET us_existence = 2, last_seen_at = SYSDATETIME() 
WHERE asin = ?
"""

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(SQL_SELECT)
        rows = cursor.fetchall()
        target_asins = [row[0] for row in rows]
    except Exception as e:
        print(f"DB接続エラー: {e}")
        conn.close()
        sys.exit(1)

    print(f"US存在確認＆価格取得対象: {len(target_asins)}件")

    if not target_asins:
        print("処理対象のASINがありません。")
        conn.close()
        sys.exit(0)

    try:
        token = get_spapi_access_token("US")
    except Exception as e:
        print(f"初期トークン取得失敗: {e}")
        conn.close()
        sys.exit(1)

    total_processed = 0

    for i in range(0, len(target_asins), BATCH_SIZE):
        batch = target_asins[i : i + BATCH_SIZE]
        current_retry = 0
        max_retries = 1
        
        while current_retry <= max_retries:
            try:
                print(f"Checking batch {i} - {i+len(batch)} (Retry: {current_retry})")
                
                # ① まずカタログAPIで存在確認
                items = get_spapi_items_batch(batch, "US", token)
                found_asins = [item["asin"] for item in items if "asin" in item]
                
                # ② 存在するASINがある場合のみ、Pricing APIを呼び出す (無駄な通信を防止)
                price_map = {}
                if found_asins:
                    price_map = get_spapi_prices_batch(found_asins, "US", token)

                # ③ DBの更新処理
                for asin in batch:
                    if asin in found_asins:
                        # 価格マップから価格を取得（取得できなければNoneになるためNULLとして保存される）
                        price = price_map.get(asin)
                        cursor.execute(SQL_UPDATE_EXIST, [price, asin])
                    else:
                        cursor.execute(SQL_UPDATE_NOT_EXIST, [asin])

                conn.commit()
                total_processed += len(batch)
                break 

            except Exception as e:
                print(f"  [Error Occurred] {e}")
                if current_retry >= max_retries:
                    print("  [Critical] 最大リトライ回数を超えました。")
                    conn.close()
                    sys.exit(1)
                
                print("  トークンを再取得してリトライします...")
                try:
                    token = get_spapi_access_token("US")
                    current_retry += 1
                    time.sleep(5)
                except:
                    conn.close()
                    sys.exit(1)
        
        time.sleep(BASE_WAIT_TIME)

    conn.close()
    print(f"=== US存在確認＆価格取得完了: {total_processed}件 ===")

if __name__ == "__main__":
    main()