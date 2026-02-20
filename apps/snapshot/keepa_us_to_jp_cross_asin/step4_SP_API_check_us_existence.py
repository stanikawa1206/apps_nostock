# step4_check_us.py
# -*- coding: utf-8 -*-
import time
import requests
import sys
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
SELECT asin FROM trx.amazon_cross_market_asin WITH (NOLOCK)
WHERE (wakarunda IN ('D', '-')) 
  AND us_existence = 0
"""

SQL_UPDATE_EXIST = """
UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK) SET us_existence = 1, last_seen_at = SYSDATETIME() WHERE asin = ?
"""

SQL_UPDATE_NOT_EXIST = """
UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK) SET us_existence = 2, last_seen_at = SYSDATETIME() WHERE asin = ?
"""

def main():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # 1. 対象取得
    try:
        cursor.execute(SQL_SELECT)
        rows = cursor.fetchall()
        # row.asin がエラーになる環境があるため、インデックス指定を推奨
        target_asins = [row[0] for row in rows]
    except Exception as e:
        print(f"DB接続エラー: {e}")
        conn.close()
        sys.exit(1) # 異常終了

    print(f"US存在確認対象: {len(target_asins)}件")

    if not target_asins:
        print("処理対象のASINがありません。")
        conn.close()
        sys.exit(0) # 正常終了（これによりパイプラインが次へ進める）

    # トークン取得 (US)
    try:
        token = get_spapi_access_token("US")
    except Exception as e:
        print(f"初期トークン取得失敗: {e}")
        conn.close()
        sys.exit(1) # 異常終了

    total_processed = 0

    # 2. バッチ処理ループ
    for i in range(0, len(target_asins), BATCH_SIZE):
        batch = target_asins[i : i + BATCH_SIZE]
        current_retry = 0
        max_retries = 1 # トークン切れ等に対する再試行は1回
        
        while current_retry <= max_retries:
            try:
                print(f"Checking batch {i} - {i+len(batch)} (Retry: {current_retry})")
                items = get_spapi_items_batch(batch, "US", token)
                found_asins = [item["asin"] for item in items if "asin" in item]
                
                for asin in batch:
                    if asin in found_asins:
                        cursor.execute(SQL_UPDATE_EXIST, [asin])
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
                    sys.exit(1) # 異常終了して親に再起動させる
                
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
    print(f"=== US存在確認完了: {total_processed}件 ===")

if __name__ == "__main__":
    main()