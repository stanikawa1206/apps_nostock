# step2_SP_API_amazon_jp_data.py
# -*- coding: utf-8 -*-
import time
import requests
import sys
import traceback
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
            continue
            
        summary = summaries[0]
        attr = item.get("attributes", {})
        
        title = summary.get("itemName")
        brand = summary.get("brand") or attr.get("brand", [{}])[0].get("value")
        price = extract_price(item)
        
        if brand:
            brand = str(brand).strip()
        
        for retry in range(3):
            try:
                cursor.execute(SQL_UPDATE, [title, price, brand, asin])
                cursor.connection.commit() 
                count += 1
                break 
            except Exception as e:
                if "timeout" in str(e).lower() or "deadlock" in str(e).lower():
                    time.sleep(1)
                    continue
                print(f"  [DB Update Error] {asin}: {e}")
                cursor.connection.rollback()
                break
    return count

def main():
    conn = None
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"DB接続エラー: {e}")
        sys.exit(1)

    print("更新対象を検索中...")
    try:
        cursor.execute(SQL_SELECT)
        rows = cursor.fetchall()
        target_asins = [row[0] for row in rows]
    except Exception as e:
        print(f"SQL実行エラー: {e}")
        conn.close()
        sys.exit(1)

    print(f"更新対象: {len(target_asins)}件")

    if not target_asins:
        print("処理対象のASINがありませんでした。")
        conn.close()
        return

    # 初回のトークン取得
    try:
        token = get_spapi_access_token("JP")
    except Exception as e:
        print(f"初期トークン取得失敗: {e}")
        conn.close()
        sys.exit(1)

    total_processed = 0
    
    for i in range(0, len(target_asins), BATCH_SIZE):
        batch = target_asins[i : i + BATCH_SIZE]
        
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing batch {i} - {i+len(batch)}")
            
            # 実行
            items = get_spapi_items_batch(batch, "JP", token)
            
            # もし my_utils 内で print されるだけで例外が投げられない場合への対策
            # 標準出力を監視するのは難しいため、戻り値が None かつエラーが疑われる場合はここで判定
            if items is None:
                # ここに到達＝関数内でエラーが発生して return None された可能性が高い
                # 安全のため、1度リトライせず終了してバッチファイルに任せる
                print("!!! APIから有効なレスポンスがありませんでした。トークン切れの可能性があるため終了します !!!")
                conn.close()
                sys.exit(1)

            if items:
                processed_count = process_items(cursor, items)
                total_processed += processed_count
            
        except Exception as e:
            error_msg = str(e)
            print(f"  [Error Occurred] {error_msg}")
            
            # トークン切れキーワードが含まれていたら即終了
            if "Unauthorized" in error_msg or "expired" in error_msg:
                print("!!! トークン期限切れ検知。強制終了して再起動を待機します !!!")
                if conn: conn.close()
                sys.exit(1)
            
            # その他の不明なエラーも、ループが止まらないリスクを避けるため終了させる
            print("!!! 予期せぬエラーのためシステムを再起動します !!!")
            if conn: conn.close()
            sys.exit(1)
        
        time.sleep(BASE_WAIT_TIME)

    conn.close()
    print(f"=== 正常終了: 合計 {total_processed}件 処理しました ===")

if __name__ == "__main__":
    from datetime import datetime
    main()