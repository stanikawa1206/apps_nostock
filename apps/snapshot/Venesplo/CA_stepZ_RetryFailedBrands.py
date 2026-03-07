# -*- coding: utf-8 -*-
import time
import datetime
import os
import csv
import logging
from my_utils import get_spapi_access_token, get_sql_server_connection, get_spapi_items_batch

# ==========================================
# グローバル変数
# ==========================================
LOG_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo\log_CA"
CSV_OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo\input_CA"

# ==========================================
# ロギング設定
# ==========================================
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"retry_restriction_check_ca_{datetime.datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def check_ca_restriction(asin, access_token, retry_count=3):
    import requests
    seller_id = "A3LDC1YQ3725LV"
    marketplace_id = "A2EUQ1WTGCTBG2"
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    url = f"{endpoint}/listings/2021-08-01/restrictions"
    
    params = {
        "asin": asin,
        "sellerId": seller_id,
        "marketplaceIds": marketplace_id,
        "conditionType": "new_new"
    }
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    for attempt in range(retry_count):
        try:
            time.sleep(1.0)
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code == 200:
                data = resp.json()
                if not data.get("restrictions", []):
                    return "OK"
                return "RESTRICTED"
            elif resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(3.0)
                continue
            else:
                return "ERROR"
        except Exception:
            time.sleep(3.0)
            continue
    return "ERROR"

def main():
    logging.info("【カナダ再トライ処理】を開始します。")
    
    try:
        token = get_spapi_access_token(region="US")
        token_acquired_time = time.time()
    except Exception as e:
        logging.error(f"初期トークンの取得に失敗: {e}")
        return

    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"DB接続エラー: {e}")
        return

    # m.CA = '×' で、かつ制限チェック未了のASINを拾う
    sql_select = """
        SELECT a.jp_brand, a.asin, ISNULL(a.ca_existence, 0) as ca_existence
        FROM trx.amazon_cross_market_asin a
        LEFT JOIN mst.amazon_brand m ON a.jp_brand = m.brand
        WHERE a.wakarunda IN ('D', '-')
          AND a.CA_Venesplo IS NULL
          AND ISNULL(a.ca_existence, 0) != 2
          AND m.CA = '×'
          AND a.CA_restriction IS NULL
    """
    try:
        cursor.execute(sql_select)
        rows = cursor.fetchall()
    except Exception as e:
        logging.error(f"DB抽出エラー: {e}")
        conn.close()
        return

    brand_asin_map = {}
    for row in rows:
        brand = row.jp_brand
        asin = row.asin
        ca_ext = row.ca_existence
        if brand not in brand_asin_map:
            brand_asin_map[brand] = []
        brand_asin_map[brand].append((asin, ca_ext))

    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    today_str_file = datetime.datetime.now().strftime('%Y%m%d')
    newly_listable_brands = []

    for brand, asins_data in brand_asin_map.items():
        logging.info(f"ブランド再確認中(CA): {brand}")
        
        valid_asins = [a for a, ext in asins_data if ext == 1]
        pending_asins = [a for a, ext in asins_data if ext == 0]

        for i in range(0, len(pending_asins), 10):
            if len(valid_asins) >= 3:
                break
            
            batch = pending_asins[i:i+10]
            if time.time() - token_acquired_time >= 3000:
                token = get_spapi_access_token(region="US")
                token_acquired_time = time.time()

            try:
                items = get_spapi_items_batch(batch, "CA", token)
                found_asins = [item["asin"] for item in items if "asin" in item]
            except Exception as e:
                time.sleep(3.0)
                continue

            for asin in batch:
                if asin in found_asins:
                    cursor.execute("UPDATE trx.amazon_cross_market_asin SET ca_existence = 1 WHERE asin = ?", asin)
                    valid_asins.append(asin)
                else:
                    cursor.execute("UPDATE trx.amazon_cross_market_asin SET ca_existence = 2 WHERE asin = ?", asin)
            conn.commit()
            time.sleep(2.0)

        if not valid_asins:
            continue

        target_asins = valid_asins[:3]
        brand_is_listable = False
        checked_results = {} 

        for asin in target_asins:
            if time.time() - token_acquired_time >= 3000:
                token = get_spapi_access_token(region="US")
                token_acquired_time = time.time()

            status = check_ca_restriction(asin, token)
            if status == "OK":
                checked_results[asin] = '〇'
                brand_is_listable = True
                break 
            elif status == "RESTRICTED":
                checked_results[asin] = '×'
            else:
                checked_results[asin] = None

        try:
            if brand_is_listable:
                cursor.execute("UPDATE mst.amazon_brand SET CA = '〇' WHERE brand = ?", brand)
                for asin, res in checked_results.items():
                    if res == '〇':
                        cursor.execute("""
                            UPDATE trx.amazon_cross_market_asin 
                            SET CA_restriction = '〇', CA_Venesplo = ? 
                            WHERE asin = ?
                        """, today_str, asin)
                newly_listable_brands.append(brand)
                logging.info(f"ブランド【{brand}】が再判定で出品可能(〇)に昇格しました！")
            else:
                for asin, res in checked_results.items():
                    if res == '×':
                        cursor.execute("UPDATE trx.amazon_cross_market_asin SET CA_restriction = '×' WHERE asin = ?", asin)
            conn.commit()
        except Exception as e:
            conn.rollback()

    try:
        if newly_listable_brands:
            placeholders = ','.join(['?'] * len(newly_listable_brands))
            query = f"""
                SELECT jp_brand, asin 
                FROM trx.amazon_cross_market_asin 
                WHERE jp_brand IN ({placeholders})
                  AND ca_existence = 1
            """
            cursor.execute(query, newly_listable_brands)
            today_db_records = cursor.fetchall()

            os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
            csv_filename = f"ca_listable_items_retry_{today_str_file}.csv"
            csv_file_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)
            
            with open(csv_file_path, mode='w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(["Brand", "ASIN", "CA_Venesplo"])
                for row in today_db_records:
                    writer.writerow([row.jp_brand, row.asin, today_str])
    except Exception as e:
        logging.error(f"CSV出力エラー: {e}")

    conn.close()

if __name__ == "__main__":
    main()