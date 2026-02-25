# -*- coding: utf-8 -*-
import time
import datetime
import os
import csv
import logging

# my_utils.py からDB接続とトークン取得関数をインポート
from my_utils import get_spapi_access_token, get_sql_server_connection

# ==========================================
# グローバル変数（ファイルパス・設定の一元管理）
# ==========================================
LOG_DIR = r"X:\apps\snapshot\keepa_us_to_jp_cross_asin\logs"
CSV_OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo\input"

# ==========================================
# ロギング設定
# ==========================================
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"restriction_check_{datetime.datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ==========================================
# SP-API 制限チェック関数
# ==========================================
def check_us_restriction(asin, access_token, retry_count=3):
    import requests
    seller_id = "A3LDC1YQ3725LV"
    marketplace_id = "ATVPDKIKX0DER"
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
            time.sleep(1.0) # APIレートリミット対策
            resp = requests.get(url, headers=headers, params=params)
            
            if resp.status_code == 200:
                data = resp.json()
                restrictions = data.get("restrictions", [])
                if not restrictions:
                    return "OK"          # 出品可能
                return "RESTRICTED"      # 出品不可
            elif resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(3.0)
                continue
            else:
                return "ERROR"
        except Exception as e:
            time.sleep(3.0)
            continue
    return "ERROR"

# ==========================================
# メイン処理
# ==========================================
def main():
    logging.info("処理を開始します。")
    
    # 1. 初期トークン取得と取得時間の記録
    try:
        token = get_spapi_access_token(region="US")
        token_acquired_time = time.time() # 取得した現在時刻（秒）を記録
    except Exception as e:
        logging.error(f"初期トークンの取得に失敗しました: {e}")
        return

    # 2. DB接続
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"DB接続エラー: {e}")
        return

    # 3. 処理対象のブランドとASINを取得
    sql_select = """
        SELECT a.jp_brand, a.asin
        FROM trx.amazon_cross_market_asin a
        LEFT JOIN mst.amazon_brand m ON a.jp_brand = m.brand
        WHERE a.wakarunda IN ('D', '-')
          AND a.US_Venesplo IS NULL
          AND m.US IS NULL
    """
    try:
        cursor.execute(sql_select)
        rows = cursor.fetchall()
    except Exception as e:
        logging.error(f"DBデータ抽出エラー: {e}")
        conn.close()
        return

    brand_asin_map = {}
    for row in rows:
        brand = row.jp_brand
        asin = row.asin
        if brand not in brand_asin_map:
            brand_asin_map[brand] = []
        if len(brand_asin_map[brand]) < 3:
            brand_asin_map[brand].append(asin)

    logging.info(f"チェック対象ブランド数: {len(brand_asin_map)}")
    
    # バッチの基準日を固定
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    today_str_file = datetime.datetime.now().strftime('%Y%m%d')

    # ★追加：本日OKになったブランドを格納するリスト
    newly_listable_brands = []

    # 4. ブランドごとにAPIで判定ループ
    for brand, asins in brand_asin_map.items():
        logging.info(f"ブランド確認中: {brand} (対象ASIN: {asins})")
        
        brand_is_listable = False
        checked_results = {} 

        for asin in asins:
            # --- トークンの有効期限チェック (50分 = 3000秒) ---
            if time.time() - token_acquired_time >= 3000:
                logging.info("トークン取得から50分経過しました。SP-APIの再認証を行います...")
                try:
                    token = get_spapi_access_token(region="US")
                    token_acquired_time = time.time() # 取得時間をリセット
                    logging.info("再認証に成功しました。処理を継続します。")
                except Exception as e:
                    logging.error(f"トークンの再取得に失敗しました。処理を中断します: {e}")
                    conn.close()
                    return

            # API実行
            status = check_us_restriction(asin, token)
            
            if status == "OK":
                checked_results[asin] = '〇'
                brand_is_listable = True
                break # 1つでもOKが出たら、残りのASINはAPIを叩かずに抜ける
            elif status == "RESTRICTED":
                checked_results[asin] = '×'
            else:
                checked_results[asin] = None

        # 5. 結果に応じたデータベースの更新
        try:
            if brand_is_listable:
                cursor.execute("UPDATE mst.amazon_brand SET US = '〇' WHERE brand = ?", brand)
                for asin, res in checked_results.items():
                    if res == '〇':
                        # トランザクションは調査したASINのみ更新
                        cursor.execute("""
                            UPDATE trx.amazon_cross_market_asin 
                            SET US_restriction = '〇', US_Venesplo = ? 
                            WHERE asin = ?
                        """, today_str, asin)
                
                # CSV出力のために、本日OKになったブランド名をリストに追加
                newly_listable_brands.append(brand)
                
            else:
                cursor.execute("UPDATE mst.amazon_brand SET US = '×' WHERE brand = ?", brand)
                for asin, res in checked_results.items():
                    if res == '×':
                        # トランザクションは調査したASINのみ更新
                        cursor.execute("""
                            UPDATE trx.amazon_cross_market_asin 
                            SET US_restriction = '×' 
                            WHERE asin = ?
                        """, asin)

            conn.commit()
            
        except Exception as e:
            logging.error(f"DB更新エラー (Brand: {brand}): {e}")
            conn.rollback()

    # 6. CSVへの書き出し処理（日別ファイル作成）
    try:
        if newly_listable_brands:
            # 本日OKになったブランドリストを使って、該当ブランドの全ASINをDBから取得
            # IN句用のプレースホルダ（?, ?, ?...）を作成
            placeholders = ','.join(['?'] * len(newly_listable_brands))
            query = f"""
                SELECT jp_brand, asin 
                FROM trx.amazon_cross_market_asin 
                WHERE jp_brand IN ({placeholders})
            """
            cursor.execute(query, newly_listable_brands)
            today_db_records = cursor.fetchall()

            os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
            csv_filename = f"us_listable_items_{today_str_file}.csv"
            csv_file_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)
            
            with open(csv_file_path, mode='w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(["Brand", "ASIN", "US_Venesplo"]) # ヘッダー行
                
                for row in today_db_records:
                    # 調査していないASINにはUS_Venesplo列に値が入っていないため、ここで日付を強制セット
                    writer.writerow([row.jp_brand, row.asin, today_str])
                    
            logging.info(f"CSVを出力しました: {csv_file_path} (対象ブランド: {len(newly_listable_brands)}件, 出力ASIN: {len(today_db_records)}件)")
        else:
            logging.info("本日新規に出品可能と判定されたデータはありませんでした（CSV出力スキップ）")
            
    except Exception as e:
        logging.error(f"CSV出力エラー: {e}")

    conn.close()
    logging.info("すべての処理が完了しました。")

if __name__ == "__main__":
    main()