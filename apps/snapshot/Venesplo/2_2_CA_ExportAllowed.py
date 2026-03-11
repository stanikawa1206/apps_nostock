# -*- coding: utf-8 -*-
import time
import datetime
import os
import csv
import logging
from my_utils import get_sql_server_connection, get_spapi_access_token, get_spapi_items_batch

# ==========================================
# グローバル変数
# ==========================================
LOG_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo\log_CA"
CSV_OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo\input_CA"

# ==========================================
# ロギング設定
# ==========================================
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"export_judged_asins_ca_{datetime.datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def main():
    logging.info("カナダ向け判定済み・未書き出しASINの出力処理を開始します。")
    
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"DB接続エラー: {e}")
        return

    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    today_str_file = datetime.datetime.now().strftime('%Y%m%d')

    # 出力対象のASINを抽出 (ca_existence が 2 以外)
    sql_select = """
        SELECT a.jp_brand, a.asin, ISNULL(a.ca_existence, 0)
        FROM trx.amazon_cross_market_asin a
        INNER JOIN mst.amazon_brand m ON a.jp_brand = m.brand
        WHERE m.CA = '〇'
          AND a.CA_Venesplo IS NULL
          AND a.wakarunda IN ('D', '-')
          AND ISNULL(a.ca_existence, 0) != 2
    """
    
    try:
        cursor.execute(sql_select)
        records = cursor.fetchall()
        
        if not records:
            logging.info("出力対象のデータはありませんでした。")
            conn.close()
            return
            
        logging.info(f"出力候補のASINを {len(records)} 件取得しました。カタログ存在確認を行います。")

        # カタログ未確認のものを抽出し、APIで一括確認
        pending_asins = [row[1] for row in records if row[2] == 0]
        if pending_asins:
            token = get_spapi_access_token(region="US")
            for i in range(0, len(pending_asins), 10):
                batch = pending_asins[i:i+10]
                try:
                    items = get_spapi_items_batch(batch, "CA", token)
                    found_asins = [item["asin"] for item in items if "asin" in item]
                except Exception as e:
                    logging.error(f"カタログ確認エラー: {e}")
                    continue

                for asin in batch:
                    if asin in found_asins:
                        cursor.execute("UPDATE trx.amazon_cross_market_asin SET ca_existence = 1 WHERE asin = ?", asin)
                    else:
                        cursor.execute("UPDATE trx.amazon_cross_market_asin SET ca_existence = 2 WHERE asin = ?", asin)
                conn.commit()
                time.sleep(2.0)

        # 改めて ca_existence = 1 のものだけを抽出して出力
        cursor.execute("""
            SELECT a.jp_brand, a.asin
            FROM trx.amazon_cross_market_asin a
            INNER JOIN mst.amazon_brand m ON a.jp_brand = m.brand
            WHERE m.CA = '〇'
              AND a.CA_Venesplo IS NULL
              AND a.wakarunda IN ('D', '-')
              AND a.ca_existence = 1
        """)
        valid_records = cursor.fetchall()

        if not valid_records:
            logging.info("出力対象の有効なカタログデータはありませんでした。")
            conn.close()
            return

        os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
        csv_filename = f"ca_listable_items_additional_{today_str_file}.csv"
        csv_file_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)
        
        with open(csv_file_path, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(["Brand", "ASIN", "CA_Venesplo"]) 
            for row in valid_records:
                writer.writerow([row[0], row[1], today_str])
                
        logging.info(f"CSV(CA)を出力しました: {csv_file_path} ({len(valid_records)}件)")

        # DBの更新（出力したASINのみ）
        sql_update = """
            UPDATE a
            SET a.CA_Venesplo = ?
            FROM trx.amazon_cross_market_asin a
            INNER JOIN mst.amazon_brand m ON a.jp_brand = m.brand
            WHERE m.CA = '〇'
              AND a.CA_Venesplo IS NULL
              AND a.wakarunda IN ('D', '-')
              AND a.ca_existence = 1
        """
        cursor.execute(sql_update, today_str)
        conn.commit()
        logging.info("データベースの出力フラグ(CA_Venesplo)を更新しました。")

    except Exception as e:
        logging.error(f"処理中にエラーが発生しました: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()