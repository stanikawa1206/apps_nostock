# -*- coding: utf-8 -*-
import datetime
import os
import csv
import logging

# my_utils.py からDB接続関数をインポート
from my_utils import get_sql_server_connection

# ==========================================
# グローバル変数
# ==========================================
LOG_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo\log_US"
CSV_OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo\input_US"

# ==========================================
# ロギング設定
# ==========================================
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"export_judged_asins_{datetime.datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ==========================================
# メイン処理
# ==========================================
def main():
    logging.info("判定済み・未書き出しASINの出力処理を開始します。")
    
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"DB接続エラー: {e}")
        return

    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    today_str_file = datetime.datetime.now().strftime('%Y%m%d')

    # 1. 出力対象のASINを抽出
    sql_select = """
        SELECT a.jp_brand, a.asin
        FROM trx.amazon_cross_market_asin a
        INNER JOIN mst.amazon_brand m ON a.jp_brand = m.brand
        WHERE m.US = '〇'
          AND a.US_Venesplo IS NULL
          AND a.us_existence = 1
          AND a.wakarunda IN ('D', '-')
    """
    
    try:
        cursor.execute(sql_select)
        records = cursor.fetchall()
        
        if not records:
            logging.info("出力対象のデータ（判定済み・未書き出し）はありませんでした。")
            conn.close()
            return
            
        logging.info(f"出力対象のASINを {len(records)} 件取得しました。")

        # 2. CSVへの書き出し処理
        os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
        csv_filename = f"us_listable_items_additional_{today_str_file}.csv"
        csv_file_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)
        
        with open(csv_file_path, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(["Brand", "ASIN", "US_Venesplo"])
            
            for row in records:
                writer.writerow([row.jp_brand, row.asin, today_str])
                
        logging.info(f"CSVを出力しました: {csv_file_path}")

        # 3. DBの更新
        sql_update = """
            UPDATE a
            SET a.US_Venesplo = ?
            FROM trx.amazon_cross_market_asin a
            INNER JOIN mst.amazon_brand m ON a.jp_brand = m.brand
            WHERE m.US = '〇'
              AND a.US_Venesplo IS NULL
              AND a.us_existence = 1
              AND a.wakarunda IN ('D', '-')
        """
        cursor.execute(sql_update, today_str)
        conn.commit()
        
        logging.info("データベースの出力フラグ(US_Venesplo)を更新しました。")

    except Exception as e:
        logging.error(f"処理中にエラーが発生しました: {e}")
        conn.rollback()
    finally:
        conn.close()

    logging.info("すべての処理が完了しました。")

if __name__ == "__main__":
    main()