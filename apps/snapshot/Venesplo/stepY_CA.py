# -*- coding: utf-8 -*-
import datetime
import os
import csv
import logging
from my_utils import get_sql_server_connection

# ==========================================
# グローバル変数
# ==========================================
LOG_DIR = r"X:\apps\snapshot\keepa_ca_to_jp_cross_asin\logs"
CSV_OUTPUT_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo_CA\input"

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

    # 出力対象のASINを抽出 (CAカラム/CA_Venesploを参照)
    sql_select = """
        SELECT a.jp_brand, a.asin
        FROM trx.amazon_cross_market_asin a
        INNER JOIN mst.amazon_brand m ON a.jp_brand = m.brand
        WHERE m.CA = '〇'
          AND a.CA_Venesplo IS NULL
          AND a.wakarunda IN ('D', '-')
    """
    
    try:
        cursor.execute(sql_select)
        records = cursor.fetchall()
        
        if not records:
            logging.info("出力対象のデータ（CA判定済み・未書き出し）はありませんでした。")
            conn.close()
            return
            
        logging.info(f"出力対象のASIN(CA)を {len(records)} 件取得しました。")

        os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
        csv_filename = f"ca_listable_items_additional_{today_str_file}.csv"
        csv_file_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)
        
        with open(csv_file_path, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(["Brand", "ASIN", "CA_Venesplo"]) 
            for row in records:
                writer.writerow([row.jp_brand, row.asin, today_str])
                
        logging.info(f"CSV(CA)を出力しました: {csv_file_path}")

        # DBの更新（CA_Venesploに本日の日付を入れる）
        sql_update = """
            UPDATE a
            SET a.CA_Venesplo = ?
            FROM trx.amazon_cross_market_asin a
            INNER JOIN mst.amazon_brand m ON a.jp_brand = m.brand
            WHERE m.CA = '〇'
              AND a.CA_Venesplo IS NULL
              AND a.wakarunda IN ('D', '-')
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