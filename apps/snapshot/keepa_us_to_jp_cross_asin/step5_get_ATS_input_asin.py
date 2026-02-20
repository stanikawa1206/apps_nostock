import pandas as pd
import pyodbc
import os
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv

# --- 設定・環境読み込み ---
load_dotenv()

DB_DRIVER = os.getenv("DB_DRIVER")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME   = os.getenv("DB_NAME")
DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")

conn_str = f'DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}'
BASE_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin\ATS_file"

def get_unique_filepath(directory, date_str):
    counter = 1
    while True:
        file_name = f"{date_str}_{counter}.csv"
        file_path = os.path.join(directory, file_name)
        if not os.path.exists(file_path):
            return file_path
        counter += 1

def execute_extraction(trigger_reason):
    """メインの抽出・書き出しロジック"""
    today_str = datetime.now().strftime("%y%m%d")
    now_time = datetime.now().strftime("%H:%M:%S")
    print(f"[{now_time}] 実行開始 (理由: {trigger_reason})")

    try:
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()

        # 1. 10,000件抽出
        select_query = """
            SELECT TOP 10000 asin 
            FROM trx.amazon_cross_market_asin WITH (NOLOCK)
            WHERE us_existence = 1 AND ATS IS NULL;
        """
        cur.execute(select_query)
        rows = cur.fetchall()
        
        if not rows:
            print(f"[{now_time}] 対象のASINが0件のため、処理をスキップしました。")
            return

        asins = [row.asin for row in rows]
        df = pd.DataFrame(asins, columns=['asin'])

        # 2. フラグ更新
        update_query = f"""
            UPDATE trx.amazon_cross_market_asin WITH (ROWLOCK)
            SET ATS = '{today_str}' 
            WHERE asin IN (
                SELECT TOP 10000 asin 
                FROM trx.amazon_cross_market_asin WITH (NOLOCK)
                WHERE us_existence = 1 AND ATS IS NULL
            );
        """
        cur.execute(update_query)
        conn.commit()

        # 3. 保存
        save_path = get_unique_filepath(BASE_DIR, today_str)
        df['asin'].to_csv(save_path, index=False, header=False, encoding='utf-8')
        
        print(f"[{now_time}] 正常終了: {len(asins)} 件を {os.path.basename(save_path)} に出力しました。")

    except Exception as e:
        print(f"[{now_time}] エラー発生: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def check_count_and_run():
    """DBの件数を確認し、10,000件を超えていたら実行する"""
    try:
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()
        
        check_query = """
            SELECT COUNT(*) 
            FROM trx.amazon_cross_market_asin WITH (NOLOCK)
            WHERE us_existence = 1 AND ATS IS NULL;
        """
        cur.execute(check_query)
        count = cur.fetchone()[0]
        cur.close()
        conn.close()

        if count >= 10000:
            print(f"--- 件数検知: 現在 {count} 件の未処理データがあります ---")
            execute_extraction(f"件数超過({count}件)")
        else:
            # 5分ごとのチェック時にログがうるさければ、このprintは消してもOKです
            print(f"[{datetime.now().strftime('%H:%M:%S')}] チェック完了: {count} 件 (10,000件未満のため待機)")

    except Exception as e:
        print(f"件数チェック中にエラー: {e}")

# --- スケジュール登録 ---

# 1. 5分ごとに件数チェック
schedule.every(5).minutes.do(check_count_and_run)

# 2. 毎日 08:30 に強制実行
schedule.every().day.at("08:30").do(execute_extraction, trigger_reason="定刻実行(08:30)")

# --- メインループ ---
print("========================================")
print(" ATS ASIN 抽出スケジューラー 起動中 ")
print(" ・5分ごとに10,000件到達をチェック")
print(" ・毎日 08:30 に残りを一括書き出し")
print("========================================")

while True:
    schedule.run_pending()
    time.sleep(1)