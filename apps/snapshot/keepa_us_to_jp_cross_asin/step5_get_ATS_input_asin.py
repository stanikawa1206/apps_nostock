import pandas as pd
import pyodbc
import os
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# --- 設定・環境読み込み ---
load_dotenv()

DB_DRIVER = os.getenv("DB_DRIVER")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME   = os.getenv("DB_NAME")
DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")

# SQLAlchemy用の接続文字列 (URLエンコードが必要な場合があるため、直接構築)
# 形式: mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+...
connection_url = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_SERVER}/{DB_NAME}?"
    f"driver={DB_DRIVER.replace(' ', '+')}"
)
engine = create_engine(connection_url)

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
    """メインの抽出・書き出し・カテゴリ別ログ記録ロジック"""
    today_str = datetime.now().strftime("%y%m%d")
    now_dt = datetime.now()
    now_time = now_dt.strftime("%H:%M:%S")
    log_file_path = os.path.join(BASE_DIR, "extraction_summary.log")
    
    print(f"[{now_time}] 実行開始 (理由: {trigger_reason})")

    try:
        # 1. 10,000件抽出 (SQLAlchemy engineを使用)
        select_query = """
            SELECT TOP 10000 asin, jp_category_id
            FROM trx.amazon_cross_market_asin WITH (NOLOCK)
            WHERE us_existence = 1 AND ATS IS NULL;
        """
        # SQLAlchemy経由で読み込むことでWarningを回避
        with engine.connect() as conn:
            df = pd.read_sql(text(select_query), conn)
        
        if df.empty:
            print(f"[{now_time}] 対象のASINが0件のため、処理をスキップしました。")
            return

        # 2. フラグ更新 (チャンク分割処理)
        asin_list = df['asin'].tolist()
        
        # SQLAlchemyのトランザクション内で生コネクションを利用
        with engine.begin() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()
            
            # SQL Serverのパラメータ上限(2100)を回避するため、2000件ずつ分割して処理
            chunk_size = 2000
            for i in range(0, len(asin_list), chunk_size):
                chunk = asin_list[i:i + chunk_size]
                placeholders = ','.join(['?' for _ in chunk])
                
                # ATS (today_str) 用の '?' を先頭に1つ用意し、IN句用の '?' を続ける
                update_query = f"""
                    UPDATE trx.amazon_cross_market_asin
                    SET ATS = ? 
                    WHERE asin IN ({placeholders});
                """
                
                # パラメータのリストを作成 (先頭が today_str、以降が ASINのリスト)
                params = [today_str] + chunk
                
                cursor.execute(update_query, params)

        # 3. CSV保存 (10,000件を一括で保存)
        save_path = get_unique_filepath(BASE_DIR, today_str)
        df['asin'].to_csv(save_path, index=False, header=False, encoding='utf-8')
        
        # 4. カテゴリ別件数のログ記録
        category_counts = df['jp_category_id'].value_counts(dropna=False).to_dict()
        
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(f"--- {now_dt.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
            f.write(f"Trigger : {trigger_reason}\n")
            f.write(f"Output  : {os.path.basename(save_path)}\n")
            f.write(f"Total   : {len(df)} rows\n")
            f.write("Category Breakdown (jp_category_id):\n")
            for cat_id, count in category_counts.items():
                cat_label = str(int(cat_id)) if pd.notnull(cat_id) else "Unknown/Null"
                f.write(f"  - ID {cat_label}: {count} items\n")
            f.write("\n")

        print(f"[{now_time}] 正常終了: {len(df)} 件を {os.path.basename(save_path)} に出力しました。")

    except Exception as e:
        print(f"[{now_time}] エラー発生: {e}")
    # engineは閉じずに保持してOK

def check_count_and_run():
    """DBの件数を確認"""
    try:
        check_query = """
            SELECT COUNT(*) 
            FROM trx.amazon_cross_market_asin WITH (NOLOCK)
            WHERE us_existence = 1 AND ATS IS NULL;
        """
        with engine.connect() as conn:
            result = conn.execute(text(check_query))
            count = result.scalar()

        if count >= 10000:
            print(f"--- 件数検知: 現在 {count} 件 ---")
            execute_extraction(f"件数超過({count}件)")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] チェック: {count} 件")

    except Exception as e:
        print(f"件数チェック中にエラー: {e}")

schedule.every(5).minutes.do(check_count_and_run)
schedule.every().day.at("08:30").do(execute_extraction, trigger_reason="定刻実行(08:30)")

print(" ATS ASIN 抽出スケジューラー 起動中 (SQLAlchemy対応済) ")
while True:
    schedule.run_pending()
    time.sleep(1)