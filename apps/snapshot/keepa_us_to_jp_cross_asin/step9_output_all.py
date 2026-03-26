import pandas as pd
import os
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
connection_url = (
    f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_SERVER}/{DB_NAME}?"
    f"driver={DB_DRIVER.replace(' ', '+')}"
)
engine = create_engine(connection_url)

BASE_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin\ATS_file"

def get_unique_filepath(directory, date_str):
    """ファイル名の連番を取得する"""
    counter = 1
    while True:
        file_name = f"{date_str}_{counter}.csv"
        file_path = os.path.join(directory, file_name)
        if not os.path.exists(file_path):
            return file_path
        counter += 1

def process_all_ats_nulls():
    """ATS IS NULLのレコードがなくなるまで1万件ずつ抽出・更新・書き出しを行う"""
    today_str = datetime.now().strftime("%y%m%d")
    log_file_path = os.path.join(BASE_DIR, "extraction_summary.log")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 処理開始: ATS IS NULLの全件抽出")
    
    total_extracted = 0

    while True:
        now_dt = datetime.now()
        now_time = now_dt.strftime("%H:%M:%S")

        try:
            # 1. 10,000件抽出 (us_existenceの条件を削除)
            select_query = """
                SELECT TOP 10000 asin, jp_category_id
                FROM trx.amazon_cross_market_asin WITH (NOLOCK)
                WHERE ATS IS NULL;
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(select_query), conn)
            
            # 対象がなくなったらループを抜ける
            if df.empty:
                print(f"[{now_time}] 対象データ(ATS IS NULL)がなくなりました。処理を完了します。")
                print(f"--- 最終抽出累計: {total_extracted} 件 ---")
                break

            # 2. フラグ更新 (チャンク分割処理)
            asin_list = df['asin'].tolist()
            
            with engine.begin() as conn:
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                chunk_size = 2000
                for i in range(0, len(asin_list), chunk_size):
                    chunk = asin_list[i:i + chunk_size]
                    placeholders = ','.join(['?' for _ in chunk])
                    
                    update_query = f"""
                        UPDATE trx.amazon_cross_market_asin
                        SET ATS = ? 
                        WHERE asin IN ({placeholders});
                    """
                    
                    params = [today_str] + chunk
                    cursor.execute(update_query, params)

            # 3. CSV保存 (10,000件を一括で保存)
            save_path = get_unique_filepath(BASE_DIR, today_str)
            df['asin'].to_csv(save_path, index=False, header=False, encoding='utf-8')
            
            # 4. カテゴリ別件数のログ記録
            category_counts = df['jp_category_id'].value_counts(dropna=False).to_dict()
            
            with open(log_file_path, "a", encoding="utf-8") as f:
                f.write(f"--- {now_dt.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
                f.write(f"Trigger : Continuous Batch\n")
                f.write(f"Output  : {os.path.basename(save_path)}\n")
                f.write(f"Total   : {len(df)} rows\n")
                f.write("Category Breakdown (jp_category_id):\n")
                for cat_id, count in category_counts.items():
                    cat_label = str(int(cat_id)) if pd.notnull(cat_id) else "Unknown/Null"
                    f.write(f"  - ID {cat_label}: {count} items\n")
                f.write("\n")

            total_extracted += len(df)
            print(f"[{now_time}] 正常終了: {len(df)} 件を {os.path.basename(save_path)} に出力しました。(累計: {total_extracted} 件)")

        except Exception as e:
            print(f"[{now_time}] エラー発生: {e}")
            break # エラーが発生した場合は無限ループを防ぐために終了

if __name__ == "__main__":
    process_all_ats_nulls()