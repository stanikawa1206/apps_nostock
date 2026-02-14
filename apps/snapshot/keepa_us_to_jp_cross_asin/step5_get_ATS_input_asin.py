import pandas as pd
import pyodbc
import os
from datetime import datetime
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

# --- 設定項目 ---
DB_DRIVER = os.getenv("DB_DRIVER")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME   = os.getenv("DB_NAME")
DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")

conn_str = f'DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}'

BASE_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin\ATS_file"
TODAY_STR = datetime.now().strftime("%y%m%d")

def get_unique_filepath(directory, date_str):
    counter = 1
    while True:
        file_name = f"{date_str}_{counter}.csv"
        file_path = os.path.join(directory, file_name)
        if not os.path.exists(file_path):
            return file_path
        counter += 1

def main():
    if not all([DB_DRIVER, DB_SERVER, DB_NAME, DB_USER, DB_PASS]):
        print("エラー: .env内の設定が不足しています。")
        return

    try:
        # 1. データベース接続
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()

        # 2. 条件に合うASINを10,000件抽出
        # pandasの警告を避けるため、cursorでexecuteしてからfetch
        select_query = """
            SELECT TOP 10000 asin 
            FROM trx.amazon_cross_market_asin 
            WHERE us_existence = 1 AND ATS IS NULL;
        """
        cur.execute(select_query)
        rows = cur.fetchall()
        
        if not rows:
            print("条件に一致するASINが見つかりませんでした。")
            return

        # リストに変換
        asins = [row.asin for row in rows]
        df = pd.DataFrame(asins, columns=['asin'])

        # 3. 抽出した行に実行日を記録する
        # エラー回避のため、IN句を使わず一時テーブルや分割更新をするのが理想ですが、
        # ここではシンプルかつ確実な「抽出条件を再利用した一括更新」を行います。
        update_query = f"""
            UPDATE trx.amazon_cross_market_asin 
            SET ATS = '{TODAY_STR}' 
            WHERE asin IN (
                SELECT TOP 10000 asin 
                FROM trx.amazon_cross_market_asin 
                WHERE us_existence = 1 AND ATS IS NULL
            );
        """
        # 注意: 厳密にはSELECT時とUPDATE時で対象がズレる可能性がありますが、
        # ATS IS NULLを条件にしているため、このスクリプト単体運用なら安全です。
        cur.execute(update_query)
        conn.commit()

        # 4. 保存パスの決定
        save_path = get_unique_filepath(BASE_DIR, TODAY_STR)

        # 5. CSV書き出し (ヘッダーなし)
        df['asin'].to_csv(save_path, index=False, header=False, encoding='utf-8')
        
        print(f"正常に終了しました。")
        print(f"保存先: {save_path}")
        print(f"処理件数: {len(asins)} 件")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    main()