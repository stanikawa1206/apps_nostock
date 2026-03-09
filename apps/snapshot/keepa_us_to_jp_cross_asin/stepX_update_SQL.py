import pandas as pd
import pyodbc
from datetime import datetime
import glob
import os

# my_utils.py からデータベース接続用の関数をインポート
from my_utils import get_sql_server_connection

def update_jp_listed_date_from_csv(csv_file_path):
    print(f"CSVファイルを読み込んでいます: {csv_file_path}")
    
    # 1. CSVファイルの読み込み
    try:
        # ネットワークドライブ上のファイルで文字コードエラーが出る場合は、
        # encoding='utf-8-sig' や 'cp932' などを追記してください
        df = pd.read_csv(csv_file_path)
    except Exception as e:
        print(f"CSVの読み込みに失敗しました: {e}")
        return

    # 2. 「出品」列が「出品」となっているASINを抽出
    if '出品' not in df.columns or '出品ASIN' not in df.columns:
        print("CSVに「出品」または「出品ASIN」列が見つかりません。")
        return
        
    target_asins = df[df['出品'] == '出品']['出品ASIN'].tolist()

    if not target_asins:
        print("更新対象のASINが見つかりませんでした。")
        return

    # 実行日（今日の日付）を取得 (YYYY-MM-DD形式)
    today_str = datetime.now().strftime('%Y-%m-%d')

    print(f"対象ASIN数: {len(target_asins)}件")
    print(f"設定する日付: {today_str}")

    # 3. データベースへの接続と更新処理
    conn = None
    try:
        # my_utils.pyの関数を呼び出して接続
        conn = get_sql_server_connection()
        cursor = conn.cursor()

        # 4. SQLクエリの定義
        update_sql = """
        UPDATE [nostock].[trx].[amazon_cross_market_asin]
        SET [JP_listed_date] = ?
        WHERE [asin] = ?
        """

        # executemany用のパラメータリストを作成
        params = [(today_str, asin) for asin in target_asins]

        # 5. SQLの実行 (一括処理)
        print("データベースを更新しています...")
        cursor.executemany(update_sql, params)

        # 6. コミットして変更を確定
        conn.commit()
        print("データベースの更新が正常に完了しました。")

    except Exception as e:
        # エラーが発生した場合はロールバックして変更を取り消す
        if conn:
            conn.rollback()
        print(f"データベースの更新中にエラーが発生しました: {e}")

    finally:
        # 7. 接続を閉じる
        if conn:
            cursor.close()
            conn.close()
            print("データベース接続を閉じました。")

if __name__ == "__main__":
    # 対象のフォルダパスを指定 (r をつけることで \ をそのまま認識させます)
    target_dir = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin"
    
    # 検索パターンを作成 (例: \\MOUSE\...\asin_new_all_*.csv)
    search_pattern = os.path.join(target_dir, "asin_new_all_*.csv")
    
    # パターンに一致するファイルをすべて探す
    target_files = glob.glob(search_pattern)
    
    if not target_files:
        print(f"対象のCSVファイルが見つかりませんでした。\n検索先: {search_pattern}")
    else:
        # 複数ファイルがある場合、更新日時 (getmtime) が一番新しいものを取得する
        latest_csv = max(target_files, key=os.path.getmtime)
        print(f"以下のファイルを処理対象として選択しました: {latest_csv}")
        
        # 抽出した最新のファイルを使って更新処理を実行
        update_jp_listed_date_from_csv(latest_csv)