import pandas as pd
import pyodbc
from datetime import datetime
import glob
import os

# my_utils.py からデータベース接続用の関数をインポート
from my_utils import get_sql_server_connection

def update_listed_date_from_report(file_path):
    # ファイル名のみを取得
    filename = os.path.basename(file_path)
    
    # ファイル名のプレフィックス（先頭の文字）で国（更新対象の列）を判定
    if filename.startswith("US_"):
        target_column = "US_listed_date"
    elif filename.startswith("CA_"):
        target_column = "CA_listed_date"
    else:
        print(f"スキップ: ファイル名に US_ または CA_ のプレフィックスがありません ({filename})")
        return

    print(f"[{target_column} の更新] ファイルを読み込んでいます: {file_path}")
    
    try:
        # テキストファイルはタブ区切り(TSV)なので、sep='\t' を指定して読み込む
        df = pd.read_csv(file_path, sep='\t', dtype=str) # ASINが数値扱いにならないよう文字列として読み込む
    except Exception as e:
        print(f"ファイルの読み込みに失敗しました: {e}")
        return

    # product-id 列の存在確認
    if 'product-id' not in df.columns:
        print("ファイルに 'product-id' 列が見つかりません。")
        return
        
    # product-id 列からASINのリストを作成（重複と空白を排除）
    target_asins = df['product-id'].dropna().unique().tolist()

    if not target_asins:
        print("更新対象のASINが見つかりませんでした。")
        return

    # 実行日を取得
    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"抽出したASIN数: {len(target_asins)}件")
    print(f"設定する日付: {today_str}")

    # データベースへの接続と更新処理
    conn = None
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()

        # 動的に更新先カラムを切り替えるSQLクエリ
        update_sql = f"""
        UPDATE [nostock].[trx].[amazon_cross_market_asin]
        SET [{target_column}] = ?
        WHERE [asin] = ?
        """

        # パラメータリストを作成
        params = [(today_str, asin) for asin in target_asins]

        # SQLの実行
        print("データベースを更新しています（DBに存在しないASINは自動スキップされます）...")
        cursor.executemany(update_sql, params)

        conn.commit()
        print(f"({filename}) のデータベース更新が正常に完了しました。")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"データベースの更新中にエラーが発生しました: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # 対象フォルダのパスを指定（ネットワークパス）
    target_dir = r"\\MOUSE\apps_nostock\apps\snapshot\Venesplo"
    
    # US_*.txt と CA_*.txt の両方を検索パターンとして定義
    search_patterns = [
        os.path.join(target_dir, "US_*.txt"),
        os.path.join(target_dir, "CA_*.txt")
    ]
    
    target_files = []
    for pattern in search_patterns:
        # パターンに一致するファイルをリストに追加
        target_files.extend(glob.glob(pattern))
        
    if not target_files:
        print(f"対象のレポートファイル (US_*.txt または CA_*.txt) が見つかりませんでした。\n検索先: {target_dir}")
    else:
        # 見つかったすべてのファイルに対して順番に処理を実行
        for file_path in target_files:
            update_listed_date_from_report(file_path)
            print("-" * 40) # ログの区切り線