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
    
    # ファイル名のプレフィックスで国を判定
    if filename.startswith("US_"):
        target_column = "US_listed_date"
    elif filename.startswith("CA_"):
        target_column = "CA_listed_date"
    else:
        print(f"スキップ: プレフィックスがありません ({filename})")
        return

    print(f"[{target_column} の更新] ファイル読み込み中: {file_path}")
    
    try:
        # TSVとして読み込み
        df = pd.read_csv(file_path, sep='\t', dtype=str)
    except Exception as e:
        print(f"読み込み失敗: {e}")
        return

    # 必要列の存在確認 (product-id と quantity)
    if 'product-id' not in df.columns or 'quantity' not in df.columns:
        print("必要な列 (product-id または quantity) が見つかりません。")
        return
        
    # --- 追加されたロジック: quantityが1以上のASINのみを抽出 ---
    # 数値に変換し、1以上の行だけを残す
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    filtered_df = df[df['quantity'] >= 1]
    
    # product-id 列からASINのリストを作成
    target_asins = filtered_df['product-id'].dropna().unique().tolist()
    # -------------------------------------------------------

    if not target_asins:
        print("更新対象（在庫あり）のASINが見つかりませんでした。")
        return

    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"在庫ありASIN数: {len(target_asins)}件 / 設定日付: {today_str}")

    conn = None
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()

        update_sql = f"""
        UPDATE [nostock].[trx].[amazon_cross_market_asin]
        SET [{target_column}] = ?
        WHERE [asin] = ?
        """

        params = [(today_str, asin) for asin in target_asins]

        print("DB更新中...")
        cursor.executemany(update_sql, params)
        conn.commit()
        print(f"({filename}) 正常に完了しました。")

    except Exception as e:
        if conn: conn.rollback()
        print(f"DBエラー: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    target_dir = r"X:\apps\snapshot\Venesplo\reportfile"
    search_patterns = [
        os.path.join(target_dir, "US_*.txt"),
        os.path.join(target_dir, "CA_*.txt")
    ]
    
    target_files = []
    for pattern in search_patterns:
        target_files.extend(glob.glob(pattern))
        
    if not target_files:
        print(f"対象ファイルが見つかりません: {target_dir}")
    else:
        for file_path in target_files:
            update_listed_date_from_report(file_path)
            print("-" * 40)