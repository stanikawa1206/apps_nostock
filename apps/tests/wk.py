import pandas as pd
import urllib
from sqlalchemy import create_engine

# 1. 接続情報の設定（環境に合わせて書き換えてください）
server = '192.168.100.105,1433'      # 例: localhost や PC名\SQLEXPRESS
database = 'nostock'

# 1. ユーザー名とパスワードを指定
db_user = 'sa'     # SQL Serverで作ったユーザー名
db_password = 'tani6021' # そのパスワード

# 2. 接続文字列の作成
params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={db_user};'
    f'PWD={db_password};'
)

# 【ここが必要な engine です！】
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# 3. Excelデータの読み込み
df = pd.read_excel(r"Y:\Amazon輸出\brands.xlsx")

# 列名の整理（念のため）
df.columns = ['brand', 'rank', 'last_seen_at']

# 日付の変換（数値になってしまう場合）
df['last_seen_at'] = pd.to_datetime(df['last_seen_at'], errors='coerce')

# 4. 書き込み
try:
    # 念のため重複を削除
    df = df.drop_duplicates(subset=['brand'])
    
    df.to_sql('amazon_brand', schema='mst', con=engine, if_exists='append', index=False)
    print("書き込みが完了しました！")
except Exception as e:
    print(f"エラーが発生しました: {e}")