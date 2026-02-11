import pandas as pd
import urllib
from sqlalchemy import create_engine, text  # create_engine を追加

# 1. 接続情報の設定
server = '192.168.100.105,1433'
database = 'nostock'
db_user = 'sa'
db_password = 'tani6021'

# 2. 接続文字列の作成
params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={db_user};'
    f'PWD={db_password};'
)

# engine オブジェクトを正しく作成
db_url = f"mssql+pyodbc:///?odbc_connect={params}"
engine_obj = create_engine(db_url)
# 3. Excelデータの読み込みと成形# 3. Excelデータの読み込みと成形
try:
    df = pd.read_excel(r"Y:\Amazon輸出\brands.xlsx")
    df.columns = ['brand', 'rank', 'last_seen_at']

    # --- クリーニング ---
    # 文字列化し、前後の空白を除去
    df['brand'] = df['brand'].astype(str).str.strip()
    
    # 【最重要】Excel内での重複を完全に排除
    # brand列が全く同じ値の行を1つに絞ります
    df = df.drop_duplicates(subset=['brand'], keep='first')

    # 空白行を除去
    df = df[df['brand'].notna() & (df['brand'] != '') & (df['brand'] != 'nan')]
    
    # 日付の修正
    df['last_seen_at'] = pd.to_datetime(df['last_seen_at'], unit='D', origin='1899-12-30', errors='coerce')

    # --- 4. UPSERTの実行 ---
    with engine_obj.connect() as conn:
        with conn.begin():
            # 一時テーブル作成（ここで確実に重複がないdfを渡す）
            df.to_sql('temp_brands', con=conn, if_exists='replace', index=False)

            # MERGE文（COLLATEを指定した状態で実行）
            upsert_query = text("""
                MERGE INTO mst.amazon_brand AS target
                USING (SELECT brand, [rank], last_seen_at FROM temp_brands) AS source
                ON (target.brand = source.brand COLLATE Japanese_CS_AS)
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.[rank] = source.[rank], 
                        target.last_seen_at = source.last_seen_at
                WHEN NOT MATCHED THEN
                    INSERT (brand, [rank], last_seen_at) 
                    VALUES (source.brand, source.[rank], source.last_seen_at);
            """)
            
            conn.execute(upsert_query)
            conn.execute(text("DROP TABLE temp_brands"))
            
    print("書き込み（更新・挿入）が完了しました！")

except Exception as e:
    print(f"エラーが発生しました: {e}")