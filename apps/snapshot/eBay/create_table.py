import pyodbc

def create_access_table():
    # ご指定のAccessデータベースへのパス
    db_path = r'Y:\ebay在庫管理.accdb'
    
    # Accessデータベースへの接続文字列
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        rf'DBQ={db_path};'
    )
    
    try:
        # DBに接続
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print(f"データベース {db_path} に接続しました。")

        # テーブルを作成するSQL文
        sql_create_table = """
        CREATE TABLE Payoneer_Data (
            [Currency] VARCHAR(50),
            [Payout method] VARCHAR(50),
            [Transaction date] DATETIME,
            [Credit amount] DOUBLE,
            [Debit amount] DOUBLE,
            [Running balance] DOUBLE,
            [Description] VARCHAR(255),
            [payアカウント] VARCHAR(50)
        )
        """
        
        # SQLを実行してテーブルを作成
        cursor.execute(sql_create_table)
        conn.commit()
        print("テーブル『Payoneer_Data』の作成が完了しました！")

    except pyodbc.Error as e:
        print(f"データベースエラーが発生しました: {e}")
        print("※既に同名のテーブルが存在している場合もエラーになります。")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        
    finally:
        # 接続を確実に閉じる
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# 実行
if __name__ == "__main__":
    create_access_table()