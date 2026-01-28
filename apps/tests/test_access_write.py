import pyodbc

db_path = r"y:\ヤフオクDB.accdb"

conn = pyodbc.connect(
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    f"DBQ={db_path};"
)

cursor = conn.cursor()

sql = """
INSERT INTO [フリマ仕入d] ([注文ID])
VALUES (?)
"""

cursor.execute(sql, ("test",))
conn.commit()

conn.close()

print("insert done")
