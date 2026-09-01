r"""
テスト：\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb の「日常」テーブルへ1レコードINSERT
"""

import requests
import pyodbc
from decimal import Decimal

# ============================================================
# 1. 為替レート取得（Yahoo Finance API）
# ============================================================
resp = requests.get(
    "https://query1.finance.yahoo.com/v8/finance/chart/USDJPY%3DX",
    params={"interval": "1d", "range": "1d"},
    headers={"User-Agent": "Mozilla/5.0"},
    timeout=10,
)
resp.raise_for_status()
rate = resp.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
print(f"USD/JPY レート : {rate}")

# ============================================================
# 2. 売価計算
# ============================================================
price = round(120 * rate)
print(f"売価           : {price} 円  (120 USD × {rate})")

# ============================================================
# 3. Access 接続
# ============================================================
conn = pyodbc.connect(
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb;"
)
print("Access 接続    : OK")

# ============================================================
# 4. INSERT
# ============================================================
cur = conn.cursor()
cur.execute("""
    INSERT INTO 日常 (品目text, 販売, 区分, 仕入元, 店舗, 売価)
    VALUES (?, ?, ?, ?, ?, ?)
""",
    "Louis Vuitton Monogram Coin Case",
    "eBay",
    "eMS",
    "電脳",
    "メルカリ",
    Decimal(str(price)),
)
conn.commit()
print("INSERT         : 完了")

cur.close()
conn.close()
