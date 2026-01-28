import sys
import re
import inspect

# プロジェクトルートをパスに追加
sys.path.append(r"D:\apps_nostock")

from apps.common import utils
from apps.common.utils import translate_to_english

# ===== 設定 =====
vendor_item_id = "m62703355274"

# ===== utils確認 =====
print("UTILS FILE:", utils.__file__)
print(
    "HAS KALE PATCH:",
    "Kale ?90" in inspect.getsource(utils.translate_to_english)
)

# ===== DB から title_jp / description / preset 取得 =====
conn = utils.get_sql_server_connection()
cur = conn.cursor()

sql = """
SELECT
    title_jp,
    description,
    preset
FROM trx.vendor_item
WHERE vendor_item_id = ?
"""

row = cur.execute(sql, vendor_item_id).fetchone()
cur.close()
conn.close()

if not row:
    print(f"[ERROR] vendor_item_id not found: {vendor_item_id}")
    sys.exit(1)

title_jp, description, preset = row

print("\n=== SOURCE (JP) ===")
print("vendor_item_id:", vendor_item_id)
print("preset:", preset)
print("title_jp:")
print(title_jp)
print("\ndescription:")
print(description)

# ===== 翻訳実行（preset を渡す） =====
title_en = translate_to_english(title_jp, description, preset)

# ===== 結果表示 =====
print("\n=== RESULT (EN) ===")
print("title_en:")
print(title_en)