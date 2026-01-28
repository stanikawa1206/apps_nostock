#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_keepa_csv_dump.py
Keepa APIレスポンスの全CSVフィールドをダンプして、年齢制限情報を探す
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta

import keepa

sys.path.append(r"D:\apps_nostock\common")
from utils import get_sql_server_connection, fetch_keepa_product_snapshot

# ====== Keepa 設定 ======
DEFAULT_KEY = "13b6942juaqrentk46epa7jkgokpl3fuhd21vf36h70iefsln2cr9q73i9jh31ui"
API_KEY = os.getenv("KEEPA_API_KEY", DEFAULT_KEY)

DOMAIN = "JP"

# テスト対象ASIN（成人向け商品）
TEST_ASIN = "B00LE7TO0K"

# 出力ファイルパス
OUTPUT_FILE = r"D:\apps_nostock\keepa_csv_dump.txt"

def main():
    if not API_KEY:
        raise SystemExit("KEEPA_API_KEY を設定してください。")

    print("=== Keepa CSV フィールド全ダンプ ===")
    print(f"ASIN: {TEST_ASIN}")
    print(f"出力先: {OUTPUT_FILE}\n")
    
    api = keepa.Keepa(API_KEY, timeout=60)

    # 商品情報取得
    res = api.query(
        [TEST_ASIN],
        domain=DOMAIN,
        stats=90,
        history=True,
        rating=True,
        offers=20,
        only_live_offers=True,
        buybox=True,
        wait=True
    ) or []

    if not res:
        print("データ取得失敗")
        return

    product = res[0]
    
    # txtファイルに書き出し
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("Keepa CSV フィールド全ダンプ\n")
        f.write("=" * 80 + "\n")
        f.write(f"ASIN: {TEST_ASIN}\n")
        f.write(f"取得日時: {datetime.now()}\n\n")
        
        # CSVフィールドをダンプ
        if "csv" in product:
            csv_data = product["csv"]
            f.write(f"CSV フィールド数: {len(csv_data)}\n\n")
            
            for i, value in enumerate(csv_data):
                # 値が配列の場合は、最新値だけ表示
                if isinstance(value, list):
                    latest_val = value[-1] if value else None
                    f.write(f"[{i:3d}] {str(latest_val)[:200]}\n")
                else:
                    f.write(f"[{i:3d}] {str(value)[:200]}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("フルJSON出力（確認用）\n")
        f.write("=" * 80 + "\n")
        f.write(json.dumps(product, indent=2, default=str))
    
    print(f"✅ 出力完了: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()