# -*- coding: utf-8 -*-
import sys
import os
import json
from pathlib import Path

# ==========================================
# 1. パスの設定（commonフォルダを読み込めるようにする）
# ==========================================
# このファイル自身の場所 (...\apps\snapshot\base)
current_dir = Path(__file__).resolve().parent
# common フォルダの場所 (...\apps\common)
common_dir = current_dir.parents[1] / "common"

# sys.path に common フォルダを追加
if str(common_dir) not in sys.path:
    sys.path.insert(0, str(common_dir))

# common フォルダにパスが通ったので、spapi.py をインポートできる
from spapi import SpapiSession


# ==========================================
# 2. アメリカAmazonからデータを取得する関数
# ==========================================
def get_us_amazon_item(asin: str):
    """
    アメリカAmazonから指定したASINの商品生データを取得する
    """
    # アメリカAmazon（NAリージョン）の設定
    US_ENDPOINT = "https://sellingpartnerapi-na.amazon.com"
    US_MARKETPLACE_ID = "ATVPDKIKX0DER"
    US_REGION = "us-east-1"  

    # セッションの初期化（spapi.py が .env を自動で読んでくれます）
    session = SpapiSession(
        endpoint=US_ENDPOINT,
        marketplace_id=US_MARKETPLACE_ID,
        aws_region=US_REGION
    )

    # Catalog Items API (v2022-04-01) のパス
    path = f"/catalog/2022-04-01/items/{asin}"

    # 取得したいデータを指定（属性、画像、概要）
    params = {
        "marketplaceIds": US_MARKETPLACE_ID,
        "includedData": "attributes,images,summaries"
    }

    print(f"ASIN: {asin} のデータを取得中 (北米リージョン)...")

    # リクエストの実行
    response = session.get(path, params=params)

    # 結果の処理
    if response.status_code == 200:
        print("✅ 取得成功！\n")
        data = response.json()
        # 生のJSONデータを分かりやすくインデントして表示
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return data
    else:
        print(f"❌ 取得失敗: HTTP {response.status_code}")
        print(response.text)
        return None


# ==========================================
# 3. 実行部分
# ==========================================
if __name__ == "__main__":
    # テスト用のASIN（例として適当なものを入れています。お好きなASINに変更してください）
    target_asin = "B000052Y74" 
    
    raw_data = get_us_amazon_item(target_asin)