# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple, Set
from pathlib import Path
import sys
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection
# SP-APIの認証情報を取得するための既存ユーティリティを想定
# もし別名であれば適宜書き換えてください
# from apps.common.spapi_utils import get_spapi_access_token 

# ============================================================
# 設定値
# ============================================================
category_map = { 14304371: "スポーツ＆アウトドア" }

PRICE90_NEW_JPY_MIN = 10_000
PRICE90_NEW_JPY_MAX = 300_000
SALES_RANK_MIN_START = 1
# SALES_RANK_MAX_START = 1_000_000
SALES_RANK_MAX_START = 1_000

KEEPA_QUERY_LIMIT = 10000 
# SP-APIの一括照会上限に合わせて20に変更（効率最大化）
DETAIL_BATCH_SIZE = 20    
LIMIT_TOKEN = 200  

MAX_EDGE_CM = 160
MAX_WEIGHT_G = 30_000
MAX_VOLUME_CM3 = 180_000
MAX_EDGE_MM = MAX_EDGE_CM * 10

KEEPA_API_KEY = os.getenv("KEEPA_API_KEY") or os.getenv("KEEPA_KEY")
KEEPA_BASE = "https://api.keepa.com"
DOMAIN_JP = 5

# SP-API設定
MARKETPLACE_ID_JP = "A1VC38T7YXB528"
MARKETPLACE_ID_US = "ATVPDKIKX0DER"
SPAPI_BASE_URL = "https://sellingpartnerapi-fe.amazon.com" # 極東(JP)
SPAPI_DELAY = 0.55 # 秒間2リクエスト制限に対する安全マージン

# ============================================================
# トークン管理・APIリクエスト (Keepa)
# ============================================================

def get_token_status() -> dict:
    url = f"{KEEPA_BASE}/token"
    params = {"key": KEEPA_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return {"tokens_left": data.get("tokensLeft", 0), "refill_in_ms": data.get("refillIn", 0)}
    except Exception as e:
        print(f"   [Keepa Error] トークン確認失敗: {e}")
        return {"tokens_left": 0, "refill_in_ms": 5000}

def ensure_keepa_tokens():
    while True:
        status = get_token_status()
        if status["tokens_left"] >= LIMIT_TOKEN: break
        wait_sec = (status["refill_in_ms"] / 1000.0) + 2.0
        print(f"   [Keepa Token Wait] 残り{status['tokens_left']}。 {wait_sec:.1f}秒待機...")
        time.sleep(wait_sec)

def keepa_request(endpoint: str, method: str = "GET", params: dict = None, data: dict = None):
    ensure_keepa_tokens()
    time.sleep(0.5)
    url = f"{KEEPA_BASE}/{endpoint}"
    p = {"key": KEEPA_API_KEY}
    if params: p.update(params)
    for attempt in range(3):
        try:
            r = requests.post(url, params=p, data=json.dumps(data), timeout=180) if method.upper() == "POST" else requests.get(url, params=p, timeout=180)
            if r.status_code == 429:
                time.sleep(30 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2: raise e
            time.sleep(5)
    return {}

# ============================================================
# SP-API リクエスト処理
# ============================================================

def get_spapi_items_batch(asin_list: List[str], marketplace_id: str, access_token: str) -> List[Dict[str, Any]]:
    """SP-API searchCatalogItemsを使用して最大20件の商品詳細を一括取得"""
    if not asin_list: return []
    
    url = f"{SPAPI_BASE_URL}/catalog/2022-04-01/items"
    params = {
        "identifiers": ",".join(asin_list),
        "identifiersType": "ASIN",
        "marketplaceIds": marketplace_id,
        "includedData": "summaries,attributes"
    }
    headers = {"X-Amz-Access-Token": access_token, "Content-Type": "application/json"}

    for attempt in range(3):
        time.sleep(SPAPI_DELAY) # レート制限遵守
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 429:
                print(f"   [SP-API 429] 制限超過。{10*(attempt+1)}秒待機...")
                time.sleep(10 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json().get("items", [])
        except Exception as e:
            print(f"   [SP-API Error] {e}")
            time.sleep(2)
    return []

# ============================================================
# フィルタ・ロジック
# ============================================================

def passes_spapi_size_filter(item: Dict[str, Any]) -> bool:
    """SP-APIのattributesから寸法を確認"""
    attrs = item.get("attributes", {})
    # 一般的にpackage_dimensionsに格納されている
    dims_list = attrs.get("package_dimensions")
    if not dims_list: return False
    
    d = dims_list[0]
    try:
        # 単位を確認しつつ数値取得 (mm/gに変換)
        h = d.get("height", {}).get("value", 0)
        l = d.get("length", {}).get("value", 0)
        w = d.get("width", {}).get("value", 0)
        weight = d.get("weight", {}).get("value", 0)
        
        # 単位がインチ/ポンド等の場合は変換が必要だが、JPは通常センチ/グラム
        # ここでは簡易的にセンチメートル前提で判定（必要に応じ単位変換ロジック追加）
        h_cm, l_cm, w_cm = h, l, w
        if max(h_cm, l_cm, w_cm) > MAX_EDGE_CM: return False
        if (h_cm + l_cm + w_cm) > 200: return False
        if (h_cm * l_cm * w_cm) >= MAX_VOLUME_CM3: return False
        if weight > MAX_WEIGHT_G: return False
        return True
    except:
        return False

SQL_MERGE = r"""
MERGE trx.amazon_cross_market_asin AS tgt
USING (SELECT ? AS asin, ? AS jp_title, ? AS jp_price, ? AS jp_category_id) AS src
ON tgt.asin = src.asin
WHEN MATCHED THEN
    UPDATE SET last_seen_at=SYSDATETIME(), jp_title=src.jp_title, jp_price=src.jp_price, jp_category_id=src.jp_category_id
WHEN NOT MATCHED THEN
    INSERT (asin, last_seen_at, jp_title, jp_price, jp_category_id)
    VALUES (src.asin, SYSDATETIME(), src.jp_title, src.jp_price, src.jp_category_id);
"""

def process_batch_details(asin_list: List[str], cat_id: int, conn, spapi_token: str):
    """20件単位でJP詳細(SP-API)取得 -> US存在確認(SP-API) -> DB保存"""
    
    # 1. JP側詳細取得
    items_jp = get_spapi_items_batch(asin_list, MARKETPLACE_ID_JP, spapi_token)
    if not items_jp: return

    # JPでフィルタを通過したASINのみ抽出
    valid_jp_items = []
    for item in items_jp:
        if passes_spapi_size_filter(item):
            valid_jp_items.append(item)
    
    if not valid_jp_items: return
    valid_asins = [it["asin"] for it in valid_jp_items]

    # 2. US側存在確認 (JPフィルタ通過分のみ効率的に確認)
    items_us = get_spapi_items_batch(valid_asins, MARKETPLACE_ID_US, spapi_token)
    existing_us_asins = {it["asin"] for it in items_us}

    # 3. DB保存
    cursor = conn.cursor()
    success_count = 0
    for item in valid_jp_items:
        asin = item["asin"]
        if asin not in existing_us_asins: continue
        
        summary = item.get("summaries", [{}])[0]
        title = summary.get("itemName") # SP-APIではitemName
        
        # ※価格情報はFinder(Keepa)の結果を保持するか、別途Pricing APIが必要
        # ここではFinderの条件を満たしているため、タイトル保存を優先
        try:
            cursor.execute(SQL_MERGE, [asin, title, None, cat_id])
            success_count += 1
        except Exception as e:
            print(f"      [DB Error] {asin}: {e}")
            
    conn.commit()
    cursor.close()
    if success_count > 0:
        print(f"         [DB Saved] {success_count}件 (JPフィルタ通過: {len(valid_jp_items)}件)")

def fetch_and_process_recursive(cat_id, min_rank, max_rank, conn, spapi_token: str):
    print(f"   [Finder Search] Rank {min_rank} - {max_rank}")
    
    selection = {
        "categories_include": [cat_id], "productType": 0,
        "avg90_NEW_gte": PRICE90_NEW_JPY_MIN, "avg90_NEW_lte": PRICE90_NEW_JPY_MAX,
        "current_SALES_gte": min_rank, "current_SALES_lte": max_rank,
        "packageLength_lte": MAX_EDGE_MM, "packageWeight_lte": MAX_WEIGHT_G,
        "perPage": KEEPA_QUERY_LIMIT
    }
    
    res = keepa_request("query", method="POST", params={"domain": DOMAIN_JP}, data=selection)
    asins = res.get("asinList", [])
    total = res.get("totalResults", 0)

    if total > KEEPA_QUERY_LIMIT and (max_rank - min_rank) > 1:
        mid = (min_rank + max_rank) // 2
        fetch_and_process_recursive(cat_id, min_rank, mid, conn, spapi_token)
        fetch_and_process_recursive(cat_id, mid + 1, max_rank, conn, spapi_token)
    else:
        if asins:
            # SP-APIの一括取得上限(20件)ずつ処理
            for i in range(0, len(asins), DETAIL_BATCH_SIZE):
                process_batch_details(asins[i : i + DETAIL_BATCH_SIZE], cat_id, conn, spapi_token)

# ============================================================
# 実行
# ============================================================
def main():
    conn = get_sql_server_connection()
    # SP-APIのアクセストークンを取得する関数を呼び出し
    # spapi_token = get_spapi_access_token() 
    spapi_token = "YOUR_ACCESS_TOKEN" # テスト用。実際には取得関数を呼ぶ
    
    try:
        for cat_id, cat_name in category_map.items():
            print(f"\n--- カテゴリ: {cat_name} ({cat_id}) 開始 ---")
            fetch_and_process_recursive(cat_id, SALES_RANK_MIN_START, SALES_RANK_MAX_START, conn, spapi_token)
    finally:
        conn.close()

if __name__ == "__main__":
    main()