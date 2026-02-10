# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import sys
import requests

# パス設定 (環境に合わせて調整してください)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection

# ============================================================
# 設定値
# ============================================================

target_brands = ["LEGO", "Nike", "Adidas"]

PRICE90_NEW_JPY_MIN = 10_000
PRICE90_NEW_JPY_MAX = 300_000
SALES_RANK_MIN_START = 1
SALES_RANK_MAX_START = 1_000_000

KEEPA_QUERY_LIMIT = 10000 

# ★変更: SP-APIのデータ欠損を防ぐため、1回のリクエストを10件に制限
DETAIL_BATCH_SIZE = 10    
LIMIT_TOKEN = 150

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

SPAPI_ENDPOINT_JP = "https://sellingpartnerapi-fe.amazon.com"
SPAPI_ENDPOINT_US = "https://sellingpartnerapi-na.amazon.com"
SPAPI_DELAY = 1.2  # リクエスト間隔を少し広めに確保

# ============================================================
# SP-API 認証
# ============================================================

def get_spapi_access_token(region: str = "JP") -> str:
    """region="JP" or "US" に応じてトークンを取得"""
    client_id = os.getenv("LWA_CLIENT_ID")
    client_secret = os.getenv("LWA_CLIENT_SECRET")
    
    if region == "US":
        refresh_token = os.getenv("REFRESH_TOKEN_US")
    else:
        refresh_token = os.getenv("REFRESH_TOKEN")

    if not (refresh_token and client_id and client_secret):
        target_var = "REFRESH_TOKEN_US" if region == "US" else "REFRESH_TOKEN"
        raise RuntimeError(f"SP-API認証情報不足: {target_var}")

    url = "https://api.amazon.com/auth/o2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    try:
        resp = requests.post(url, data=data, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()["access_token"]
    except Exception as e:
        print(f"❌ SP-API Token Error ({region}): {e}")
        raise

# ============================================================
# Keepa API 
# ============================================================

def get_keepa_token_status() -> dict:
    url = f"{KEEPA_BASE}/token"
    params = {"key": KEEPA_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return {"tokens_left": data.get("tokensLeft", 0), "refill_in_ms": data.get("refillIn", 0)}
    except:
        return {"tokens_left": 0, "refill_in_ms": 5000}

def ensure_keepa_tokens():
    while True:
        status = get_keepa_token_status()
        if status["tokens_left"] >= LIMIT_TOKEN: break
        time.sleep((status["refill_in_ms"] / 1000.0) + 2.0)

def keepa_request(endpoint: str, method: str = "GET", params: dict = None, data: dict = None):
    ensure_keepa_tokens()
    time.sleep(0.5)
    url = f"{KEEPA_BASE}/{endpoint}"
    p = {"key": KEEPA_API_KEY}
    if params: p.update(params)
    try:
        if method.upper() == "POST":
            r = requests.post(url, params=p, data=json.dumps(data), timeout=180)
        else:
            r = requests.get(url, params=p, timeout=180)
        r.raise_for_status()
        return r.json()
    except:
        return {}

# ============================================================
# SP-API (詳細取得・フィルタ)
# ============================================================

def get_spapi_items_batch(asin_list: List[str], marketplace_id: str, access_token: str) -> List[Dict[str, Any]]:
    """SP-API searchCatalogItems (最大10-20件程度推奨)"""
    if not asin_list: return []
    
    if marketplace_id == MARKETPLACE_ID_US:
        base_url = SPAPI_ENDPOINT_US
    else:
        base_url = SPAPI_ENDPOINT_JP

    url = f"{base_url}/catalog/2022-04-01/items"
    params = {
        "identifiers": ",".join(asin_list),
        "identifiersType": "ASIN",
        "marketplaceIds": marketplace_id,
        "includedData": "summaries,attributes"
    }
    headers = {"X-Amz-Access-Token": access_token, "Content-Type": "application/json"}

    for attempt in range(3):
        # APIコール前の待機（呼び出し元で制御するが、念のためここでも最低限待つ）
        time.sleep(SPAPI_DELAY)
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            
            if r.status_code == 403:
                print(f"   [SP-API 403] 権限エラー ({base_url})")
                return []
            if r.status_code == 429:
                print(f"   [SP-API 429] Limit Exceeded. Waiting...")
                time.sleep(5 * (attempt + 1))
                continue
            
            r.raise_for_status()
            return r.json().get("items", [])
        except Exception as e:
            print(f"   [SP-API Error] {e}")
            time.sleep(2)
    return []

def passes_spapi_size_filter(item: Dict[str, Any]) -> Tuple[bool, str]:
    """サイズフィルタ"""
    attrs = item.get("attributes", {})
    
    # 寸法取得 (package_dimensions -> item_package_dimensions -> item_dimensions)
    dims_list = attrs.get("package_dimensions") or attrs.get("item_package_dimensions") or attrs.get("item_dimensions")
    dim_type = "pkg" if attrs.get("package_dimensions") else ("item_pkg" if attrs.get("item_package_dimensions") else "item")

    if not dims_list:
        return False, "寸法なし"
    
    d = dims_list[0]
    try:
        h = d.get("height", {}).get("value", 0)
        l = d.get("length", {}).get("value", 0)
        w = d.get("width", {}).get("value", 0)
        weight = d.get("weight", {}).get("value", 0)
        
        if weight == 0:
            w_list = attrs.get("package_weight") or attrs.get("item_package_weight") or attrs.get("item_weight")
            if w_list: weight = w_list[0].get("value", 0)

        info_str = f"[{dim_type}] {h}x{l}x{w}|{weight}"

        if max(h, l, w) > MAX_EDGE_CM: return False, f"NG:辺 {info_str}"
        if (h + l + w) > 200: return False, f"NG:3辺 {info_str}"
        if (h * l * w) >= MAX_VOLUME_CM3: return False, f"NG:体積 {info_str}"
        if weight > MAX_WEIGHT_G: return False, f"NG:重量 {info_str}"
        
        return True, f"OK {info_str}"
    except Exception as e:
        return False, f"Err {e}"

# DB保存用SQL
SQL_MERGE = r"""
MERGE trx.amazon_cross_market_asin AS tgt
USING (SELECT ? AS asin, ? AS jp_title, ? AS jp_price, ? AS jp_category_id) AS src
ON tgt.asin = src.asin
WHEN MATCHED THEN
    UPDATE SET last_seen_at=SYSDATETIME(), jp_title=src.jp_title, jp_category_id=src.jp_category_id
WHEN NOT MATCHED THEN
    INSERT (asin, last_seen_at, jp_title, jp_price, jp_category_id)
    VALUES (src.asin, SYSDATETIME(), src.jp_title, src.jp_price, src.jp_category_id);
"""

# ============================================================
# バッチ処理 (リスト分割は呼び出し元で行う前提)
# ============================================================

def process_batch_details_spapi(asin_list: List[str], conn, token_jp: str, token_us: str):
    """
    1. SP-API(JP)詳細取得 -> 2. US存在確認 -> 3. DB保存
    ※ asin_list は呼び出し元で既に DETAIL_BATCH_SIZE (10件) 以下に分割されている前提
    """
    if not asin_list: return

    # --- 1. JP側詳細取得 ---
    items_jp = get_spapi_items_batch(asin_list, MARKETPLACE_ID_JP, token_jp)
    
    fetched_map = {item.get('asin'): item for item in items_jp}
    valid_jp_items = []
    
    print(f"      [Debug] JP取得: {len(asin_list)}件中 {len(items_jp)}件成功")
    
    for asin in asin_list:
        item = fetched_map.get(asin)
        if item:
            is_pass, debug_msg = passes_spapi_size_filter(item)
            
            summary = item.get("summaries", [{}])[0]
            title = summary.get("itemName", "No Title")
            short_title = title[:15] + "..."
            
            # ログ出力
            mark = "OK" if is_pass else "NG"
            print(f"      ASIN: {asin} | {mark:4} | {short_title} | {debug_msg}")
            
            if is_pass:
                valid_jp_items.append(item)
        else:
            print(f"      ASIN: {asin} | MISSING | データなし")

    if not valid_jp_items:
        print("      [Info] 有効データなしのためスキップ")
        # 次のバッチへの安全マージン
        time.sleep(SPAPI_DELAY)
        return
        
    valid_asins = [it["asin"] for it in valid_jp_items]

    # --- 2. US側存在確認 ---
    # JP取得から連続しないよう少し待つ
    time.sleep(0.5)
    
    items_us = get_spapi_items_batch(valid_asins, MARKETPLACE_ID_US, token_us)
    existing_us_asins = {it["asin"] for it in items_us}

    # --- 3. DB保存 ---
    # cursor = conn.cursor()  # ★本番用
    success_count = 0
    
    # print(f"      --- 保存候補 (Dry Run) ---")
    for item in valid_jp_items:
        asin = item["asin"]
        if asin not in existing_us_asins:
            # print(f"      [Skip] USなし: {asin}")
            continue
        
        summary = item.get("summaries", [{}])[0]
        title = summary.get("itemName", "")
        cat_id_str = summary.get("browseClassification", {}).get("nodeId")
        try:
            cat_id = int(cat_id_str) if cat_id_str and cat_id_str.isdigit() else 0
        except:
            cat_id = 0

        # DB保存 (本番時はコメント解除)
        """
        try:
            cursor.execute(SQL_MERGE, [asin, title, None, cat_id])
            success_count += 1
        except Exception as e:
            print(f"      [DB Error] {asin}: {e}")
        """
        print(f"      [Will Save] {asin} (Cat:{cat_id})")
        success_count += 1
            
    # conn.commit()  # ★本番用
    # cursor.close() # ★本番用

    if success_count > 0:
        print(f"         -> {success_count}件 書き込み対象")

    # バッチ処理完了ウェイト
    time.sleep(SPAPI_DELAY)

# ============================================================
# 再帰検索 (ここで分割して呼び出す)
# ============================================================

def fetch_and_process_recursive(brand_name: str, min_rank: int, max_rank: int, conn, token_jp: str, token_us: str):
    print(f"   [Finder] Brand: {brand_name} | Rank {min_rank} - {max_rank}")
    
    selection = {
        "brand": [brand_name], 
        "productType": 0,
        "avg90_NEW_gte": PRICE90_NEW_JPY_MIN, "avg90_NEW_lte": PRICE90_NEW_JPY_MAX,
        "current_SALES_gte": min_rank, "current_SALES_lte": max_rank,
        "packageLength_lte": MAX_EDGE_MM, "packageWeight_lte": MAX_WEIGHT_G,
        "perPage": KEEPA_QUERY_LIMIT
    }
    
    res = keepa_request("query", method="POST", params={"domain": DOMAIN_JP}, data=selection)
    total = res.get("totalResults", 0)
    asins = res.get("asinList", [])

    if total > KEEPA_QUERY_LIMIT and (max_rank - min_rank) > 1:
        print(f"   [Split] Hit {total} > Limit. Splitting...")
        mid = (min_rank + max_rank) // 2
        fetch_and_process_recursive(brand_name, min_rank, mid, conn, token_jp, token_us)
        fetch_and_process_recursive(brand_name, mid + 1, max_rank, conn, token_jp, token_us)
    else:
        if asins:
            print(f"      -> {len(asins)}件取得。10件ずつSP-API処理開始...")
            
            # ★ここで DETAIL_BATCH_SIZE (10) ずつに分割して関数を呼び出す
            for i in range(0, len(asins), DETAIL_BATCH_SIZE):
                batch_asins = asins[i : i + DETAIL_BATCH_SIZE]
                process_batch_details_spapi(batch_asins, conn, token_jp, token_us)

# ============================================================
# テスト実行 (ここで分割して呼び出す)
# ============================================================

def test_main():
    print("\n=== SP-API テスト (10件分割呼び出し) ===")

    # テストデータ (20件)
    test_asins = [
        'B0DSP1JVWV', 'B0DSP6L9FC', 'B0DSPSX621', 'B0DSP24SK9', 'B0DSP68TDW', 
        'B0DSPHSXHC', 'B0DSPJ216Q', 'B0DSNTFRNP', 'B0DSPGYSXG', 'B0DSPG6G4F',
        'B0CCN6CCFV', 'B0CCN628NZ', 'B0DF2MHQBZ', 'B0DF25CP4X', 'B0DF21LJG7', 
        'B0DF2C9YTX', 'B0F9Y6GJVZ', 'B0F9Y5B2NT', 'B0F9Y8TXNF', 'B07THGG264'
    ]
    print(f"1. 対象ASIN: {len(test_asins)}件")

    conn = get_sql_server_connection()
    try:
        token_jp = get_spapi_access_token("JP")
        token_us = get_spapi_access_token("US")
        print("2. トークン取得完了")

        print("3. バッチ処理開始...")
        
        # ★ここでも 10件ずつ (DETAIL_BATCH_SIZE) に分割して呼び出す
        for i in range(0, len(test_asins), DETAIL_BATCH_SIZE):
            batch_chunk = test_asins[i : i + DETAIL_BATCH_SIZE]
            print(f"\n>>> Batch {i // DETAIL_BATCH_SIZE + 1} (ASIN数: {len(batch_chunk)}) <<<")
            
            process_batch_details_spapi(batch_chunk, conn, token_jp, token_us)
            
        print("\n=== テスト完了 ===")

    except Exception as e:
        print(f"\n[Error] {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

# ============================================================
# 本番実行
# ============================================================
def main():
    conn = get_sql_server_connection()
    try:
        token_jp = get_spapi_access_token("JP")
        token_us = get_spapi_access_token("US")
        
        for brand in target_brands:
            print(f"\n--- Brand: {brand} Start ---")
            fetch_and_process_recursive(brand, SALES_RANK_MIN_START, SALES_RANK_MAX_START, conn, token_jp, token_us)
    finally:
        conn.close()

if __name__ == "__main__":
    test_main()
    # main()