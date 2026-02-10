# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import sys
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection

# ============================================================
# 設定値
# ============================================================

# 【変更点1】 カテゴリマップではなく、対象ブランドのリストを定義
target_brands = [
    "シマノ(SHIMANO)",
    "ダイワ(DAIWA)",
    "new balance(ニューバランス)"
    # 必要なブランドをここに追加してください
]

PRICE90_NEW_JPY_MIN = 10_000
PRICE90_NEW_JPY_MAX = 300_000
SALES_RANK_MIN_START = 1
SALES_RANK_MAX_START = 1_000_000

KEEPA_QUERY_LIMIT = 10000 
DETAIL_BATCH_SIZE = 100    
LIMIT_TOKEN = 150

MAX_EDGE_CM = 160
MAX_WEIGHT_G = 30_000
MAX_VOLUME_CM3 = 180_000
MAX_EDGE_MM = MAX_EDGE_CM * 10

KEEPA_API_KEY = os.getenv("KEEPA_API_KEY") or os.getenv("KEEPA_KEY")
KEEPA_BASE = "https://api.keepa.com"
DOMAIN_JP = 5
DOMAIN_US = 1

# ============================================================
# トークン管理・APIリクエスト (変更なし)
# ============================================================

def get_token_status() -> dict:
    url = f"{KEEPA_BASE}/token"
    params = {"key": KEEPA_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return {
            "tokens_left": data.get("tokensLeft", 0),
            "refill_in_ms": data.get("refillIn", 0)
        }
    except Exception as e:
        print(f"   [Error] トークン確認に失敗: {e}")
        return {"tokens_left": 0, "refill_in_ms": 5000}

def ensure_tokens():
    while True:
        status = get_token_status()
        tokens = status["tokens_left"]
        
        if tokens >= LIMIT_TOKEN:
            break
            
        wait_sec = (status["refill_in_ms"] / 1000.0) + 2.0
        print(f"   [Token Wait] 残り{tokens}のため {wait_sec:.1f}秒 待機します...")
        time.sleep(wait_sec)
        print("   [Token Re-check] トークンを再確認します。")

def keepa_request(endpoint: str, method: str = "GET", params: dict = None, data: dict = None):
    ensure_tokens()
    time.sleep(0.5)

    url = f"{KEEPA_BASE}/{endpoint}"
    p = {"key": KEEPA_API_KEY}
    if params: p.update(params)
    
    for attempt in range(3):
        try:
            if method.upper() == "POST":
                r = requests.post(url, params=p, data=json.dumps(data), timeout=180)
            else:
                r = requests.get(url, params=p, timeout=180)

            if r.status_code == 429:
                wait_time = 30 * (attempt + 1)
                print(f"   [Alert] 429 Too Many Requests. {wait_time}秒待機してリトライします({attempt+1}/3)")
                time.sleep(wait_time)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            if attempt == 2: raise e
            print(f"   [Retry] リクエストエラー: {e}。5秒後に再試行します。")
            time.sleep(5)
    return {}

# ============================================================
# フィルタ・DB・ロジック
# ============================================================

def passes_size_weight_volume(prod: Dict[str, Any]) -> bool:
    h, l, w = prod.get("packageHeight", 0), prod.get("packageLength", 0), prod.get("packageWidth", 0)
    weight = prod.get("packageWeight", 0)
    if not (h and l and w and weight): return False
    h_cm, l_cm, w_cm = h/10, l/10, w/10
    if max(h_cm, l_cm, w_cm) > MAX_EDGE_CM: return False
    if (h_cm + l_cm + w_cm) > 200: return False
    if (h_cm * l_cm * w_cm) >= MAX_VOLUME_CM3: return False
    if weight > MAX_WEIGHT_G: return False
    return True

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

# 【変更点2】 引数から cat_id を削除し、内部で商品データから取得するように変更
def process_batch_details(asin_list: List[str], conn):
    print(f"      -> バッチ処理中: {len(asin_list)}件")
    
    # JP詳細取得
    jp_data = keepa_request("product", params={"domain": DOMAIN_JP, "asin": ",".join(asin_list), "stats": 90})
    products_jp = {p["asin"]: p for p in jp_data.get("products", []) if "asin" in p}
    
    # US詳細取得
    us_data = keepa_request("product", params={"domain": DOMAIN_US, "asin": ",".join(asin_list)})
    existing_us_asins = {p["asin"] for p in us_data.get("products", []) if p.get("title")}

    cursor = conn.cursor()
    success_count = 0
    for asin in asin_list:
        p_jp = products_jp.get(asin)
        if not p_jp or not passes_size_weight_volume(p_jp): continue
        if asin not in existing_us_asins: continue
        
        jp_title = p_jp.get("title")
        stats = p_jp.get("stats", {})
        jp_price = stats.get("avg90_NEW") or stats.get("avg90")
        if isinstance(jp_price, list): jp_price = None

        # 【追加】商品情報からカテゴリIDを取得（rootCategory または categories配列の先頭）
        # ブランド検索だとカテゴリが混在するため、固定値ではなく動的に取得する必要があります
        cat_id = p_jp.get("rootCategory")
        if not cat_id:
            cats = p_jp.get("categories")
            if cats and isinstance(cats, list) and len(cats) > 0:
                cat_id = cats[0]
        
        # もしカテゴリIDが取れない場合は 0 または NULL扱いの値をセット
        if not cat_id:
            cat_id = 0

        cursor.execute(SQL_MERGE, [asin, jp_title, jp_price, cat_id])
        success_count += 1
            
    conn.commit()
    cursor.close()
    print(f"         [DB Saved] {success_count}件")

# 【変更点3】引数を brand_name に変更し、検索条件を修正
def fetch_and_process_recursive(brand_name: str, min_rank: int, max_rank: int, conn):
    print(f"   [Finder Search] Brand: {brand_name} | Rank {min_rank} - {max_rank}")
    
    selection = {
        # categories_include を削除し、brand を追加
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

    print(f"total:{total}   ASIN:{asins[:10]}")

    # # ヒット数が上限(10000)を超えた場合、ランキング範囲を分割して再帰検索
    # # (大手ブランドの場合、範囲分割が必要になる可能性が高いです)
    # if total > KEEPA_QUERY_LIMIT and (max_rank - min_rank) > 1:
    #     print(f"   [Split] ヒット数 {total} が上限を超えたため、ランキング範囲を分割します。")
    #     mid = (min_rank + max_rank) // 2
    #     fetch_and_process_recursive(brand_name, min_rank, mid, conn)
    #     fetch_and_process_recursive(brand_name, mid + 1, max_rank, conn)
    # else:
    #     if asins:
    #         for i in range(0, len(asins), DETAIL_BATCH_SIZE):
    #             process_batch_details(asins[i : i + DETAIL_BATCH_SIZE], conn)

# ============================================================
# 実行
# ============================================================
def main():
    conn = get_sql_server_connection()
    try:
        # ブランドリストでループ
        for brand in target_brands:
            print(f"\n--- ブランド: {brand} 開始 ---")
            fetch_and_process_recursive(brand, SALES_RANK_MIN_START, SALES_RANK_MAX_START, conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()