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
category_map = { 14304371: "スポーツ＆アウトドア" }

# category_map = {
#     4788676051: "Alexaスキル",
#     4976279051: "Amazonデバイス・アクセサリ",
#     7471077051: "Audible オーディオブック",
#     2016929051: "DIY・工具・ガーデン",
#     561958: "DVD",
#     2250738051: "Kindleストア",
#     637392: "PCソフト",
#     2351649051: "Prime Video",
#     2381130051: "アプリ＆ゲーム",
#     13299531: "おもちゃ",
#     637394: "ゲーム",
#     14304371: "スポーツ＆アウトドア",
#     2128134051: "デジタルミュージック",
#     160384011: "ドラッグストア",
#     2127209051: "パソコン・周辺機器",
#     52374051: "ビューティー",
#     2320455051: "ファイナンス",
#     2229202051: "ファッション",
#     2127212051: "ペット用品",
#     344845011: "ベビー＆マタニティ",
#     3828871: "ホーム＆キッチン",
#     2277721051: "ホビー",
#     561956: "ミュージック",
#     3210981: "家電＆カメラ",
#     2123629051: "楽器・音響機器",
#     3445393051: "産業・研究開発用品",
#     2017304051: "車＆バイク",
#     57239051: "食品・飲料・お酒",
#     2277724051: "大型家電",
#     86731051: "文房具・オフィス用品",
#     465392: "本",
#     52033011: "洋書"
# }

PRICE90_NEW_JPY_MIN = 10_000
PRICE90_NEW_JPY_MAX = 300_000
SALES_RANK_MIN_START = 1
SALES_RANK_MAX_START = 1_000_000

KEEPA_QUERY_LIMIT = 10000 
DETAIL_BATCH_SIZE = 100    
LIMIT_TOKEN = 150  # トークンに余裕を持って待機を開始する

MAX_EDGE_CM = 160
MAX_WEIGHT_G = 30_000
MAX_VOLUME_CM3 = 180_000
MAX_EDGE_MM = MAX_EDGE_CM * 10

KEEPA_API_KEY = os.getenv("KEEPA_API_KEY") or os.getenv("KEEPA_KEY")
KEEPA_BASE = "https://api.keepa.com"
DOMAIN_JP = 5
DOMAIN_US = 1

# ============================================================
# トークン管理・APIリクエスト
# ============================================================

def get_token_status() -> dict:
    """現在のトークン状況を取得する"""
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
    """リクエスト前にトークンを確認し、不足していれば再確認を繰り返して待機する"""
    while True:
        status = get_token_status()
        tokens = status["tokens_left"]
        
        if tokens >= LIMIT_TOKEN:
            # トークンが十分あればループを抜けてリクエストへ
            break
            
        wait_sec = (status["refill_in_ms"] / 1000.0) + 2.0
        print(f"   [Token Wait] 残り{tokens}のため {wait_sec:.1f}秒 待機します...")
        time.sleep(wait_sec)
        print("   [Token Re-check] トークンを再確認します。")

def keepa_request(endpoint: str, method: str = "GET", params: dict = None, data: dict = None):
    """
    Keepa APIへの共通リクエスト関数。
    トークン事前チェック、429回避の待機、リトライ機能を備える。
    """
    # 1. APIを叩く前に必ずトークンを確保
    ensure_tokens()

    # 2. 429(Too Many Requests)回避のため、リクエスト前に最低限のインターバルを置く
    time.sleep(0.5)

    url = f"{KEEPA_BASE}/{endpoint}"
    p = {"key": KEEPA_API_KEY}
    if params: p.update(params)
    
    for attempt in range(3): # 最大3回リトライ
        try:
            if method.upper() == "POST":
                r = requests.post(url, params=p, data=json.dumps(data), timeout=180)
            else:
                r = requests.get(url, params=p, timeout=180)

            # 429エラー(短時間のアクセス過多)が発生した場合
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

def process_batch_details(asin_list: List[str], cat_id: int, conn):
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

        cursor.execute(SQL_MERGE, [asin, jp_title, jp_price, cat_id])
        success_count += 1
            
    conn.commit()
    cursor.close()
    print(f"         [DB Saved] {success_count}件")

def fetch_and_process_recursive(cat_id, min_rank, max_rank, conn):
    print(f"   [Finder Search] Rank {min_rank} - {max_rank}")
    
    selection = {
        "categories_include": [cat_id], "productType": 0,
        "avg90_NEW_gte": PRICE90_NEW_JPY_MIN, "avg90_NEW_lte": PRICE90_NEW_JPY_MAX,
        "current_SALES_gte": min_rank, "current_SALES_lte": max_rank,
        "packageLength_lte": MAX_EDGE_MM, "packageWeight_lte": MAX_WEIGHT_G,
        "perPage": KEEPA_QUERY_LIMIT
    }
    
    res = keepa_request("query", method="POST", params={"domain": DOMAIN_JP}, data=selection)
    total = res.get("totalResults", 0)
    asins = res.get("asinList", [])

    if total > KEEPA_QUERY_LIMIT and (max_rank - min_rank) > 1:
        print(f"   [Split] ヒット数 {total} が上限を超えたため、ランキング範囲を分割します。")
        mid = (min_rank + max_rank) // 2
        fetch_and_process_recursive(cat_id, min_rank, mid, conn)
        fetch_and_process_recursive(cat_id, mid + 1, max_rank, conn)
    else:
        if asins:
            for i in range(0, len(asins), DETAIL_BATCH_SIZE):
                process_batch_details(asins[i : i + DETAIL_BATCH_SIZE], cat_id, conn)

# ============================================================
# 実行
# ============================================================
def main():
    conn = get_sql_server_connection()
    try:
        for cat_id, cat_name in category_map.items():
            print(f"\n--- カテゴリ: {cat_name} ({cat_id}) 開始 ---")
            fetch_and_process_recursive(cat_id, SALES_RANK_MIN_START, SALES_RANK_MAX_START, conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()