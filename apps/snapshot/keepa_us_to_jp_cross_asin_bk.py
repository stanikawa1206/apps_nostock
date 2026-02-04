# -*- coding: utf-8 -*-
from __future__ import annotations

"""
apps/seed/keepa_jp_seed_asin.py

第一段階（JPのみ）:
- Keepa JP(Product Finder)で候補ASINを抽出
- Keepa /product(JP)で寸法/重量/タイトル/価格(取れれば)を取得
- Pythonで最終条件（最大長辺/三辺合計/体積/重量）を満たすASINだけをDBにUPSERT
- 複数のカテゴリIDに対して処理を実行

保存先:
- trx.amazon_cross_market_asin
"""

import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # D:/apps_nostock
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import requests

# .env 読み込み
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from apps.common.utils import get_sql_server_connection

# ============================================================
# 実行条件（固定）
# ============================================================
LIMIT_ASINS_PER_CATEGORY: int = 60  # 1カテゴリあたりの上限（テスト用）

category_map = {
    14304371: "スポーツ＆アウトドア",
}

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

# Finder ページング
MAX_PAGES: int = 50
PER_PAGE: int = 50

# API呼び出し間隔
SLEEP_SEC: float = 0.2

# ============================================================
# Keepa API 設定
# ============================================================
KEEPA_API_KEY = os.getenv("KEEPA_API_KEY") or os.getenv("KEEPA_KEY")
if not KEEPA_API_KEY:
    raise RuntimeError("KEEPA_API_KEY (or KEEPA_KEY) が未設定です。.env を確認してください。")

KEEPA_BASE = "https://api.keepa.com"
DOMAIN_JP = 5  # Amazon.co.jp
DOMAIN_US = 1  # Amazon.com
LIMIT_TOKEN = 300

# ============================================================
# フェーズ1（JP抽出条件）
# ============================================================
PRICE90_NEW_JPY_MIN = 10_000
PRICE90_NEW_JPY_MAX = 300_000
SALES_RANK_MAX = 1_000_000

MAX_EDGE_CM = 160
SUM_EDGES_CM = 200
MAX_WEIGHT_G = 30_000
MAX_VOLUME_CM3 = 180_000

# Finder一次フィルタ
MAX_EDGE_MM = MAX_EDGE_CM * 10
MAX_WEIGHT_G_FINDER = MAX_WEIGHT_G

# ============================================================
# SQL (UPSERT)
# ============================================================
SQL_MERGE = r"""
MERGE trx.amazon_cross_market_asin AS tgt
USING (
    SELECT
        ? AS asin,
        ? AS jp_title,
        ? AS jp_price,
        ? AS jp_category_id
) AS src
ON tgt.asin = src.asin
WHEN MATCHED THEN
    UPDATE SET
        tgt.last_seen_at   = SYSDATETIME(),
        tgt.jp_title       = src.jp_title,
        tgt.jp_price       = src.jp_price,
        tgt.jp_category_id = src.jp_category_id
WHEN NOT MATCHED THEN
    INSERT (
        asin,
        sent_at,
        last_seen_at,
        jp_title,
        jp_price,
        jp_category_id,
        us_title,
        us_price
    )
    VALUES (
        src.asin,
        NULL,
        SYSDATETIME(),
        src.jp_title,
        src.jp_price,
        src.jp_category_id,
        NULL,
        NULL
    );
"""

# ============================================================
# Keepa API tokenチェック
# ============================================================
def check_keepa_tokens() -> dict:
    url = f"{KEEPA_BASE}/token"
    params = {"key": KEEPA_API_KEY}
    
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        
        data = r.json()
        tokens_left = data.get("tokensLeft", 0)
        refill_in = data.get("refillIn", 0)
        
        # 頻繁に出すぎるとログが汚れるのでコメントアウト、必要なら復活
        # print(f"   [Keepa Status] 残り: {tokens_left} | 回復まで: {refill_in/1000:.1f}秒")
        
        return {
            "tokens_left": tokens_left,
            "refill_in_sec": refill_in / 1000.0
        }
        
    except Exception as e:
        print(f"   [Keepa Error] トークン確認失敗: {e}")
        return {"tokens_left": 0, "refill_in_sec": 0}

def wait_for_tokens(wait_time):
    wait_time = wait_time + 2
    print(f"   [Wait] token不足 {wait_time:.1f}秒 待機します...")
    time.sleep(wait_time)
    print("   [Resume] 再開します。")

# ============================================================
# Keepa API 呼び出し
# ============================================================
def keepa_query_jp(selection: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{KEEPA_BASE}/query"
    params = {"domain": DOMAIN_JP, "key": KEEPA_API_KEY}
    r = requests.post(url, params=params, data=json.dumps(selection), timeout=180)
    r.raise_for_status()
    tokens_data = check_keepa_tokens()
    while (tokens_data["tokens_left"] < LIMIT_TOKEN):
        print(f"   token不足 : {tokens_data['tokens_left']} (< {LIMIT_TOKEN})")
        wait_for_tokens(tokens_data["refill_in_sec"])
        tokens_data = check_keepa_tokens()
    return r.json()

def keepa_product_jp(asins: List[str], stats_days: int = 90) -> Dict[str, Any]:
    url = f"{KEEPA_BASE}/product"
    params = {
        "key": KEEPA_API_KEY,
        "domain": DOMAIN_JP,
        "asin": ",".join(asins),
        "stats": stats_days,
        "history": 0,
    }
    r = requests.get(url, params=params, timeout=180)
    r.raise_for_status()
    tokens_data = check_keepa_tokens()
    while (tokens_data["tokens_left"] < LIMIT_TOKEN):
        print(f"   token不足 : {tokens_data['tokens_left']} (< {LIMIT_TOKEN})")
        wait_for_tokens(tokens_data["refill_in_sec"])
        tokens_data = check_keepa_tokens()
    return r.json()

def keepa_product_us(asins: List[str]) -> Dict[str, Any]:
    url = f"{KEEPA_BASE}/product"
    params = {
        "key": KEEPA_API_KEY,
        "domain": DOMAIN_US,
        "asin": ",".join(asins),
        "stats": 0,
        "history": 0,
    }
    r = requests.get(url, params=params, timeout=180)
    r.raise_for_status()
    
    tokens_data = check_keepa_tokens()
    while (tokens_data["tokens_left"] < LIMIT_TOKEN):
        print(f"   token不足 (US Check): {tokens_data['tokens_left']} (< {LIMIT_TOKEN})")
        wait_for_tokens(tokens_data["refill_in_sec"])
        tokens_data = check_keepa_tokens()
        
    return r.json()


# ============================================================
# Finder selection JSON（JP抽出）
# ============================================================
# ★変更：category_id を引数に追加
def build_finder_selection(page: int, per_page: int, category_id: int) -> Dict[str, Any]:
    return {
        "page": page,
        "perPage": per_page,

        # カテゴリ（動的に指定）
        "categories_include": [category_id],

        # 物理商品
        "productType": 0,

        # 新品価格
        "avg90_NEW_gte": PRICE90_NEW_JPY_MIN,
        "avg90_NEW_lte": PRICE90_NEW_JPY_MAX,

        # ランキング
        "current_SALES_lte": SALES_RANK_MAX,

        # 新品FBA：0 / Amazonオファー無し
        "current_NEW_FBA_gte": -1,
        "current_NEW_FBA_lte": -1,
        "current_AMAZON_gte": -1,
        "current_AMAZON_lte": -1,

        # 一次サイズ・重量
        "packageLength_lte": MAX_EDGE_MM,
        "packageWidth_lte": MAX_EDGE_MM,
        "packageHeight_lte": MAX_EDGE_MM,
        "packageWeight_lte": MAX_WEIGHT_G_FINDER,
    }

# ★変更：category_id を引数に追加
def fetch_jp_finder_asins(category_id: int) -> List[str]:
    out: List[str] = []
    seen = set()

    print(f"--- Fetching Finder for Category: {category_id} ---")

    for page in range(MAX_PAGES):
        # category_id を渡す
        sel = build_finder_selection(page=page, per_page=PER_PAGE, category_id=category_id)
        data = keepa_query_jp(sel)

        asin_list = data.get("asinList") or []
        if not asin_list:
            break

        for a in asin_list:
            if a in seen:
                continue
            seen.add(a)
            out.append(a)

            # if len(out) >= LIMIT_ASINS_PER_CATEGORY: 
            #     return out

        time.sleep(SLEEP_SEC)
        print(f"   Page {page} done. Current count: {len(out)}")
        tokens_data = check_keepa_tokens()
        while (tokens_data["tokens_left"] < LIMIT_TOKEN):
            print(f"   token不足 (US Check): {tokens_data['tokens_left']} (< {LIMIT_TOKEN})")
            wait_for_tokens(tokens_data["refill_in_sec"])
            tokens_data = check_keepa_tokens()

    return out


# ============================================================
# 寸法・価格などユーティリティ
# ============================================================
def pick_package_dims_mm(prod: Dict[str, Any]) -> Optional[Tuple[int, int, int]]:
    h = prod.get("packageHeight")
    l = prod.get("packageLength")
    w = prod.get("packageWidth")
    if not all(isinstance(x, int) and x > 0 for x in (h, l, w)):
        return None
    return (h, l, w)

def mm_to_cm(x_mm: int) -> float:
    return x_mm / 10.0

def passes_size_weight_volume(prod_jp: Dict[str, Any]) -> bool:
    dims = pick_package_dims_mm(prod_jp)
    if dims is None:
        return False

    weight_g = prod_jp.get("packageWeight")
    if not (isinstance(weight_g, int) and weight_g > 0):
        return False

    h_mm, l_mm, w_mm = dims
    h_cm, l_cm, w_cm = mm_to_cm(h_mm), mm_to_cm(l_mm), mm_to_cm(w_mm)

    max_edge = max(h_cm, l_cm, w_cm)
    sum_edges = h_cm + l_cm + w_cm
    volume_cm3 = h_cm * l_cm * w_cm

    if max_edge > MAX_EDGE_CM:
        return False
    if sum_edges > SUM_EDGES_CM:
        return False
    if volume_cm3 >= MAX_VOLUME_CM3:
        return False
    if weight_g > MAX_WEIGHT_G:
        return False

    return True

def get_title(prod: Dict[str, Any]) -> Optional[str]:
    t = prod.get("title")
    if isinstance(t, str) and t.strip():
        return t.strip()
    return None

def get_price_guess_jpy(prod: Dict[str, Any]) -> Optional[int]:
    stats = prod.get("stats")
    if not isinstance(stats, dict):
        return None

    for k in ("avg90_NEW", "avg90NEW", "avg90_new", "avg90"):
        v = stats.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return int(v)

    v2 = stats.get("avg90")
    if isinstance(v2, dict):
        for kk in ("NEW", "new", "New"):
            vv = v2.get(kk)
            if isinstance(vv, (int, float)) and vv > 0:
                return int(vv)
    return None

# ============================================================
# DB UPSERT
# ============================================================
def upsert_jp(conn, asin: str, jp_title: Optional[str], jp_price: Optional[int], category_id: int) -> None:
    cur = conn.cursor()
    try:
        # category_id を動的に挿入
        cur.execute(SQL_MERGE, [asin, jp_title, jp_price, category_id])
        conn.commit()
    finally:
        cur.close()


# ============================================================
# メイン
# ============================================================
def main() -> None:
    conn = get_sql_server_connection()
    try:
        # ★変更：カテゴリリストをループ処理
        for cat_id, cat_name in category_map.items():
            print(f"\n========================================")
            print(f"Start processing Category ID: {cat_id} Category name: {cat_name}")
            print(f"========================================")

            # 1) JP Finder抽出（カテゴリごと）
            asins = fetch_jp_finder_asins(cat_id)
            print(f"[JP Finder] Category {cat_id}: {len(asins)} candidates found.")

            if not asins:
                print(f"No ASINs found for category {cat_id}. Skipping.")
                continue

            # 2) 詳細取得 → USチェック → DB UPSERT
            BATCH = 30
            upserted = 0
            skipped = 0

            for i in range(0, len(asins), BATCH):
                batch = asins[i : i + BATCH]

                # --- JPデータ取得 ---
                payload = keepa_product_jp(batch, stats_days=90)
                products = payload.get("products") or []
                
                pmap: Dict[str, Dict[str, Any]] = {}
                for p in products:
                    a = p.get("asin")
                    if isinstance(a, str):
                        pmap[a] = p

                # --- USデータ取得 (存在確認用) ---
                payload_us = keepa_product_us(batch)
                products_us = payload_us.get("products") or []
                us_existing_asins = {p.get("asin") for p in products_us if p.get("asin") and p.get("title") is not None}

                for asin in batch:
                    # JPデータなし
                    p = pmap.get(asin)
                    if not p:
                        print(f"    [JPデータなし]: {asin}")
                        skipped += 1
                        continue

                    # 最終条件チェック
                    if not passes_size_weight_volume(p):
                        skipped += 1
                        continue

                    # US存在チェック
                    if asin not in us_existing_asins:
                        # print(f"Skip: {asin} not found in US")
                        skipped += 1
                        continue

                    jp_title = get_title(p)
                    jp_price = get_price_guess_jpy(p)

                    print(f"   [UPSERT] ASIN: {asin}, Price: {jp_price}")
                    
                    # ★変更：ループ中の current_cat_id を渡す
                    upsert_jp(conn, asin=asin, jp_title=jp_title, jp_price=jp_price, category_id=cat_id)
                    upserted += 1

                time.sleep(SLEEP_SEC)

            print(f"\n[Result] Category {cat_id} -> Upserted: {upserted}, Skipped: {skipped}")
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()