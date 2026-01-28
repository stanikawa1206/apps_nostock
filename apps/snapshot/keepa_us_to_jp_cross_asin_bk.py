# -*- coding: utf-8 -*-
from __future__ import annotations

"""
apps/seed/keepa_jp_seed_asin.py

第一段階（JPのみ）:
- Keepa JP(Product Finder)で候補ASINを抽出
- Keepa /product(JP)で寸法/重量/タイトル/価格(取れれば)を取得
- Pythonで最終条件（最大長辺/三辺合計/体積/重量）を満たすASINだけをDBにUPSERT

保存先:
- trx.amazon_cross_market_asin
  - asin: PK
  - sent_at: NULLのまま（次工程用）
  - last_seen_at: SYSDATETIME()
  - jp_title/jp_price/jp_category_id: 記録
  - us_title/us_price: NULL（USチェックは第二段階）
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

# .env 読み込み（作業ディレクトリ=D:\apps_nostock を前提）
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from apps.common.utils import get_sql_server_connection

# ============================================================
# 実行条件（固定）
# ============================================================
LIMIT_ASINS: int = 60  # テストなので最初の100件だけ

# テストとして「スポーツ＆アウトドア」に絞る
JP_CATEGORY_ID: int = 14304371

# Finder ページング（必要に応じてここを書き換える）
MAX_PAGES: int = 50
PER_PAGE: int = 50

# API呼び出し間隔（必要に応じてここを書き換える）
SLEEP_SEC: float = 0.2

# ============================================================
# Keepa API 設定
# ============================================================
KEEPA_API_KEY = os.getenv("KEEPA_API_KEY") or os.getenv("KEEPA_KEY")
if not KEEPA_API_KEY:
    raise RuntimeError("KEEPA_API_KEY (or KEEPA_KEY) が未設定です。.env を確認してください。")

KEEPA_BASE = "https://api.keepa.com"
DOMAIN_JP = 5  # Amazon.co.jp
LIMIT_TOKEN = 400

# ============================================================
# フェーズ1（JP抽出条件）
# ============================================================
PRICE90_NEW_JPY_MIN = 10_000
PRICE90_NEW_JPY_MAX = 300_000
SALES_RANK_MAX = 1_000_000

MAX_EDGE_CM = 160
SUM_EDGES_CM = 200
MAX_WEIGHT_G = 30_000
MAX_VOLUME_CM3 = 180_000  # ★追加：体積 < 180,000 cm3

# Finder一次フィルタ（取りすぎ抑制のため）
MAX_EDGE_MM = MAX_EDGE_CM * 10  # cm→mm
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
    """
    Keepaの現在のトークン状態を確認する。
    通信環境によりヘッダーが削除される場合でも、ボディから情報を取得するため確実に動作します。
    
    Returns:
        dict: {'tokens_left': int, 'refill_in_sec': float}
    """
    url = f"{KEEPA_BASE}/token"
    params = {"key": KEEPA_API_KEY}
    
    try:
        # このリクエスト自体はトークンを消費しません
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        
        data = r.json()
        tokens_left = data.get("tokensLeft", 0)
        refill_in = data.get("refillIn", 0)
        
        print(f"   [Keepa Status] 残り: {tokens_left} | 回復まで: {refill_in/1000:.1f}秒")
        
        return {
            "tokens_left": tokens_left,
            "refill_in_sec": refill_in / 1000.0
        }
        
    except Exception as e:
        print(f"   [Keepa Error] トークン確認失敗: {e}")
        return {"tokens_left": 0, "refill_in_sec": 0}

def wait_for_tokens(wait_time):
    """
    トークンが指定数以下なら、回復するまで自動で待機する便利関数
    """
    wait_time = wait_time + 2 # 余裕を持って+2秒
    print(f"   [Wait] token不足 {wait_time:.1f}秒 待機します...")
    time.sleep(wait_time)
    print("   [Resume] 再開します。")

# ============================================================
# Keepa API 呼び出し
# ============================================================
def keepa_query_jp(selection: Dict[str, Any]) -> Dict[str, Any]:
    """Product Finder (/query) POST"""
    url = f"{KEEPA_BASE}/query"
    params = {"domain": DOMAIN_JP, "key": KEEPA_API_KEY}
    r = requests.post(url, params=params, data=json.dumps(selection), timeout=180)
    r.raise_for_status()
    tokens_data = check_keepa_tokens()
    while (tokens_data["tokens_left"] < LIMIT_TOKEN):
        print(f"   token不足 : {tokens_data["tokens_left"]} (< {LIMIT_TOKEN})")
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
        print(f"   token不足 : {tokens_data["tokens_left"]} (< {LIMIT_TOKEN})")
        wait_for_tokens(tokens_data["refill_in_sec"])
        tokens_data = check_keepa_tokens()
    return r.json()



# ============================================================
# Finder selection JSON（JP抽出）
# ============================================================
def build_finder_selection(page: int, per_page: int) -> Dict[str, Any]:
    return {
        "page": page,
        "perPage": per_page,

        # カテゴリ（テスト）
        "categories_include": [JP_CATEGORY_ID],

        # 物理商品
        "productType": 0,

        # 新品価格（90日平均）: 10,000 ～ 300,000 円
        "avg90_NEW_gte": PRICE90_NEW_JPY_MIN,
        "avg90_NEW_lte": PRICE90_NEW_JPY_MAX,

        # ランキング: 100万位以内
        "current_SALES_lte": SALES_RANK_MAX,

        # 新品FBA：0 / Amazonオファー無し
        # Keepaでは「存在しない価格 = -1」扱いが一般的なので -1固定一致
        "current_NEW_FBA_gte": -1,
        "current_NEW_FBA_lte": -1,
        "current_AMAZON_gte": -1,
        "current_AMAZON_lte": -1,

        # 一次サイズ・重量（mm/g）
        "packageLength_lte": MAX_EDGE_MM,
        "packageWidth_lte": MAX_EDGE_MM,
        "packageHeight_lte": MAX_EDGE_MM,
        "packageWeight_lte": MAX_WEIGHT_G_FINDER,
    }


def fetch_jp_finder_asins() -> List[str]:
    out: List[str] = []
    seen = set()

    for page in range(MAX_PAGES):
        sel = build_finder_selection(page=page, per_page=PER_PAGE)
        data = keepa_query_jp(sel)

        asin_list = data.get("asinList") or []
        if not asin_list:
            break

        for a in asin_list:
            if a in seen:
                continue
            seen.add(a)
            out.append(a)

            if len(out) >= LIMIT_ASINS:  # ← テストなので固定で100件
                return out

        time.sleep(SLEEP_SEC)

    return out


# ============================================================
# 寸法フィルタ（mm→cm, 体積cm3）
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


# ============================================================
# タイトル・価格抽出（取れなければNone）
# ============================================================
def get_title(prod: Dict[str, Any]) -> Optional[str]:
    t = prod.get("title")
    if isinstance(t, str) and t.strip():
        return t.strip()
    return None


def get_price_guess_jpy(prod: Dict[str, Any]) -> Optional[int]:
    """
    JP側価格（90日平均新品）を取れる範囲で拾う。
    Keepa stats構造は揺れるので、取れない場合は None。
    """
    stats = prod.get("stats")
    if not isinstance(stats, dict):
        return None

    # よくある候補を最小限で
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
def upsert_jp(conn, asin: str, jp_title: Optional[str], jp_price: Optional[int]) -> None:
    cur = conn.cursor()
    try:
        cur.execute(SQL_MERGE, [asin, jp_title, jp_price, JP_CATEGORY_ID])
        conn.commit()
    finally:
        cur.close()


# ============================================================
# メイン
# ============================================================
def main() -> None:
    # 1) JP Finder抽出
    asins = fetch_jp_finder_asins()
    print(f"[JP Finder] candidates: {len(asins)} (category_id={JP_CATEGORY_ID})")

    if not asins:
        return

    # 2) /product でJP詳細 → 最終条件判定 → DB UPSERT
    conn = get_sql_server_connection()
    try:
        BATCH = 30
        upserted = 0
        skipped = 0

        for i in range(0, len(asins), BATCH):
            batch = asins[i : i + BATCH]

            payload = keepa_product_jp(batch, stats_days=90)
            products = payload.get("products") or []

            # asin→prod
            pmap: Dict[str, Dict[str, Any]] = {}
            for p in products:
                a = p.get("asin")
                if isinstance(a, str):
                    pmap[a] = p

            for asin in batch:
                p = pmap.get(asin)
                if not p:
                    skipped += 1
                    continue

                # 最終条件（最大長辺/三辺合計/体積/重量）
                if not passes_size_weight_volume(p):
                    skipped += 1
                    continue

                jp_title = get_title(p)
                jp_price = get_price_guess_jpy(p)

                # DBに書き込むべき情報
                print(f"ASIN: {asin}, Title: {jp_title[:100]}, Price: {jp_price}")
                
                # テスト用でDBには書き込まないようにする
                # upsert_jp(conn, asin=asin, jp_title=jp_title, jp_price=jp_price)
                upserted += 1

            time.sleep(SLEEP_SEC)

        print("---- summary ----")
        print(f"JP candidates: {len(asins)}")
        print(f"DB upserted : {upserted}")
        print(f"skipped     : {skipped}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
