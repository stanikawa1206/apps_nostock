# -*- coding: utf-8 -*-
# publish_joom_from_market_items.py
#
# 対象ASIN:
#   - nostock.ext.market_items から
#       marketplace='joom'
#       ASIN IS NOT NULL/空でない
#       DISTINCT(重複排除)
#       かつ trx.listings に
#           account='joom' AND vendor_item_id=ASIN
#       が存在するものは除外（未出品のみ）
#
# ASINごと処理:
#   - nostock.trx.vendor_item_amazon (vendor_item_id=ASIN) から商品情報を取得
#   - (任意) dbo.asin_master に upsert（同一DB内にある前提）
#   - Joomへ出品（SKU=ASIN）
#   - 成功したら trx.listings に出品済みとしてINSERT
#
# .env: apps/common/.env を固定で使用

from __future__ import annotations

import sys
import time
import traceback
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import requests
from dotenv import load_dotenv


# =========================
# sys.path bootstrap（直実行でも apps.* import できるように）
# =========================
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]  # .../apps/publish/xxx.py -> 2つ上がプロジェクトルート想定
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# utils.py の get_sql_server_connection を使用
from apps.common.utils import get_sql_server_connection, translate_ja_to_en_deepl

MAX_LISTINGS = 5

from apps.snapshot.amazon_snapshot import (
    run_snapshot,
    upsert_vendor_item_amazon,
    judge_amazon_in_stock,
)


# =========================
# .env 固定ロード
# =========================
dotenv_path = PROJECT_ROOT / "apps" / "common" / ".env"
load_dotenv(dotenv_path=str(dotenv_path), override=True)

# 環境変数（JOOM）
# =========================
JOOM_CLIENT_ID = os.getenv("JOOM_CLIENT_ID")
JOOM_CLIENT_SECRET = os.getenv("JOOM_CLIENT_SECRET")
JOOM_REFRESH_TOKEN = os.getenv("JOOM_REFRESH_TOKEN")
JOOM_STORE_ID = os.getenv("JOOM_STORE_ID")

JOOM_API_V3 = "https://api-merchant.joom.com/api/v3"
JOOM_REFRESH_ENDPOINT = "https://api-merchant.joom.com/api/v2/oauth/refresh_token"

# =========================
# SQL: 候補ASIN取得（ext.market_items）
# =========================
def fetch_candidate_asins(limit: int = 1000) -> List[str]:
    sql = """
SELECT DISTINCT TOP (?) LTRIM(RTRIM(m.ASIN)) AS ASIN
FROM ext.market_items AS m
WHERE m.marketplace = 'joom'
  AND m.ASIN IS NOT NULL
  AND LTRIM(RTRIM(m.ASIN)) <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM trx.listings AS l
      WHERE l.account = 'joom'
        AND l.vendor_item_id = m.ASIN
  )
ORDER BY LTRIM(RTRIM(m.ASIN));
"""
    with get_sql_server_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    return [r[0].strip() for r in rows if r and r[0]]

# =========================
# JOOM: access_token 取得
# =========================
def joom_access_token_from_refresh() -> str:
    r = requests.post(
        JOOM_REFRESH_ENDPOINT,
        data={
            "grant_type": "refresh_token",
            "refresh_token": JOOM_REFRESH_TOKEN,
            "client_id": JOOM_CLIENT_ID,
            "client_secret": JOOM_CLIENT_SECRET,
        },
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"[JOOM refresh] NG: {r.status_code} {r.text[:200]}")

    js = r.json()
    data = js.get("data", js) if isinstance(js, dict) else {}
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"[JOOM refresh] access_token missing: {js}")

    return token


# =========================
# JOOM: 既存チェック（SKU=ASIN）
# =========================
def joom_find_variant_by_sku(token: str, sku: str) -> Optional[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    r = requests.get(f"{JOOM_API_V3}/variants", headers=headers, params={"sku": sku}, timeout=30)
    if r.status_code == 200:
        items = (r.json() or {}).get("items", [])
        return items[0] if items else None
    return None


# =========================
# JOOM: 出品（新規）
# =========================
def joom_create_product(
    token: str,
    store_id: str,
    name_en: str,
    desc_en: str,
    sku: str,
    price: float,
    qty: int,
    image_ids: List[str],
    main_image_url: str = None,
) -> requests.Response:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    payload: Dict[str, Any] = {
        "storeId": str(store_id),
        "name": (str(name_en)[:255] if name_en else str(sku)),
        "description": desc_en or "",
        "sku": str(sku),  # ★ product レベルのsku (必須)
        "mainImage": main_image_url or "https://via.placeholder.com/550x550",  # ★ 必須
        "variants": [{
            "sku": str(sku),  # ★ variant レベルのsku (必須)
            "price": str(round(float(price), 2)),
            "inventory": int(qty),
            "currency": "USD",  # ★ 必須
        }],
    }

    if image_ids:
        payload["extraImages"] = image_ids

    url = f"{JOOM_API_V3}/products/create"
    return requests.post(url, headers=headers, json=payload, timeout=60)

def insert_listing_record(listing_id: str, asin: str) -> None:
    sql = """
INSERT INTO trx.listings (
    listing_id,
    account,
    start_time,
    vendor_item_id,
    vendor_name
)
VALUES (
    ?,
    'JOOM',
    GETDATE(),
    ?,
    'amazon'
);
"""
    with get_sql_server_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (listing_id, asin))
        conn.commit()

# =========================
# 1 ASIN 処理
# =========================
from typing import List

def process_asin(asin: str, store_id: str, access_token: str) -> None:
    print(f"[ASIN] start asin={asin}")

    # 1) スナップショット取得
    snapshot = run_snapshot(asin)

    # 2) 基本情報 upsert
    with get_sql_server_connection() as conn:
        upsert_vendor_item_amazon(conn, snapshot)

    # 3) 在庫判定
    in_stock, reason = judge_amazon_in_stock(snapshot)
    print(f"[ASIN] in_stock={in_stock} reason={reason}")

    # 3-1) 在庫なし → 理由だけ書いて終了
    if not in_stock:
        with get_sql_server_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
UPDATE trx.vendor_item_amazon
SET
    out_of_stock_reason = ?,
    last_updated_at = GETDATE()
WHERE vendor_item_id = ?;
""",
                (reason, asin),
            )
            conn.commit()

        print(f"[ASIN] done asin={asin} (out_of_stock only)")
        return

    # 4) title_en 翻訳（在庫ありのときのみ）
    title_ja = (snapshot.get("title") or "").strip()
    product_title_en = translate_ja_to_en_deepl(title_ja) if title_ja else asin

    with get_sql_server_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
UPDATE trx.vendor_item_amazon
SET product_title_en = ?,
    out_of_stock_reason = NULL,
    last_updated_at = GETDATE()
WHERE vendor_item_id = ?;
""",
            (product_title_en, asin),
        )
        conn.commit()

    print(f"[ASIN] product_title_en='{product_title_en}'")

    # 5) JOOM用 description（DB保存しない）
    desc_ja = (snapshot.get("description") or "").strip()
    desc_en = (
        "Condition: New\n"
        "Authentic product.\n"
        "Ships from Japan.\n"
        "Please check images for details.\n"
        "\n"
        "---- Japanese description ----\n"
        f"{desc_ja}"
    ).strip()

    print("[JOOM] description_en generated")

    # 6) JOOM売価計算（DB保存しない）
    s = snapshot.get("buybox_price_jpy") or snapshot.get("new_current_price")
    if s is None:
        print("[PRICE] buybox_price_jpy と new_current_price の両方が None -> skip pricing")
        return

    s = float(s)
    fee_rate = 0.15
    profit_rate = 0.10
    fx_rate = 150.0

    den = 1.0 - fee_rate - profit_rate  # 0.75
    x_jpy = s / den
    x_usd = x_jpy / fx_rate

    print(f"[PRICE] s_jpy={s:.0f} sell_jpy={x_jpy:.0f} sell_usd={x_usd:.2f}")
    
    # 7) 既存SKUチェック（JOOM）
    v = joom_find_variant_by_sku(access_token, asin)
    if v:
        print(f"[SKIP] Joomに既に存在(SKU): {asin} variantId={v.get('id')}")
        return

    # 8) 画像
    image_ids: List[str] = []
    qty = 1

    # 9) JOOM 出品
    time.sleep(1.5)
    print("[DEBUG] POST URL =", f"{JOOM_API_V3}/products")

    res = joom_create_product(
        access_token,
        store_id,
        product_title_en,
        desc_en,
        asin,
        x_usd,
        qty,
        image_ids,
    )

    print(f"[DEBUG] status_code={res.status_code}")
    print(f"[DEBUG] response={res.text[:500]}")

    if res.status_code != 200:
        print(f"[NG] JOOM 出品失敗: {asin} status={res.status_code}")
        return

    js = res.json() if res.text else {}
    print(f"[DEBUG] parsed_json={str(js)[:500]}")

    product_id = js.get("data", {}).get("id")
    if not product_id:
        print(f"[WARN] JOOM 出品だが id が取れない: {asin} body={str(js)[:200]}")
        return

    insert_listing_record(str(product_id), asin)
    print(f"[OK] JOOM 出品 & trx.listings記録: ASIN={asin} listing_id={product_id}")




# =========================
# Main
# =========================
def main():
    if not (JOOM_CLIENT_ID and JOOM_CLIENT_SECRET and JOOM_REFRESH_TOKEN and JOOM_STORE_ID):
        print("[ERR] JOOM_* 環境変数が不足しています（apps/common/.env を確認）")
        return

    token = joom_access_token_from_refresh()
    asins = fetch_candidate_asins(limit=1000)

    for i, asin in enumerate(asins, 1):
        if i > MAX_LISTINGS:
            print(f"出品上限 {MAX_LISTINGS} 件に達したため終了")
            break
        
        try:
            process_asin(asin, JOOM_STORE_ID, token)
        except Exception as e:
            print(f"[ERR] {asin}: {e}")
            traceback.print_exc()

        time.sleep(2.0)

    print("全処理完了")


if __name__ == "__main__":
    main()
