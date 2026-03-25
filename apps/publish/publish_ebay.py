# -*- coding: utf-8 -*-
# publish_ebay.py — listings / vendor_item 対応（Shops/通常 両対応・processing_by方式, Py3.8/3.9互換）

from __future__ import annotations

# =========================
# Standard library
# =========================
import random
import re
import sys
import time
import socket  
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import os
from dotenv import load_dotenv
import boto3
from datetime import timedelta  # 追加（mainのstateで使う）
from dataclasses import dataclass
from playwright.sync_api import sync_playwright
from selenium import webdriver

# =========================
# Third-party
# =========================
import pyodbc

# =========================
# sys.path bootstrap: file-direct run safe
# =========================
# このファイル: D:\apps_nostock\apps\publish\publish_ebay.py
# プロジェクトルート: D:\apps_nostock  ← parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# =========================
# Local (project)
# =========================
from apps.common.utils import (
    compute_start_price_usd,
    generate_ebay_description,
    get_sql_server_connection,
    send_mail,
    translate_to_english,
    contains_risky_word,
    get_openai_client,
)

from apps.adapters.ebay_api import ApiHandledError, ListingLimitError, post_one_item
from apps.adapters.mercari_search import fetch_active_presets
from apps.adapters.mercari_item_status import (
    MercariItemUnavailableError,
    mark_vendor_item_unavailable,
)
from apps.adapters.mercari_item_status import fetch_mercari_api_data,_parse_status_from_res,detect_status_from_mercari_shops
from datetime import datetime

# ========= 固定値／運用設定 =========
IMG_LIMIT     = 20
BATCH_COMMIT  = 10
MAX_PARALLEL_PC = 8  # 1アカウントあたりの最大同時稼働PC数

# ========= NG打刻・スキップ関連定義 =========
NG_HEADS_FOR_TIMESTAMP: Set[str] = {
    "古い更新",
    "計算価格が範囲外",
}

HEADS_FOR_7DAY_SKIP: Set[str] = {
    "古い更新",
    "計算価格が範囲外",
}

def is_fatal_renderer_error(e: Exception) -> bool:
    s = str(e).lower()

    return (
        "timed out receiving message from renderer" in s
        or "unable to receive message from renderer" in s
        or ("disconnected" in s and "renderer" in s)
        or "chrome not reachable" in s
        or "httpconnectionpool(host='localhost'" in s
        or "read timed out" in s
    )

class FatalRendererError(Exception):
    pass

# ========= 詳細解析（Shops / 通常） =========
def parse_detail_shops(page, url: str, preset: str, vendor_name: str, driver) -> Dict[str, Any]:
    """
    メルカリShops:
    状態判定 → Selenium
    データ取得 → Playwright(API)
    """
    import time
    
    # =========================
    # ① 状態判定（Selenium）
    # =========================
    driver.get(url)
    status, price = detect_status_from_mercari_shops(driver)

    if status != "販売中":
        raise MercariItemUnavailableError(status)

    # =========================
    # ② API取得（Playwright）
    # =========================
    api_payload = {"data": None}

    def handle_response(response):
        if "view=FULL" in response.url:
            try:
                api_payload["data"] = response.json()
            except:
                pass

    page.on("response", handle_response)
    
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)

        start = time.time()
        while api_payload["data"] is None:
            if time.time() - start > 15:
                raise Exception("API timeout")

            page.mouse.wheel(0, 800)
            time.sleep(1)

        res = api_payload["data"]

        # ❌ ここで削除判定しない（Seleniumに任せる）
        if res is None:
            raise Exception("API取得失敗")  # ← retry対象

        detail = res.get("productDetail", {})
        shop = detail.get("shop", {})

        update_time_str = res.get("updateTime")
        vendor_updated_at = None
        if update_time_str:
            vendor_updated_at = datetime.fromisoformat(update_time_str.replace("Z", "+00:00"))

        return {
            "vendor_name": vendor_name,
            "title_jp": res.get("displayName"),
            "title_en": "",
            "price": int(res.get("price", 0)),  # ← JSON優先
            "vendor_updated_at": vendor_updated_at,
            "shipping_region": detail.get("shippingFromArea", {}).get("displayName", ""),
            "shipping_days": detail.get("shippingDuration", {}).get("displayName", ""),
            "seller_id": shop.get("name", ""),
            "seller_name": shop.get("displayName", ""),
            "rating_count": int(shop.get("shopStats", {}).get("reviewCount", 0)),
            "images": detail.get("photos", []),
            "preset": preset,
            "description": detail.get("description", ""),
            "description_en": "",
        }

    finally:
        page.remove_listener("response", handle_response)

def parse_detail_personal(page, url: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """
    【Newer Version】通常メルカリ: Playwright(API) 1回で全データを解析。
    Seleniumを使わず、JSONから直接「39分前」や「セラー評価」を抽出します。
    """
    # 1. APIからJSONデータを取得
    res, _ = fetch_mercari_api_data(page, url)
    if not res or res.get("result") != "OK":
        # データが取れない（404等）場合は削除扱い
        raise MercariItemUnavailableError("削除")
    
    # 2. ステータスチェック
    status, _ = _parse_status_from_res(res)
    if status != "販売中":
        raise MercariItemUnavailableError(status)
    
    item = res.get("data", {})

    updated = item.get("updated")

    vendor_updated_at = None
    if updated:
        vendor_updated_at = datetime.fromtimestamp(updated)

    # 4. rec の組み立て
    return {
        "vendor_name": vendor_name,
        "title_jp": item.get("name"),
        "title_en": "",
        "price": int(item.get("price", 0)),
        "vendor_updated_at": vendor_updated_at,   #
        "shipping_region": item.get("shipping_from_area", {}).get("name", ""),
        "shipping_days": item.get("shipping_duration", {}).get("name", ""),
        "seller_id": str(item.get("seller", {}).get("id", "")),
        "seller_name": item.get("seller", {}).get("name", ""),
        "rating_count": int(item.get("seller", {}).get("num_ratings", 0)),
        "images": item.get("photos", []),
        "preset": preset,
        "description": item.get("description", ""),
        "description_en": "",
    }

# ========= DB I/O =========
def _none_if_blank(s: Any) -> Optional[str]:
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    return s if s else None

UPSERT_VENDOR_ITEM_SQL = """
MERGE INTO [trx].[vendor_item] WITH (HOLDLOCK) AS tgt
USING (
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
) AS src (
    vendor_name, vendor_item_id,
    title_jp, title_en,
    description, description_en,
    price,
    last_updated_str,
    shipping_region, shipping_days,
    seller_id,
    preset, vendor_page,
    image_url1, image_url2, image_url3, image_url4, image_url5,
    image_url6, image_url7, image_url8, image_url9, image_url10,
    image_url11, image_url12, image_url13, image_url14, image_url15,
    image_url16, image_url17, image_url18, image_url19, image_url20,
    listing_head, listing_detail
)
ON (tgt.vendor_name = src.vendor_name AND tgt.vendor_item_id = src.vendor_item_id)
WHEN MATCHED THEN
    UPDATE SET
        title_jp         = COALESCE(src.title_jp, tgt.title_jp),
        title_en         = COALESCE(src.title_en, tgt.title_en),
        description      = COALESCE(src.description, tgt.description),
        description_en   = COALESCE(src.description_en, tgt.description_en),

        last_updated_str = COALESCE(src.last_updated_str, tgt.last_updated_str),
        shipping_region  = COALESCE(src.shipping_region, tgt.shipping_region),
        shipping_days    = COALESCE(src.shipping_days, tgt.shipping_days),
        seller_id        = COALESCE(src.seller_id, tgt.seller_id),

        image_url1       = COALESCE(src.image_url1, tgt.image_url1),
        image_url2       = COALESCE(src.image_url2, tgt.image_url2),
        image_url3       = COALESCE(src.image_url3, tgt.image_url3),
        image_url4       = COALESCE(src.image_url4, tgt.image_url4),
        image_url5       = COALESCE(src.image_url5, tgt.image_url5),
        image_url6       = COALESCE(src.image_url6, tgt.image_url6),
        image_url7       = COALESCE(src.image_url7, tgt.image_url7),
        image_url8       = COALESCE(src.image_url8, tgt.image_url8),
        image_url9       = COALESCE(src.image_url9, tgt.image_url9),
        image_url10      = COALESCE(src.image_url10, tgt.image_url10),
        image_url11      = COALESCE(src.image_url11, tgt.image_url11),
        image_url12      = COALESCE(src.image_url12, tgt.image_url12),
        image_url13      = COALESCE(src.image_url13, tgt.image_url13),
        image_url14      = COALESCE(src.image_url14, tgt.image_url14),
        image_url15      = COALESCE(src.image_url15, tgt.image_url15),
        image_url16      = COALESCE(src.image_url16, tgt.image_url16),
        image_url17      = COALESCE(src.image_url17, tgt.image_url17),
        image_url18      = COALESCE(src.image_url18, tgt.image_url18),
        image_url19      = COALESCE(src.image_url19, tgt.image_url19),
        image_url20      = COALESCE(src.image_url20, tgt.image_url20),
        prev_price       = CASE
                             WHEN src.price IS NOT NULL AND tgt.price <> src.price THEN tgt.price
                             ELSE tgt.prev_price
                           END,
        price            = COALESCE(src.price, tgt.price),

        status           = N'販売中',
        preset           = COALESCE(src.preset, tgt.preset),
        vendor_page      = COALESCE(src.vendor_page, tgt.vendor_page),
        last_checked_at  = SYSDATETIME(),

        [出品状況]       = COALESCE(src.listing_head, tgt.[出品状況]),
        [出品状況詳細] = CASE
            WHEN src.listing_head = N'出品' THEN N''
            ELSE COALESCE(src.listing_detail, tgt.[出品状況詳細])
        END,
        last_ng_at = CASE
            WHEN src.listing_head = N'出品' THEN NULL
            WHEN src.listing_head IN (N'古い更新', N'計算価格が範囲外',N'NG(セラー評価)') THEN SYSDATETIME()
            ELSE NULL
        END
WHEN NOT MATCHED THEN
    INSERT (
        vendor_name, vendor_item_id,
        title_jp, title_en, title_en_bk,
        description, description_en,
        price,
        last_updated_str, shipping_region, shipping_days, seller_id,
        preset, vendor_page,
        image_url1, image_url2, image_url3, image_url4, image_url5,
        image_url6, image_url7, image_url8, image_url9, image_url10,
        created_at, last_checked_at, prev_price, status,
        image_url11, image_url12, image_url13, image_url14, image_url15,
        image_url16, image_url17, image_url18, image_url19, image_url20,
        [出品状況], [出品状況詳細],
        last_ng_at
    )

    VALUES (
        src.vendor_name,
        src.vendor_item_id,
        src.title_jp,
        src.title_en,
        src.title_en,
        src.description,
        src.description_en,
        src.price,
        src.last_updated_str,
        src.shipping_region,
        src.shipping_days,
        src.seller_id,
        src.preset,
        src.vendor_page,

        -- ① まず image1〜10
        src.image_url1,
        src.image_url2,
        src.image_url3,
        src.image_url4,
        src.image_url5,
        src.image_url6,
        src.image_url7,
        src.image_url8,
        src.image_url9,
        src.image_url10,

        -- ② 次に日付系
        SYSDATETIME(),   -- created_at
        SYSDATETIME(),   -- last_checked_at
        NULL,            -- prev_price
        N'販売中',        -- status

        -- ③ そのあと image11〜20
        src.image_url11,
        src.image_url12,
        src.image_url13,
        src.image_url14,
        src.image_url15,
        src.image_url16,
        src.image_url17,
        src.image_url18,
        src.image_url19,
        src.image_url20,

        -- ④ 最後
        COALESCE(src.listing_head, N''),
        COALESCE(src.listing_detail, N''),

        CASE
            WHEN src.listing_head IN (N'古い更新', N'計算価格が範囲外', N'NG(セラー評価)')
            THEN SYSDATETIME()
            ELSE NULL
        END
    )
    
OUTPUT
    $action                 AS action,
    inserted.vendor_item_id AS vendor_item_id,
    deleted.price           AS old_price,
    inserted.price          AS new_price,
    inserted.status         AS status;
"""

def upsert_vendor_item(conn, rec: Dict[str, Any]):
    imgs = (rec.get("images") or [])
    imgs = (imgs + [None] * 20)[:20]

    preset_val  = _none_if_blank(rec.get("preset"))
    vendor_page = rec.get("vendor_page")

    title_jp = _none_if_blank(rec.get("title_jp"))
    title_en = _none_if_blank(rec.get("title_en"))
    desc_jp  = _none_if_blank(rec.get("description"))
    desc_en  = _none_if_blank(rec.get("description_en"))

    last_updated_str = _none_if_blank(rec.get("last_updated_str"))
    shipping_region  = _none_if_blank(rec.get("shipping_region"))
    shipping_days    = _none_if_blank(rec.get("shipping_days"))
    seller_id        = _none_if_blank(rec.get("seller_id"))

    price_val = rec.get("price")
    if price_val is not None:
        try:
            price_val = int(price_val)
        except Exception:
            price_val = None

    listing_head   = _none_if_blank(rec.get("listing_head"))
    listing_detail = _none_if_blank(rec.get("listing_detail"))

    params = (
        rec["vendor_name"],
        rec["vendor_item_id"],

        title_jp,
        title_en,
        desc_jp,
        desc_en,

        price_val,
        last_updated_str,
        shipping_region,
        shipping_days,
        seller_id,

        preset_val,
        vendor_page,

        *imgs,

        listing_head,
        listing_detail,
    )

    with conn.cursor() as cur:
        cur.execute(UPSERT_VENDOR_ITEM_SQL, params)
        _ = cur.fetchall()

def record_ebay_listing(listing_id: str, account_name: str, vendor_item_id: str, vendor_name: str):
    if not listing_id:
        return

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
INSERT INTO [trx].[listings]
    ([listing_id], [start_time], [account], [vendor_item_id], [vendor_name], [is_deleted])
VALUES
    (?, SYSDATETIME(), ?, ?, ?, 0);
""", (listing_id, account_name, vendor_item_id, vendor_name))

        conn.commit()
    finally:
        conn.close()


def _truncate_for_db(s: str, limit: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= limit else s[:max(0, limit-1)] + "…"

# ===== タイトルルール / 文字列補助 =====
TITLE_RULES: List[Tuple[str, str]] = []

def load_title_rules(conn) -> List[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT rule_id, pattern, replacement
              FROM mst.title_replace_rules
             WHERE pattern IS NOT NULL AND LTRIM(RTRIM(pattern)) <> N''
             ORDER BY rule_id
        """)
        rows = cur.fetchall()
    rules: List[Tuple[str, str]] = []
    for _id, pat, rep in rows:
        pat = (pat or "").strip()
        rep = (rep or "")
        if pat:
            rules.append((pat, rep))
    return rules

def clean_for_ebay(text: str) -> str:
    if not text:
        return ""
    s = text
    s = re.sub(r"https?://\S+", "", s)
    s = re.sub(r"\bwww\.\S+", "", s)
    s = re.sub(r"\b\S+@\S+\.\S+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _replace_literal_ignorecase(text: str, old: str, new: str) -> str:
    if not text or not old:
        return text or ""
    pattern = re.compile(re.escape(old), flags=re.IGNORECASE)
    return pattern.sub(new, text)

def apply_title_rules_literal_ci(title_en: str, rules: List[Tuple[str, str]]) -> str:
    s = title_en or ""
    for pat, rep in rules:
        s = _replace_literal_ignorecase(s, pat, rep)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def shipping_usd_from_jpy(jpy: int, usd_jpy_rate: float) -> str:
    usd = (Decimal(jpy) / Decimal(str(usd_jpy_rate))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{usd:.2f}"

def smart_truncate80(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= 80:
        return s
    cut = s[:77]
    if " " in cut and not cut.endswith(" "):
        cut = cut[: cut.rfind(" ")]
    return cut.rstrip() + "..."

def fetch_existing_title_en(conn, vendor_name: str, vendor_item_id: str) -> Optional[str]:
    sql = """
        SELECT title_en
          FROM trx.vendor_item WITH (NOLOCK)
         WHERE vendor_name = ? AND vendor_item_id = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql, (vendor_name, vendor_item_id))
        row = cur.fetchone()
        if not row:
            return None
        val = (row[0] or "").strip()
        return val or None

# ========= バッチコミット補助 =========
def _maybe_commit(conn, counter: int, batch: int) -> int:
    if counter >= batch:
        conn.commit()
        return 0
    return counter

def debug_render_sql(sql: str, params: list) -> str:
    def fmt(v):
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "N'" + v.replace("'", "''") + "'"
        return str(v)

    out = sql
    for p in params:
        out = out.replace("?", fmt(p), 1)
    return out

def _check_shipping_condition_values(region: Optional[str], days: Optional[str]) -> Tuple[bool, bool]:
    region = (region or "").strip()
    days   = (days or "").strip()

    if not region and not days:
        return False, False

    bad_days = {"8〜14日で発送", "4〜7日で発送", "4~7日で発送","90日以内で発送"}

    if region == "海外":
        return True, True
    if days in bad_days:
        return True, True

    return False, True

def postprocess_common_title(jp_title: str, desc_jp: str, title_en: str) -> str:
    jp = jp_title or ""
    desc = desc_jp or ""
    t = title_en or ""

    if "未使用" not in jp and "新品" not in jp:
        t = re.sub(r"\bUnused\b", "Excellent", t, flags=re.IGNORECASE)

    if not any(k in jp or k in desc for k in ["ヴェルニ", "エナメル", "vernis"]):
        t = re.sub(r"\bVernis\b", "", t, flags=re.IGNORECASE)

    t = re.sub(r"\bPython\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t

def postprocess_title(jp_title: str, desc_jp: str, title_en: str) -> str:
    title_en = postprocess_common_title(jp_title or "", desc_jp or "", title_en or "")
    return re.sub(r"\s+", " ", title_en or "").strip()

DANGEROUS_TITLE_WORDS = {
    r"\bpython\b": "",
    r"\bsnakeskin\b": "",
    r"\bcrocodile\b": "",
    r"\balligator\b": "",
    r"\blizard\b": "",
    r"\bostrich\b": "",
    r"\bstingray\b": "",
}

def sanitize_title_dangerous_words(title: str) -> str:
    s = title or ""
    for pat, repl in DANGEROUS_TITLE_WORDS.items():
        s = re.sub(pat, repl, s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

SQL_UPSERT_MST_SELLER = """
MERGE INTO mst.seller AS tgt
USING (VALUES (?, ?, ?, ?)) AS src (vendor_name, seller_id, seller_name, rating_count)
ON (tgt.vendor_name = src.vendor_name AND tgt.seller_id = src.seller_id)
WHEN MATCHED THEN
    UPDATE SET
        seller_name = COALESCE(src.seller_name, tgt.seller_name),
        rating_count = COALESCE(src.rating_count, tgt.rating_count),
        last_checked_at = CASE
                            WHEN src.rating_count IS NOT NULL THEN SYSDATETIME()
                            ELSE tgt.last_checked_at
                          END
WHEN NOT MATCHED THEN
    INSERT (vendor_name, seller_id, seller_name, rating_count, is_ng, last_checked_at)
    VALUES (src.vendor_name, src.seller_id, src.seller_name, src.rating_count, 0,
            CASE WHEN src.rating_count IS NOT NULL THEN SYSDATETIME() ELSE NULL END);
"""

def upsert_mst_seller_from_rec(conn, vendor_name: str, rec: dict) -> None:
    seller_id = (rec.get("seller_id") or "").strip()
    seller_name = (rec.get("seller_name") or "").strip() or None
    rating_count = rec.get("rating_count")
    with conn.cursor() as cur:
        cur.execute(SQL_UPSERT_MST_SELLER, (vendor_name, seller_id, seller_name, rating_count))

def _truncate_for_db2(s: str, max_len: int = 200) -> str:
    if s is None:
        return ""
    s = str(s).replace("\r\n", "\n")
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."

def is_image_too_small_error(err_msg: str) -> bool:
    if not err_msg:
        return False

    return (
        "Picture Policy requirements" in err_msg
        and "500 pixels" in err_msg
    )

def build_pic_urls(
    *,
    rec: dict,
    sku: str,
    image_mode: str,              # "NORMAL" or "CDN"
    r2,
    r2_bucket: str,
    r2_public_base: str,
    cdn_cache: dict,              # {sku: [cdn_url,...]}
    limit: int = 12,
) -> list:
    """
    PicURL用のURLリストを作る。
    - NORMAL: mercariの画像URLをそのまま返す
    - CDN   : R2へuploadしてCDN URLを返す（SKU単位でキャッシュ）
    """
    src_urls = []
    for u in (rec.get("images") or []):
        if isinstance(u, str) and u.strip().startswith("http"):
            clean = u.strip().split("?")[0].split("#")[0]
            src_urls.append(clean)
        if len(src_urls) >= limit:
            break

    if image_mode == "NORMAL":
        return src_urls

    # CDN mode
    if sku in cdn_cache:
        return cdn_cache[sku]

    if not r2_bucket or not r2_public_base:
            print(f"[ERROR] CDN設定不足: Bucket={r2_bucket}, Base={r2_public_base}")
            return src_urls  # 設定が不完全ならメルカリの直URLを返して出品を試みる


    cdn_urls = []
    for idx, u in enumerate(src_urls):
        key = f"{sku}/{idx+1}.jpg"
        cdn_url = upload_image_to_r2(r2, r2_bucket, r2_public_base, u, key)
        cdn_urls.append(cdn_url)

    cdn_cache[sku] = cdn_urls
    return cdn_urls

def has_color_touchup_or_repair(
    jp_title: str,
    jp_description: str,
) -> tuple[bool, str]:
    """
    日本語タイトル・説明から、
    補色・修復・リカラー・改変が示唆されているかを GPT で判定する。

    戻り値:
      (is_ng, reason)
        - is_ng: True なら GA 的にアウト
        - reason: NG と判断した理由（短文）。OK の場合は空文字。
    """
    text = ((jp_title or "") + "\n" + (jp_description or "")).strip()
    if not text:
        return False, ""

    client = get_openai_client()

    prompt = f"""
You are checking whether an item violates eBay Authenticity Guarantee (GA) rules.

Determine if the following Japanese description indicates:
- color touch-up
- recoloring
- repair
- restoration
- modification

Rules:
- If any of the above is indicated → NOT GA safe
- If explicitly states "no repair" / "no recolor" → GA safe
- If unclear → treat as GA safe

Return ONLY valid JSON.
Do not include explanations, markdown, or extra text.

Japanese text:
{text}

JSON format:
{{
  "ng": true or false,
  "reason": "short explanation"
}}
""".strip()

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=prompt,
        temperature=0,
    )

    import json

    raw = (resp.output_text or "").strip()
    if not raw:
        # GPT が何も返さなかった → 判定不能＝GA OK 扱い
        return False, ""

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # JSON 壊れ → 判定不能＝GA OK 扱い
        return False, ""

    is_ng = bool(data.get("ng"))
    reason = data.get("reason") or ""

    return is_ng, reason


# =========================
# heavy_check_detail / post_to_ebay（あなたが貼った版のまま）
# =========================
def heavy_check_detail(
    conn,
    page,
    item_url,
    sku,
    preset,
    vendor_name,
    mode,
    default_brand_en,
    category_id_ebay,   # ★追加
    department,         # ★追加
    type_ebay,          # ★追加
    debug_unavailable_dump,
    writes_since_commit,
    low_jpy_target,   # ★追加
    high_jpy_target,   # ★追加
    driver 
):
    """
    ✅ ここでは「詳細解析」「NG判定」「翻訳生成」まで。
    ✅ 画像URLの最終決定（NORMAL/CDN）は post_to_ebay 側でやる（重要）
    """

    # === STEP 1: Playwright (API) で最速生存確認 ===
    try:
        # === 解析実行 (すべて Playwright 1回完結) ===
        if vendor_name == "メルカリshops":
            # ★ ShopsもPlaywright化！
            rec = parse_detail_shops(page, item_url, preset, vendor_name, driver)
        else:
            # 通常メルカリ
            rec = parse_detail_personal(page, item_url, preset, vendor_name)
 
        if not isinstance(rec, dict):
            raise Exception(f"解析失敗（データが空です）: SKU={sku}")
        rec["vendor_item_id"] = sku

    except MercariItemUnavailableError as e:
        status = e.state

        mark_vendor_item_unavailable(conn, vendor_name, sku, status)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

        return None, debug_unavailable_dump, writes_since_commit, 1, 0


    except Exception as e:
        if is_fatal_renderer_error(e):
            import traceback
            print("!!! [FATAL RENDERER ERROR] !!!", flush=True)
            print(f"Target URL: {item_url}", flush=True)
            print(f"Error Detail: {e}", flush=True)
            print(traceback.format_exc(), flush=True)
            print("[FATAL] renderer timeout detected → raising FatalRendererError", flush=True)
            raise FatalRendererError(str(e))

        rec_fail = {
            "vendor_name": vendor_name,
            "vendor_item_id": sku,

            "title_jp": None,
            "title_en": None,
            "description": None,
            "description_en": None,

            "price": None,
            "last_updated_str": None,
            "shipping_region": None,
            "shipping_days": None,
            "seller_id": None,

            "preset": preset,
            "vendor_page": None,
            "images": [],

            "listing_head": "解析失敗",
            "listing_detail": _truncate_for_db2(str(e), 200),
        }

        upsert_vendor_item(conn, rec_fail)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 0, 1

    # === 必須項目チェック ===
    if not (rec.get("description") or "").strip():
        rec["listing_head"] = "説明文なし"
        rec["listing_detail"] = "メルカリ商品説明が空"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    seller_id = (rec.get("seller_id") or "").strip()
    if not seller_id:
        rec["listing_head"] = "解析失敗"
        rec["listing_detail"] = "seller_idが空"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 0, 1

    upsert_mst_seller_from_rec(conn, vendor_name, rec)
    writes_since_commit += 1
    writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

    # === 1) 価格条件NG (最新価格でのレンジチェック) ===
    # スクレイピングした最新価格が、アカウントの担当レンジ外になっていないか確認
    current_cost = rec.get("price")
    if current_cost is None or current_cost < low_jpy_target or current_cost > high_jpy_target:
        rec["listing_head"] = "計算価格が範囲外"
        rec["listing_detail"] = f"最新価格:{current_cost} (Range:{low_jpy_target}-{high_jpy_target})"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0


    # === 2) 配送条件NG ===
    is_ng_page, has_info_page = _check_shipping_condition_values(
        rec.get("shipping_region"),
        rec.get("shipping_days"),
    )
    if has_info_page and is_ng_page:
        rec["listing_head"] = "配送条件NG"
        rec["listing_detail"] = "shipping_region/shipping_days(実ページ)判定"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 3) 古い更新（NG） ===
    vendor_updated_at = rec.get("vendor_updated_at")

    if vendor_updated_at is not None:
        if vendor_updated_at < datetime.now() - timedelta(days=40):
            rec["listing_head"] = "古い更新"
            rec["listing_detail"] = str(vendor_updated_at)
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
            return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 4.5) セラー判定 ===
    rating_count = rec.get("rating_count")
    threshold = 20 if vendor_name == "メルカリshops" else 50

    if rating_count is None:
        rec["listing_head"] = "解析失敗"
        rec["listing_detail"] = "rating_countが取得できない"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 0, 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT is_ng FROM mst.seller WHERE vendor_name = ? AND seller_id = ?",
            (vendor_name, seller_id),
        )
        row = cur.fetchone()

    if row and row[0] == 1:
        rec["listing_head"] = "NG(セラーNG)"
        rec["listing_detail"] = "mst.seller.is_ng = 1"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    if rating_count < threshold:
        rec["listing_head"] = "NG(セラー評価)"
        rec["listing_detail"] = f"rating_count={rating_count} < threshold={threshold}"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 4.9) 危険素材判定 ===
    jp_title = (rec.get("title_jp") or "").strip()
    desc_jp = (rec.get("description") or "").strip()

    if contains_risky_word(jp_title, desc_jp):
        rec["listing_head"] = "NG(危険素材)"
        rec["listing_detail"] = "エキゾチック/危険素材キーワード検出"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # =========================
    # GA 補色・修復チェック
    # =========================
    cost_jpy = rec["price"]
    start_price_usd = compute_start_price_usd(cost_jpy, mode)
    if not start_price_usd:
        # print(f"  └─ [NG] compute_start_price_usd が None を返しました")
        return None, debug_unavailable_dump, writes_since_commit, 1, 0
    price_decimal = Decimal(start_price_usd)
    # print(f"  └─ [INFO] 出品計算価格: {price_decimal} USD")

    # 実際にGA対象か？
    is_ga_actual = price_decimal >= Decimal("500.00")

    if is_ga_actual:

        is_ng, reason = has_color_touchup_or_repair(
            jp_title=jp_title,
            jp_description=desc_jp,
        )

        if is_ng:
            print(f"  └─ [NG] GA補色/修復判定により却下: {reason}")
            rec["listing_head"] = "NG(GA補色)"
            rec["listing_detail"] = reason or "Color touch-up / repair indicated"

            upsert_vendor_item(conn, rec)

            writes_since_commit += 1
            writes_since_commit = _maybe_commit(
                conn, writes_since_commit, BATCH_COMMIT
            )

            return None, debug_unavailable_dump, writes_since_commit, 1, 0   

    # === 6) 翻訳/整形（OKルート） ===
    existing_en = fetch_existing_title_en(conn, vendor_name, sku)
    if existing_en:
        rec["title_en"] = clean_for_ebay(existing_en)
    else:
        expected_brand_en = default_brand_en
        title_en_raw = translate_to_english(
            rec.get("title_jp") or "",
            rec.get("description") or "",
            expected_brand_en=expected_brand_en,
        ) or ""

        if not title_en_raw.strip():
            rec["listing_head"] = "翻訳空返し"
            rec["listing_detail"] = ""
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
            return None, debug_unavailable_dump, writes_since_commit, 0, 1

        title_en_post = postprocess_title(rec.get("title_jp") or "", rec.get("description") or "", title_en_raw)
        title_en = smart_truncate80(
            apply_title_rules_literal_ci(
                sanitize_title_dangerous_words(title_en_post),
                TITLE_RULES
            )
        )
        rec["title_en"] = clean_for_ebay(title_en)

    desc_jp = rec.get("description") or ""
    desc_en = ""
    if desc_jp:
        try:
            expected_brand_en = default_brand_en
            desc_en_raw = generate_ebay_description(
                rec.get("title_en") or "",
                desc_jp,
                expected_brand_en=expected_brand_en,
            )
            desc_en = clean_for_ebay(desc_en_raw)
        except Exception as e:
            print(f"[WARN] description_gen_failed SKU={sku}: {e}")

    if not desc_en:
        desc_en = (
            f"{rec.get('title_en') or ''}\n\n"
            "Please contact us via eBay messages for details.\n"
            "Ships from Japan with tracking."
        )
    rec["description_en"] = desc_en
    heavy = {
        "vendor_name": vendor_name,
        "sku": sku,
        "rec": rec,
        "start_price_usd": start_price_usd,
        "category_id_ebay": category_id_ebay,
        "default_brand_en": default_brand_en,
        "department": department,
        "type_ebay": type_ebay,
    }

    return heavy, debug_unavailable_dump, writes_since_commit, 0, 0

def post_to_ebay(
    *,
    conn,
    p,
    acct: str,
    heavy: dict,
    acct_targets,
    acct_success,
    acct_policies_map,
    total_listings,
    MAX_LISTINGS,
    stop_all,
    writes_since_commit,
    BATCH_COMMIT,
    # ★ state machine inputs/outputs
    image_mode: str,                # "NORMAL" or "CDN"
    image_error_count: int,
    cdn_mode_until,                 # datetime|None
    r2,
    r2_bucket: str,
    r2_public_base: str,
    cdn_cache: dict,
    now_dt: datetime,
):
    """
    - PicURLはここで組み立てる（NORMAL/CDNの分岐は main state に従う）
    - 画像500px未満系エラーが3回出たらCDNに切替
    - そのSKUはCDNで1回だけretry
    - CDNは20分で解除（解除判定はmain側でやる）
    """
    vendor_name = heavy["vendor_name"]
    sku = heavy["sku"]
    rec = heavy["rec"]
    start_price_usd = heavy["start_price_usd"]
    category_id_ebay = heavy["category_id_ebay"]
    default_brand_en = heavy["default_brand_en"]
    department = heavy["department"]
    type_ebay = heavy["type_ebay"]

    fail_other_delta = 0

    def _attempt_post(use_mode: str) -> str:
        pic_urls = build_pic_urls(
            rec=rec,
            sku=sku,
            image_mode=use_mode,
            r2=r2,
            r2_bucket=r2_bucket,
            r2_public_base=r2_public_base,
            cdn_cache=cdn_cache,
            limit=12,
        )

        payload = {
            "CustomLabel": sku,
            "*Title": rec["title_en"],
            "*StartPrice": start_price_usd,
            "*Quantity": 1,
            "PicURL": "|".join(pic_urls),
            "*Description": rec.get("description_en") or "",
            "category_id": category_id_ebay,
            "C:Brand": default_brand_en,
            "department": department,
            "C:Color": "Multicolor",
            "C:Type": type_ebay,
            "C:Country of Origin": "France",
        }

        return post_one_item(payload, acct, acct_policies_map[acct])

    # 1回目（現モード）
    try:
        item_id_ebay = _attempt_post(image_mode)

        if item_id_ebay:
            print(f"✅ 出品成功: acct={acct} SKU={sku} listing_id={item_id_ebay}")
            record_ebay_listing(item_id_ebay, acct, sku, vendor_name)

            rec["processing_by"] = None
            rec["processing_at"] = None

            rec["listing_head"] = "出品"
            rec["listing_detail"] = ""
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

            acct_success[acct] += 1
            if acct_targets[acct] is not None:
                acct_targets[acct] -= 1

            total_listings += 1
            if total_listings >= MAX_LISTINGS:
                stop_all = True

            return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                    fail_other_delta, image_mode, image_error_count, cdn_mode_until)

        # listing_id未返却
        rec["listing_head"] = "出品失敗"
        rec["listing_detail"] = "listing_id未返却"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        fail_other_delta += 1

        return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                fail_other_delta, image_mode, image_error_count, cdn_mode_until)

    except ListingLimitError as e:
        print(f"🚫 出品停止(ListingLimit): acct={acct} SKU={sku} reason={e}")

        rec["listing_head"] = "出品停止(ListingLimit)"
        rec["listing_detail"] = str(e)

        # ★ ロック解除（ここが追加）
        rec["processing_by"] = None
        rec["processing_at"] = None

        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, 1)

        fail_other_delta += 1
        acct_targets[acct] = 0

        return (
            acct_targets,
            acct_success,
            total_listings,
            stop_all,
            writes_since_commit,
            fail_other_delta,
            image_mode,
            image_error_count,
            cdn_mode_until
        )

    except ApiHandledError as e:
        err_msg = str(e) or ""
        print(f"❌ 出品失敗(API): acct={acct} SKU={sku} reason={err_msg}")

        # ★ 画像エラーを数える（NORMAL時のみ）
        switched_now = False
        if image_mode == "NORMAL" and is_image_too_small_error(err_msg):
            image_error_count += 1
            print(f"[IMG_ERR] count={image_error_count}/3 sku={sku}")

            if image_error_count >= 3:
                image_mode = "CDN"
                cdn_mode_until = now_dt + timedelta(minutes=20)
                switched_now = True
                print(f"[IMG_ERR] SWITCH -> CDN until {cdn_mode_until}")

        # ★ 3回到達したSKUは、その場でCDN retry（1回だけ）
        if switched_now:
            try:
                print(f"[IMG_ERR] retry with CDN: sku={sku}")
                item_id_ebay = _attempt_post("CDN")
                if item_id_ebay:
                    print(f"✅ 出品成功(CDN retry): acct={acct} SKU={sku} listing_id={item_id_ebay}")
                    record_ebay_listing(item_id_ebay, acct, sku, vendor_name)

                    rec["processing_by"] = None
                    rec["processing_at"] = None

                    rec["listing_head"] = "出品"
                    rec["listing_detail"] = ""
                    upsert_vendor_item(conn, rec)
                    writes_since_commit += 1
                    writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

                    acct_success[acct] += 1
                    if acct_targets[acct] is not None:
                        acct_targets[acct] -= 1

                    total_listings += 1
                    if total_listings >= MAX_LISTINGS:
                        stop_all = True

                    return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                            fail_other_delta, image_mode, image_error_count, cdn_mode_until)

                # retryでもlisting_id未返却
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = "CDN retry: listing_id未返却"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                fail_other_delta += 1

                return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                        fail_other_delta, image_mode, image_error_count, cdn_mode_until)

            except Exception as e2:
                print(f"[IMG_ERR] CDN retry failed: {e2}")
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = f"CDN retry failed: {e2}"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                fail_other_delta += 1

                return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                        fail_other_delta, image_mode, image_error_count, cdn_mode_until)

        # ★ 通常のAPI失敗確定
        rec["listing_head"] = "出品失敗"
        rec["listing_detail"] = err_msg
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        fail_other_delta += 1

        return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                fail_other_delta, image_mode, image_error_count, cdn_mode_until)

    except Exception as e:
        print(f"❌ 出品失敗(未分類): acct={acct} SKU={sku} reason={e}")
        rec["listing_head"] = "出品失敗(未分類)"
        rec["listing_detail"] = str(e)
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        fail_other_delta += 1

        return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                fail_other_delta, image_mode, image_error_count, cdn_mode_until)


def get_processing_by():
    return os.environ.get("WORKER_NAME", socket.gethostname())

def upload_image_to_r2(r2, bucket, public_base, image_url, key):
    import requests
    from botocore.exceptions import ClientError

    # 1. パスの正規化（スラッシュの重複を防止）
    # keyの先頭に / があるとバケット直下に空の名前のフォルダができる原因になるため除去
    clean_key = key.lstrip('/')
    # public_baseの末尾の / を除去
    base_url = public_base.rstrip('/')

    print(f"[R2 UPLOAD START] Target: {image_url} -> Key: {clean_key}")

    try:
        # 2. 画像のダウンロード
        res = requests.get(image_url, timeout=30)
        res.raise_for_status()
        
        # 3. R2へのアップロード
        # ACLはR2では基本的に不要（バケットポリシーで公開設定にするのが一般的）
        r2.put_object(
            Bucket=bucket,
            Key=clean_key,
            Body=res.content,
            ContentType="image/jpeg",
        )
        
        # 4. 返却URLの組み立て（必ず スラッシュ1つで結合）
        final_url = f"{base_url}/{clean_key}"
        print(f"[R2 SUCCESS] Public URL: {final_url}")
        return final_url

    except requests.exceptions.RequestException as e:
        print(f"[R2 ERROR] Failed to download image: {e}")
        return image_url  # 失敗時はオリジナルのURLを返してフォールバック
    except ClientError as e:
        print(f"[R2 ERROR] Boto3 Upload Failed: {e}")
        return image_url
    except Exception as e:
        print(f"[R2 ERROR] Unknown error: {e}")
        return image_url

@dataclass
class Account:
    account: str
    preset_group: str
    post_target: Optional[int]


def fetch_next_account_and_lock(conn, current_pc):
    """
    並行数が少ないアカウントから優先的に、かつアカウント順に1つ確保する
    """
    sql = """
    UPDATE TOP (1) mst.execute_pcs
    SET account = Target.account
    OUTPUT inserted.account, Target.preset_group, Target.post_target
    FROM mst.execute_pcs AS P
    CROSS APPLY (
        SELECT TOP 1 
            A.account, A.preset_group, A.post_target
        FROM mst.ebay_accounts A
        LEFT JOIN (
            SELECT account, COUNT(*) as active_workers
            FROM mst.execute_pcs
            WHERE account IS NOT NULL
            GROUP BY account
        ) W ON A.account = W.account
        CROSS APPLY (
            SELECT COUNT(*) as sent_count 
            FROM trx.listings 
            WHERE account = A.account 
              AND CAST(start_time AS DATE) = CAST(GETDATE() AS DATE)
              AND is_deleted = 0
        ) T
        WHERE A.is_excluded = 0
          AND A.is_closed_today = 0
          AND T.sent_count < A.post_target
          AND ISNULL(W.active_workers, 0) < ?  -- MAX_PARALLEL_PC
        ORDER BY 
            ISNULL(W.active_workers, 0) ASC, -- 1. 稼働中のWorkerが少ないアカウントを優先(分散)
            A.account ASC                    -- 2. アカウント名順(順繰り)
    ) AS Target
    WHERE P.execute_pc = ? AND P.is_active = 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (MAX_PARALLEL_PC, current_pc))
        row = cur.fetchone()
        conn.commit()
        if row:
            return Account(account=row[0].strip(), preset_group=row[1].strip(), post_target=row[2])
    return None

def release_pc_and_close_account(conn, current_pc, account_name=None, close_reason=None):
    """
    PCの占有を解除する。close_reasonがある場合はアカウント自体を当日終了とする。
    """
    with conn.cursor() as cur:
        # 1. アカウント自体の終了フラグ更新 (Limit検知や在庫切れ時)
        if account_name and close_reason:
            cur.execute("""
                UPDATE mst.ebay_accounts 
                SET is_closed_today = 1, close_reason = ? 
                WHERE account = ?
            """, (close_reason, account_name))
        
        # 2. PCの占有解除 (account を NULL に戻す)
        cur.execute("""
            UPDATE mst.execute_pcs 
            SET account = NULL 
            WHERE execute_pc = ?
        """, (current_pc,))
        
        conn.commit()


@dataclass
class PublishState:
    debug_unavailable_dump: dict
    writes_since_commit: int
    acct_targets: dict
    acct_success: dict
    acct_policies_map: dict
    total_listings: int
    stop_all: bool
    image_mode: str
    image_error_count: int
    cdn_mode_until: object
    cdn_cache: dict

def take_one_vendor_item(conn, preset_group, processing_by, account_name):
    """
    【在庫死蔵防止ロジック：物理テーブル3層結合版】
    1. v (商品) → pl (マスタ) でカテゴリを特定
    2. pl → r (レンジ) で、引数の preset_group に応じた担当範囲を特定
    3. 1段階の UPDATE で確保と全データ取得を同時に実行
    """

    # 1段階でロックとデータ取得を同時に行うSQL
    sql = r"""
        UPDATE TOP (1) v
        SET
            v.processing_by = ?,
            v.processing_at = SYSDATETIME()
        OUTPUT 
            inserted.vendor_item_id, 
            inserted.vendor_name, 
            inserted.price, 
            inserted.shipping_region, 
            inserted.shipping_days,
            inserted.preset, 
            pl.mode, 
            pl.default_brand_en, 
            pl.category_id_ebay, 
            pl.department,
            pl.type_ebay, 
            pl.category_group,
            r.low_jpy_target, 
            r.high_jpy_target,
            CAST(1 AS bit) AS is_ok_logic
        FROM trx.vendor_item v WITH (UPDLOCK, READPAST, ROWLOCK)
        INNER JOIN mst.presets_lookup pl ON pl.preset = v.preset
        INNER JOIN mst.presets_price_ranges r 
            ON r.preset_group = ? 
            AND r.category_group = pl.category_group
        WHERE
            v.processing_at IS NULL
            AND (v.status = N'販売中' OR v.status IS NULL)
            AND ISNULL(v.出品不可flg, 0) = 0
            
            AND (v.price IS NULL OR (v.price >= r.low_jpy_target AND v.price <= r.high_jpy_target))

            -- 基本的なNG条件の除外
            AND (
                v.vendor_updated_at IS NULL
                OR v.vendor_updated_at >= DATEADD(DAY, -40, SYSDATETIME())
            )
            AND ISNULL(v.[出品状況], N'') NOT IN (N'NG(GA補色)', N'NG(危険素材)')
            AND ISNULL(v.shipping_days, N'') NOT IN (
                N'4~7日で発送', N'4〜7日で発送', N'8〜14日で発送', N'90日以内で発送'
            )
            AND NOT EXISTS (
                SELECT 1 FROM trx.listings l
                WHERE l.vendor_name = v.vendor_name
                AND l.vendor_item_id = v.vendor_item_id
                AND l.is_deleted = 0
            )
        OPTION (MAXDOP 1);
        """

    while True:
        t_start = time.time()
        with conn.cursor() as cur:
            # 引数は (processing_by, preset_group) の2つ
            cur.execute(sql, (processing_by, preset_group))
            row = cur.fetchone()
            conn.commit()

            if row is None:
                # ここでNoneなら、そのグループの担当レンジに在庫がない（枯渇）
                print(f"  [DB_INFO] account={account_name} 担当範囲({preset_group})の在庫が枯渇しました。")
                return None

            elapsed = time.time() - t_start

            columns = [col[0] for col in cur.description]
            result = dict(zip(columns, row))
            
            sku = result.get("vendor_item_id")
            price = result.get("price")
            category_grp = result.get('category_group', '不明')
            low_target = result.get('low_jpy_target', 0)
            high_target = result.get('high_jpy_target', 0)

            print(f"[〇価格OK] account={account_name} SKU={sku} 価格={price} 価格range {low_target}～{high_target} {preset_group}-{category_grp} (Time: {elapsed:.3f}s)")
            return result

         
def main():
    # --- 修正ポイント1: .env の場所を絶対パスで指定 ---
    from pathlib import Path
    _PROJECT_ROOT = Path("/opt/apps_nostock") 
    env_path = _PROJECT_ROOT / ".env"
    
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path, override=True)

    current_pc = socket.gethostname().strip()
    processing_by = get_processing_by()
    start_time = datetime.now()
    
    # --- 修正ポイント2: .strip() で目に見えないゴミを削除 ---
    r2_endpoint    = os.getenv("R2_ENDPOINT", "").strip()
    r2_access_key  = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    r2_bucket_name = os.getenv("R2_BUCKET", "").strip()
    r2_public_base = os.getenv("R2_PUBLIC_BASE", "").strip()

    if r2_endpoint and r2_bucket_name and r2_endpoint.endswith("/" + r2_bucket_name):
        r2_endpoint = r2_endpoint.replace("/" + r2_bucket_name, "")

    from botocore.config import Config
    r2 = boto3.client(
        "s3",
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        region_name="auto",
        config=Config(signature_version='s3v4') 
    )

    R2_BUCKET = r2_bucket_name
    R2_PUBLIC_BASE = r2_public_base

    # ===== state machine =====
    image_mode = "NORMAL"
    image_error_count = 0
    cdn_mode_until = None
    cdn_cache = {}

    writes_since_commit = 0
    total_listings = 0
    MAX_LISTINGS = 10**9
    stop_all = False
    conn = get_sql_server_connection()
    summary_success = {}
    driver = None

    try:
        presets = fetch_active_presets(conn)
        driver = webdriver.Chrome() 

        with sync_playwright() as p:

            

            while not stop_all:
                acct = fetch_next_account_and_lock(conn, current_pc)
                if not acct:
                    print("[INFO] 実行可能なアカウントがありません。終了します。")
                    break

                print(f"🚀 アカウント開始: {acct.account} (Target: {acct.post_target})")

                acct_policies_map = {}
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT fulfillment_policy_id, payment_policy_id, return_policy_id
                        FROM mst.ebay_accounts WHERE account = ?
                    """, (acct.account,))
                    row = cur.fetchone()
                    if not row:
                        print(f"[ERROR] policy未設定のためスキップ: {acct.account}")
                        release_pc_and_close_account(conn, current_pc)
                        continue

                    acct_policies_map[acct.account] = {
                        "fulfillment_policy_id": str(row[0]),
                        "payment_policy_id": str(row[1]),
                        "return_policy_id": str(row[2]),
                        "merchant_location_key": "Default",
                    }

                acct_success = {acct.account: 0}
                acct_targets = {acct.account: acct.post_target}
                close_reason = None

                while not stop_all:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT COUNT(*) FROM trx.listings 
                            WHERE account = ? AND CAST(start_time AS DATE) = CAST(GETDATE() AS DATE)
                        """, (acct.account,))
                        sent_now = cur.fetchone()[0]

                    if sent_now >= acct.post_target:
                        print(f"✅ {acct.account} 当日目標数に達しました。")
                        break

                    if image_mode == "CDN" and cdn_mode_until and datetime.now() > cdn_mode_until:
                        print("[IMG_ERR] CDN timeout reached. Resetting to NORMAL.")
                        image_mode = "NORMAL"
                        image_error_count = 0
                        cdn_mode_until = None

                    row = take_one_vendor_item(conn, acct.preset_group, processing_by, acct.account)
                    if row:
                        print(f"[DEBUG] picked SKU={row['vendor_item_id']} price={row['price']} shipping_days={row['shipping_days']} 出品状況={row.get('出品状況')}")
                    if not row:
                        print(f"[INFO] {acct.account} 在庫枯渇")
                        close_reason = "EMPTY"
                        break

                    sku = row["vendor_item_id"].strip()
                    vendor_name = row["vendor_name"]
                    item_url = (
                        f"https://mercari-shops.com/products/{sku}"
                        if vendor_name == "メルカリshops"
                        else f"https://jp.mercari.com/item/{sku}"
                    )

                    browser = None
                    context = None
                    page = None

                    try:
                        browser = p.chromium.launch(headless=True)
                        context = browser.new_context(user_agent="...")
                        page = context.new_page()
                        page.add_init_script("""
                        (function() {
                            const origOpen = XMLHttpRequest.prototype.open;
                            const origSend = XMLHttpRequest.prototype.send;

                            XMLHttpRequest.prototype.open = function(method, url) {
                                this._url = url;
                                return origOpen.apply(this, arguments);
                            };

                            XMLHttpRequest.prototype.send = function() {
                                window.__MERCARI_DATA__ = null; 
                                this.addEventListener('load', function() {
                                    try {
                                        if (this._url && this._url.includes('api.mercari.jp/items/get')) {
                                            const json = JSON.parse(this.responseText);
                                            window.__MERCARI_DATA__ = json;
                                        }
                                    } catch (e) {}
                                });
                                return origSend.apply(this, arguments);
                            };
                        })();
                        """)
                        page.set_default_timeout(30000)

                        page.route(
                            "**/*",
                            lambda route: route.abort()
                            if route.request.resource_type in ["image", "media", "font"]
                            else route.continue_()
                        )

                        heavy, _, writes_since_commit, _, _ = heavy_check_detail(
                            conn, page, item_url, sku, row["preset"], vendor_name, row["mode"],
                            row["default_brand_en"], row["category_id_ebay"], row["department"], row["type_ebay"],
                            {}, writes_since_commit, row["low_jpy_target"], row["high_jpy_target"],driver
                        )

                        if heavy:
                            (
                                acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                                _, image_mode, image_error_count, cdn_mode_until,
                            ) = post_to_ebay(
                                conn=conn, p=None, acct=acct.account, heavy=heavy,
                                acct_targets=acct_targets, acct_success=acct_success,
                                acct_policies_map=acct_policies_map,
                                total_listings=total_listings, MAX_LISTINGS=MAX_LISTINGS,
                                stop_all=stop_all, writes_since_commit=writes_since_commit,
                                BATCH_COMMIT=BATCH_COMMIT, image_mode=image_mode,
                                image_error_count=image_error_count, cdn_mode_until=cdn_mode_until,
                                r2=r2, r2_bucket=r2_bucket_name, r2_public_base=r2_public_base,
                                cdn_cache=cdn_cache, now_dt=datetime.now(),
                            )

                            if acct_targets.get(acct.account) == 0:
                                print(f"🚫 {acct.account} APIリミットを検知しました。")
                                close_reason = "LIMIT"
                                break

                        if writes_since_commit > 0:
                            conn.commit()

                    except FatalRendererError:
                        print("[FATAL] Renderer crash → exit 1")
                        sys.exit(1)

                    except Exception as e:
                        print(f" [ERROR] SKU={sku} 処理中に例外発生: {e}")

                    finally:
                        try:
                            if page:
                                page.close()
                        except:
                            pass

                        try:
                            if context:
                                context.close()
                        except:
                            pass

                        try:
                            if browser:
                                browser.close()
                        except:
                            pass

                release_pc_and_close_account(conn, current_pc, acct.account, close_reason)
                summary_success[acct.account] = summary_success.get(acct.account, 0) + acct_success.get(acct.account, 0)
                print(f"🏁 アカウント終了: {acct.account} (Reason: {close_reason or 'DONE'})")

        end_time = datetime.now()
        elapsed = end_time - start_time
        lines = [f"{a}: 成功 {s}" for a, s in summary_success.items()]
        body = (
            f"PC: {current_pc}\n開始: {start_time}\n終了: {end_time}\n処理時間: {elapsed}\n\n"
            + "\n".join(lines)
        )
        send_mail("✅ eBay出品処理 完了通知", body)

        print(f"[EXIT] 処理完了（合計出品数: {total_listings}）")
        sys.exit(10)

    finally:
        try:
            release_pc_and_close_account(conn, current_pc)
        except:
            pass
        if driver:  
            driver.quit()        
        conn.close()

if __name__ == "__main__":
    print("--- Python Program Started ver ready対応  ---")
    try:
        main()
    except Exception as e:
        print(f"[UNHANDLED ERROR] {e}")
        sys.exit(1)