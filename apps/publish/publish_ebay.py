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
from PIL import Image
from io import BytesIO
import requests

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
from apps.adapters.mercari_item_status import fetch_shops_api_data,fetch_mercari_api_data,_parse_status_from_res,detect_status_from_mercari_shops
from datetime import datetime

# ========= 固定値／運用設定 =========
IMG_LIMIT     = 20
BATCH_COMMIT  = 10
MAX_PARALLEL_PC = 8  # 1アカウントあたりの最大同時稼働PC数
PIC_LIMIT=20

# ========= NG打刻・スキップ関連定義 =========
NG_HEADS_FOR_TIMESTAMP: Set[str] = {
    "古い更新",
    "計算価格が範囲外",
}

HEADS_FOR_7DAY_SKIP: Set[str] = {
    "古い更新",
    "計算価格が範囲外",
}

# ========= トレカ（鑑定済みカード）Condition 関連定義 =========
# eBay get_item_condition_policies（categoryId=183454）で確認した実際の conditionDescriptorValueId。
# 鑑定機関の略称 → conditionDescriptorValues[].conditionDescriptorValueId (descriptor 27501: Professional Grader)
CARD_GRADER_VALUE_IDS: Dict[str, str] = {
    "PSA": "275010",
    "BCCG": "275011",
    "BVG": "275012",
    "BGS": "275013",
    "CGC": "275015",
    "SGC": "275016",
    "KSA": "275017",
    "GMA": "275018",
    "HGA": "275019",
    "ISA": "2750110",
    "PCA": "2750111",
    "GSG": "2750112",
    "PGS": "2750113",
    "MNT": "2750114",
    "TAG": "2750115",
    "RCG": "2750117",
    "PCG": "2750118",
    "ACE": "2750119",
    "CGA": "2750120",
    "TCG": "2750121",
    "ARK": "2750122",
    "AGS": "2750124",
    "DSG": "2750125",
    "GRAAD": "2750127",
}

# ========= トレカ: eBay "Game" aspect 値マッピング =========
# default_brand_en (mst.presets_lookup) → eBay aspects["Game"] の有効値
CARD_BRAND_TO_GAME: Dict[str, str] = {
    "Pokemon":    "Pokémon TCG",
    "Pokémon":   "Pokémon TCG",
    "Yu-Gi-Oh!": "Yu-Gi-Oh! TCG",
    "Konami":    "Yu-Gi-Oh! TCG",
}

# ========= 危険素材（NG素材）判定 対象カテゴリグループ定義 =========
# 財布・財布メンズのみ判定対象。それ以外（デジカメ/ペン/スカーフ/スカーフメンズ等）はスキップ。
RISKY_MATERIAL_TARGET_CATEGORY_GROUPS: Set[str] = {
    "財布",
    "財布メンズ",
}

# グレード数値 → conditionDescriptorValueId (descriptor 27502: Grade)
CARD_GRADE_VALUE_IDS: Dict[str, str] = {
    "10": "275020",
    "9.5": "275021",
    "9": "275022",
    "8.5": "275023",
    "8": "275024",
    "7.5": "275025",
    "7": "275026",
    "6.5": "275027",
    "6": "275028",
    "5.5": "275029",
    "5": "2750210",
    "4.5": "2750211",
    "4": "2750212",
    "3.5": "2750213",
    "3": "2750214",
    "2.5": "2750215",
    "2": "2750216",
    "1.5": "2750217",
    "1": "2750218",
}

_CARD_GRADE_TEXT_RE = re.compile(r"^([A-Za-z]{2,6})\s*([0-9]+(?:\.[0-9])?)$")

def parse_card_grade_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    「グレード」属性の values[0].text（例: "PSA10" / "BGS 9.5"）から
    鑑定機関の略称とグレード数値を抽出する。解析できない場合は (None, None)。
    """
    s = (text or "").strip().upper()
    m = _CARD_GRADE_TEXT_RE.match(s)
    if not m:
        return None, None
    return m.group(1), m.group(2)

def build_card_condition_fields(item_attributes: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    トレカの item_attributes から *ConditionID と conditionDescriptors を組み立てる。

    - 「グレード」属性が存在しない → 未鑑定（4000 / Card Condition = Lightly Played (Excellent) 固定）
    - 「グレード」属性が存在し、values[0].text が "PSA10" 等の形式で解析できる
        → 鑑定済み（2750 / Professional Grader + Grade）
    - 「グレード」属性が存在するが解析できない（未知の鑑定機関表記など）
        → タイトル/説明文は見ずに安全側に倒し、未鑑定（4000）として扱う
    """
    grade_attr = next((a for a in (item_attributes or []) if a.get("text") == "グレード"), None)

    if grade_attr:
        values = grade_attr.get("values") or []
        grade_text = (values[0].get("text") if values else "") or ""
        grader_abbr, grade_num = parse_card_grade_text(grade_text)
        grader_value_id = CARD_GRADER_VALUE_IDS.get(grader_abbr or "")
        grade_value_id = CARD_GRADE_VALUE_IDS.get(grade_num or "")

        if grader_value_id and grade_value_id:
            return "2750", [
                {"name": "27501", "values": [grader_value_id]},
                {"name": "27502", "values": [grade_value_id]},
            ]

        print(f"[WARN] トレカ グレード解析失敗: text={grade_text!r} → 未鑑定として処理")

    return "4000", [{"name": "40001", "values": ["400015"]}]

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
    print("[DEBUG] before goto (shops)", datetime.now().strftime("%H:%M:%S"))

    page.goto(url, wait_until="domcontentloaded", timeout=20000)
    print("[DEBUG] waiting API...", datetime.now().strftime("%H:%M:%S"))
    res = fetch_shops_api_data(page, url)[0]
    print("[DEBUG] API取得完了", datetime.now().strftime("%H:%M:%S"))
    
    # ❌ ここで削除判定しない（Seleniumに任せる）
    if res is None:
        raise Exception("API取得失敗")  # ← retry対象

    detail = res.get("productDetail", {})
    shop = detail.get("shop", {})
    variants = res.get("variants", [])

    update_time_str = res.get("updateTime")
    vendor_updated_at = datetime.fromisoformat(update_time_str.replace("Z", "+00:00")) if update_time_str else None

    create_time_str = res.get("createTime")
    vendor_created_at = datetime.fromisoformat(create_time_str.replace("Z", "+00:00")) if create_time_str else None

    if variants:
        return {
            "vendor_name": vendor_name,
            "title_jp": res.get("displayName"),
            "title_en": "",
            "price": int(res.get("price", 0)),
            "vendor_created_at": vendor_created_at,
            "vendor_updated_at": vendor_updated_at,
            "shipping_region": detail.get("shippingFromArea", {}).get("displayName", ""),
            "shipping_days": detail.get("shippingDuration", {}).get("displayName", ""),
            "seller_id": shop.get("name", ""),
            "seller_name": shop.get("displayName", ""),
            "rating_count": int(shop.get("shopStats", {}).get("reviewCount", 0)),
            "num_likes": int((detail.get("productStats") or {}).get("likesCount", 0)),
            "images": [],
            "preset": preset,
            "description": detail.get("description", ""),
            "description_en": "",
            "item_attributes": detail.get("item_attributes", []),
            "is_variant": True,
            "variants": variants,
        }

    raw_images = detail.get("photos", []) or []

    filtered_images = []
    for img_url in raw_images:
        try:
            res_img = requests.get(img_url, timeout=10)
            res_img.raise_for_status()

            img = Image.open(BytesIO(res_img.content))
            width, height = img.size

            if width < 500 or height < 500:
                continue

            filtered_images.append(img_url)

        except Exception as e:
            print(f"[IMAGE ERROR] {img_url} {e}")
            continue

    if not filtered_images:
        raise Exception("no valid images (all <500px or error)")

    return {
        "vendor_name": vendor_name,
        "title_jp": res.get("displayName"),
        "title_en": "",
        "price": int(res.get("price", 0)),  # ← JSON優先
        "vendor_created_at": vendor_created_at,
        "vendor_updated_at": vendor_updated_at,
        "shipping_region": detail.get("shippingFromArea", {}).get("displayName", ""),
        "shipping_days": detail.get("shippingDuration", {}).get("displayName", ""),
        "seller_id": shop.get("name", ""),
        "seller_name": shop.get("displayName", ""),
        "rating_count": int(shop.get("shopStats", {}).get("reviewCount", 0)),
        "num_likes": int((detail.get("productStats") or {}).get("likesCount", 0)),
        "images": filtered_images,
        "preset": preset,
        "description": detail.get("description", ""),
        "description_en": "",
        "item_attributes": detail.get("item_attributes", []),
        "is_variant": False,
        "variants": [],
    }


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

    created = item.get("created")
    vendor_created_at = None
    if created:
        vendor_created_at = datetime.fromtimestamp(created)

    raw_images = item.get("photos", []) or []

    filtered_images = []
    for img_url in raw_images:
        try:
            res_img = requests.get(img_url, timeout=10)
            res_img.raise_for_status()

            img = Image.open(BytesIO(res_img.content))
            width, height = img.size

            if width < 500 or height < 500:
                continue

            filtered_images.append(img_url)

        except Exception as e:
            print(f"[IMAGE ERROR] {img_url} {e}")
            continue

    if not filtered_images:
        raise Exception("no valid images (all <500px or error)")


    # 4. rec の組み立て
    return {
        "vendor_name": vendor_name,
        "title_jp": item.get("name"),
        "title_en": "",
        "price": int(item.get("price", 0)),
        "vendor_created_at": vendor_created_at,
        "vendor_updated_at": vendor_updated_at,   #
        "shipping_region": item.get("shipping_from_area", {}).get("name", ""),
        "shipping_days": item.get("shipping_duration", {}).get("name", ""),
        "seller_id": str(item.get("seller", {}).get("id", "")),
        "seller_name": item.get("seller", {}).get("name", ""),
        "rating_count": int(item.get("seller", {}).get("num_ratings", 0)),
        "num_likes": int(item.get("num_likes", 0)),
        "images": filtered_images,
        "preset": preset,
        "description": item.get("description", ""),
        "description_en": "",
        "item_attributes": item.get("item_attributes", []),
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
    VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
) AS src (
    vendor_name, vendor_item_id,
    title_jp, title_en,
    description, description_en,
    price,
    vendor_created_at,
    vendor_updated_at,
    last_updated_str,
    shipping_region, shipping_days,
    seller_id, num_likes,
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
        num_likes        = src.num_likes,
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
        vendor_created_at = COALESCE(tgt.vendor_created_at, src.vendor_created_at),
        vendor_updated_at = COALESCE(src.vendor_updated_at, tgt.vendor_updated_at),
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
        price,vendor_created_at,vendor_updated_at,
        last_updated_str, shipping_region, shipping_days, seller_id, num_likes,
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
        src.vendor_created_at,
        src.vendor_updated_at,
        src.last_updated_str,
        src.shipping_region,
        src.shipping_days,
        src.seller_id,
        src.num_likes,
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

    vendor_created_at = rec.get("vendor_created_at")
    vendor_updated_at = rec.get("vendor_updated_at")

    price_val = rec.get("price")
    if price_val is not None:
        try:
            price_val = int(price_val)
        except Exception:
            price_val = None

    num_likes = rec.get("num_likes")
    try:
        num_likes = int(num_likes)
    except Exception:
        num_likes = 0


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
        vendor_created_at,
        vendor_updated_at,

        last_updated_str,
        shipping_region,
        shipping_days,
        seller_id,
        num_likes,

        preset_val,
        vendor_page,

        *imgs,

        listing_head,
        listing_detail,
    )

    with conn.cursor() as cur:
        cur.execute(UPSERT_VENDOR_ITEM_SQL, params)
        _ = cur.fetchall()




def record_ebay_listing(listing_id: str, account_name: str, vendor_item_id: str, vendor_name: str, start_price: float):
    if not listing_id:
        return

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
INSERT INTO [trx].[listings]
    ([listing_id], [start_time], [account], [vendor_item_id], [vendor_name], [start_price], [is_deleted])
VALUES
    (?, SYSDATETIME(), ?, ?, ?, ?, 0);
""", (listing_id, account_name, vendor_item_id, vendor_name, start_price))
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

    # bad_days = {"8〜14日で発送", "4〜7日で発送", "4~7日で発送","90日以内で発送"}
    bad_days = {"8〜14日で発送", "90日以内で発送"}

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


def has_junk_or_defective_condition(
    jp_title: str,
    jp_description: str,
) -> tuple[bool, str]:
    """
    日本語タイトル・説明から、ジャンク品・動作未確認・現状品など
    正常使用できない可能性が高い商品かを GPT で判定する。
    デジカメカテゴリ専用。

    戻り値:
      (is_ng, reason)
        - is_ng: True なら出品除外
        - reason: NG と判断した理由（短文）。OK の場合は空文字。
    """
    text = ((jp_title or "") + "\n" + (jp_description or "")).strip()
    if not text:
        return False, ""

    client = get_openai_client()

    prompt = f"""
You are checking whether a used Japanese item listing describes a defective, junk, or non-functional product.

Determine if the following Japanese text indicates ANY of these conditions:
- Junk item (ジャンク品)
- Operation unconfirmed (動作未確認)
- Powers on only (通電のみ確認)
- As-is / current condition only (現状品, 現状渡し)
- For parts (部品取り)
- Requires repair (修理前提)
- No operation guarantee (動作保証なし)
- Broken or damaged functionality

Important rules:
- "ジャンクではありません" / "ジャンク品ではない" → NOT defective (safe to list)
- "動作確認済み" / "動作問題なし" / "動作良好" → NOT defective (safe to list)
- If the text only describes cosmetic flaws (scratches, dirt) but NOT functional issues → safe to list
- If unclear or ambiguous → treat as safe (return ng: false)

Return ONLY valid JSON. No explanation, no markdown.

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
        return False, ""

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
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
    category_group,
    debug_unavailable_dump,
    writes_since_commit,
    low_jpy_target,   # ★追加
    high_jpy_target,   # ★追加
    driver, 
    item_condition_id   # ← これを追加
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
        rec["item_condition_id"] = item_condition_id 
        
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

    # === バリエーション商品NG ===
    if rec.get("is_variant"):
        variants = rec.get("variants", [])
        rec["listing_head"] = "NG(バリエーション商品)"
        rec["listing_detail"] = f"variants={len(variants)}"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

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
        target_dt = vendor_updated_at.replace(tzinfo=None) 
        days_old = (datetime.now() - target_dt).days
        if target_dt < datetime.now() - timedelta(days=40):
            rec["listing_head"] = "古い更新"
            rec["listing_detail"] = f"{days_old}日前"
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
            return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 3.5) 低需要NG（30日経過 & いいね<=1） ===
    vendor_created_at = rec.get("vendor_created_at")
    num_likes = rec.get("num_likes", 0)

    if vendor_created_at is not None:
        target_dt = vendor_created_at.replace(tzinfo=None)
        if target_dt < datetime.now() - timedelta(days=30) and num_likes <= 1:
            rec["listing_head"] = "低需要NG"
            rec["listing_detail"] = f"created_at={vendor_created_at}, likes={num_likes}"

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
            "SELECT is_ng, allow_new_items FROM mst.seller WHERE vendor_name = ? AND seller_id = ?",
            (vendor_name, seller_id),
        )
        row = cur.fetchone()
    allow_new_items = row[1] if row else 0

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

    # === 4.6) 新品フィルタ ===

    # ① condition未取得 → NG
    item_condition_id = rec.get("item_condition_id")

    if item_condition_id is None:
        rec["listing_head"] = "NG(状態未取得)"
        rec["listing_detail"] = "item_condition_id is NULL"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 4.6) 高リスク商品フィルタ ===
    # 高リスク商品 → 信頼セラーのみ
    is_high_risk = False

    # 新品
    if item_condition_id == 1 and category_group != "ペン" and category_group != "製図用品":
        is_high_risk = True

    # トレカ
    if category_group == "トレカ":
        is_high_risk = True
    
    # デジカメ
    if category_group in ("デジカメ", "ビデオカメラ","レンズ"):
        is_high_risk = True    

    # ヴィトン・シャネル中古
    if (
        item_condition_id != 1
        and default_brand_en in ("Louis Vuitton", "CHANEL")
    ):
        is_high_risk = True

    if is_high_risk:

        print(
            "[HIGH_RISK_CHECK]",
            f"seller_id={seller_id}",
            f"rating_count={rating_count}",
            f"type={type(rating_count)}",
            f"allow_new_items={allow_new_items}",
            f"default_brand_en={default_brand_en}",
            f"item_condition_id={item_condition_id}",
        )



        allow_high_risk = False

        # DB許可済み
        if allow_new_items == 1:
            allow_high_risk = True

        # 高評価セラー
        if rating_count >= 500:
            allow_high_risk = True

        if not allow_high_risk:
            rec["listing_head"] = "NG(高リスク商品制限)"
            rec["listing_detail"] = (
                f"allow_new_items != 1 and rating_count={rating_count} < 500"
            )

            upsert_vendor_item(conn, rec)

            writes_since_commit += 1
            writes_since_commit = _maybe_commit(
                conn,
                writes_since_commit,
                BATCH_COMMIT
            )

            return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 4.9) 危険素材判定（財布・財布メンズのみ対象） ===
    jp_title = (rec.get("title_jp") or "").strip()
    desc_jp = (rec.get("description") or "").strip()

    if (
        category_group in RISKY_MATERIAL_TARGET_CATEGORY_GROUPS
        and contains_risky_word(jp_title, desc_jp)
    ):
        rec["listing_head"] = "NG(危険素材)"
        rec["listing_detail"] = "エキゾチック/危険素材キーワード検出"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 5) ジャンク品・動作未確認判定（デジカメ・腕時計） ===
    if category_group in ("デジカメ", "腕時計", "ビデオカメラ","レンズ"):
        is_junk, junk_reason = has_junk_or_defective_condition(
            jp_title=jp_title,
            jp_description=desc_jp,
        )
        if is_junk:
            print(f"  └─ [NG] ジャンク疑い判定により却下: {junk_reason}")
            rec["listing_head"] = "NG(ジャンク疑い)"
            rec["listing_detail"] = junk_reason or "Junk / defective / no operation guarantee"
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
    is_ga_actual = (
        mode == "GA"
        and price_decimal >= Decimal("500.00")
    )

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

    # カメラモデル取得 (デジカメのみ)
    camera_models = None
    if category_group in ("デジカメ", "ビデオカメラ","レンズ"):
        camera_models = extract_camera_model(
            rec.get("title_jp") or "",
            rec.get("description") or "",
        )
        _mjp = (camera_models or {}).get("model_jp", "")
        _mus = (camera_models or {}).get("model_us", "")
        if _mjp and _mus and _mjp != _mus:
            print("######################################################################")
            print("# 🌎 CAMERA MODEL CANDIDATE")
            print(f"# SKU       : {sku}")
            print(f"# JP MODEL  : {_mjp}")
            print(f"# US MODEL  : {_mus}")
            print("######################################################################")

    # レンズ（ズーム）カテゴリ専用: eBay Item Specifics(AI)取得＋不足時Skip
    # category_id_ebay == "3323"(ズーム)のみ対象。単焦点(3223)は対象外。
    lens_specifics = None
    if category_id_ebay == "3323":
        lens_specifics = extract_lens_item_specifics(
            rec.get("title_jp") or "",
            rec.get("description") or "",
            default_brand_en or "",
        )
        missing_keys = [
            k for k in ("Maximum Aperture", "Focal Length", "Mount")
            if not lens_specifics.get(k)
        ]
        if missing_keys:
            print(f"🚫 レンズItem Specifics不足のためSkip: SKU={sku} missing={missing_keys}")
            rec["listing_head"] = "NG(レンズ情報不足)"
            rec["listing_detail"] = f"missing={','.join(missing_keys)}"
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
            return None, debug_unavailable_dump, writes_since_commit, 1, 0

        # Focus TypeはAIを使わずMountからルールベースで決定
        lens_specifics["Focus Type"] = determine_lens_focus_type(lens_specifics.get("Mount"))

    _model_jp_desc = (camera_models or {}).get("model_jp", "")
    _model_us_desc = (camera_models or {}).get("model_us", "")

    desc_jp = rec.get("description") or ""
    desc_en = ""
    if desc_jp:
        try:
            expected_brand_en = default_brand_en
            desc_en_raw = generate_ebay_description(
                rec.get("title_en") or "",
                desc_jp,
                expected_brand_en=expected_brand_en,
                model_jp=_model_jp_desc,
                model_us=_model_us_desc,
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
        "category_group": category_group,
        "camera_models": camera_models,
        "lens_specifics": lens_specifics,
    }

    return heavy, debug_unavailable_dump, writes_since_commit, 0, 0

def build_pic_urls(
    *,
    rec: dict,
    sku: str,
    image_mode: str,              # "NORMAL" or "CDN"
    r2,
    r2_bucket: str,
    r2_public_base: str,
    cdn_cache: dict,              # {sku: [cdn_url,...]}
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
        if len(src_urls) >= PIC_LIMIT:
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

def classify_ebay_error(err_msg: str):
    if "Cannot revise listing" in err_msg or "cannot contain javascript" in err_msg.lower():
        return "ポリシーNG"
    return None

def extract_camera_model(title_jp: str, description_jp: str) -> dict:
    """
    デジタルカメラの商品名・説明から日本型番と国際型番を返す。
    Returns: {"model_jp": "...", "model_us": "..."}
    """
    try:
        import json as _json
        client = get_openai_client()

        prompt = f"""
あなたは中古カメラの専門家です。

商品タイトルと商品説明から、eBay Item Specific の Model を抽出してください。

以下の2つを返してください：
- model_jp: 日本市場での型番（例: IXY 130, EOS Kiss X7）
- model_us: US/国際市場での対応型番（例: PowerShot ELPH 140 IS, EOS Rebel SL1）

ルール:
- ブランド名は含めない
- 色は含めない
- レンズキット等の販売形態は含めない
- 付属品は含めない
- 日本型番と国際型番が同じ場合（例: PowerShot SX710 HS）は両方に同じ値を入れる
- 国際型番が不明な場合は model_jp と同じ値を入れる
- 回答はJSON形式のみ（説明文不要）

商品タイトル:
{title_jp}

商品説明:
{description_jp}

JSON形式で回答:
{{"model_jp": "...", "model_us": "..."}}
"""

        response = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            temperature=0
        )

        raw = (response.output_text or "").strip()
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        data = _json.loads(raw)
        model_jp = (data.get("model_jp") or "").strip()
        model_us = (data.get("model_us") or "").strip()
        return {"model_jp": model_jp, "model_us": model_us or model_jp}

    except Exception as e:
        print(f"extract_camera_model failed: {e}")
        return {"model_jp": "", "model_us": ""}

LENS_ITEM_SPECIFICS_KEYS = [
    "Maximum Aperture",
    "Focal Length",
    "Mount",
    "Focus Type",
]

def extract_lens_item_specifics(title: str, description: str, brand: str) -> dict:
    """
    【レンズカテゴリ専用】title/description/brand から eBay Item Specifics候補をAIで推測する。
    - XML生成へは未接続（呼び出し元は別途実装する）。
    - キー名は将来そのままXMLへ渡せるよう eBay Item Specifics の名称に合わせている。
    """
    try:
        import json as _json
        client = get_openai_client()

        prompt = f"""
あなたは中古カメラレンズの専門家です。

以下の商品タイトル・商品説明・ブランド名から、eBay Item Specifics の値を推測してください。

出力は必ず次のJSON形式のみで返してください（説明文・コードブロック不要）:
{{"Maximum Aperture": "", "Focal Length": "", "Mount": "", "Focus Type": ""}}

各項目の意味と回答形式:

- Maximum Aperture（開放F値）
  例: f/1.4, f/2.8, f/3.5-5.6

- Focal Length（焦点距離）
  例: 50mm, 24-70mm

- Mount（マウント規格）
  例: Canon EF, Canon RF, Nikon F, Nikon Z, Sony E, Sony A, Micro Four Thirds, Leica M

- Focus Type（フォーカス方式。以下のいずれかのみを返す）
  Auto
  Manual
  Auto & Manual

判定ルール:
- 商品タイトルの情報を最優先で使うこと
- 商品説明はタイトルの情報を補完する目的でのみ使うこと
- ブランドや型番から一般的に明らかな仕様（例: 型番から判明するマウントや開放F値）のみ推測してよい
- 曖昧な推測や憶測はせず、判断できない項目は null を返すこと
- JSON以外は一切出力しないこと

商品タイトル:
{title}

商品説明:
{description}

ブランド:
{brand}
"""

        response = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            temperature=0
        )

        raw = (response.output_text or "").strip()
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        data = _json.loads(raw)

        return {key: (data.get(key) or None) for key in LENS_ITEM_SPECIFICS_KEYS}

    except Exception as e:
        print(f"extract_lens_item_specifics failed: {e}")
        return {key: None for key in LENS_ITEM_SPECIFICS_KEYS}

# レガシーな完全マニュアルフォーカス専用マウント（AF機構が存在しない）
_MANUAL_FOCUS_MOUNT_KEYWORDS = (
    "fd",            # Canon FD / New FD
    "leica m",       # Leica M（レンジファインダー）
    "m42",           # M42スクリューマウント
    "exakta",
    "t-mount", "t mount",
    "bronica",       # Zenza Bronica（中判）
    "contax/yashica", "c/y mount",
)

def determine_lens_focus_type(mount: str) -> str:
    """
    Mount名からFocus TypeをAIを使わずルールベースで決定する。
    レガシーな完全マニュアルマウントのみ"Manual"、それ以外は"Auto"とする。
    """
    m = (mount or "").lower()
    if any(kw in m for kw in _MANUAL_FOCUS_MOUNT_KEYWORDS):
        return "Manual"
    return "Auto"

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
    preset,
    is_collectibles=None,
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
    category_group = heavy["category_group"]

    fail_other_delta = 0

    # acct_targets とは役割を分離する専用フラグ。
    # 「eBayのListingLimitErrorが実際に発生したか」だけを表す。
    # acct_targets は残り投稿件数のカウントダウン専用とし、LIMIT判定には使わない。
    listing_limit_hit = False

    _camera_models = heavy.get("camera_models")
    _lens_specifics = heavy.get("lens_specifics")

    def _attempt_post(use_mode: str, model_name: str = None) -> str:
        pic_urls = build_pic_urls(
            rec=rec,
            sku=sku,
            image_mode=use_mode,
            r2=r2,
            r2_bucket=r2_bucket,
            r2_public_base=r2_public_base,
            cdn_cache=cdn_cache,
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
        
        # [修正ポイント] payload["itemSpecifics"] を作らず、直接 payload に C: 形式で入れる
        if category_group == "ペン":
            payload["C:Ink Color"] = ["Black"]
            payload["C:Material"] = "Steel"

        if category_group == "アクセサリー":
            payload["C:Style"] = type_ebay

        if category_group in ("デジカメ", "ビデオカメラ","レンズ") and model_name:
            payload["C:Model"] = model_name

        if category_id_ebay == "3323" and _lens_specifics:
            payload["C:Maximum Aperture"] = _lens_specifics["Maximum Aperture"]
            payload["C:Focal Length"] = _lens_specifics["Focal Length"]
            payload["C:Mount"] = _lens_specifics["Mount"]
            payload["C:Focus Type"] = _lens_specifics["Focus Type"]

        if category_group in ("トレカ", "遊戯王カード"):
            condition_id, condition_descriptors = build_card_condition_fields(rec.get("item_attributes"))
            payload["*ConditionID"] = condition_id
            payload["conditionDescriptors"] = condition_descriptors

            if category_group == "遊戯王カード":
                payload["C:Game"] = "Yu-Gi-Oh!"
            else:
                payload["C:Game"] = "Pokémon TCG"

            print(f"[INFO] C:Game={payload['C:Game']!r} (category_group={category_group!r})")

        return post_one_item(payload, acct, acct_policies_map[acct])

    # 1回目（現モード）
    _model_jp = (_camera_models or {}).get("model_jp") or None
    _model_us = (_camera_models or {}).get("model_us") or None

    try:
        item_id_ebay = _attempt_post(image_mode, model_name=_model_jp)

        if item_id_ebay:
            print(f"✅ 出品成功: acct={acct} SKU={sku} listing_id={item_id_ebay}")
            record_ebay_listing(item_id_ebay, acct, sku, vendor_name, start_price_usd)

            with conn.cursor() as cursor:
                if is_collectibles:
                    cursor.execute("""
                        UPDATE mst.ebay_accounts
                        SET additional_listing_left = additional_listing_left - 1
                        WHERE account = ?
                    """, acct)
                else:
                    cursor.execute("""
                        UPDATE mst.ebay_accounts
                        SET listing_left = listing_left - 1
                        WHERE account = ?
                    """, acct)

                # 投稿成功直後、その場で当日COUNTを再取得しtarget到達済みならDONEを確定する。
                # close_reasonがLIMIT中でも上書き対象に含めるのは、このworkerの投稿が
                # 確定する前に別workerが実際のeBay制限でLIMITを書いていた場合でも、
                # target到達という事実が判明した時点で必ずDONEへ昇格させるため。
                cursor.execute("""
                    UPDATE mst.ebay_accounts
                    SET close_reason = 'DONE'
                    WHERE account = ?
                      AND (close_reason IS NULL OR close_reason = 'LIMIT')
                      AND (
                          SELECT COUNT(*) FROM trx.listings
                          WHERE account = ?
                            AND CAST(start_time AS DATE) = CAST(GETDATE() AS DATE)
                            AND is_deleted = 0
                      ) >= (SELECT post_target FROM mst.ebay_accounts WHERE account = ?)
                """, (acct, acct, acct))

            conn.commit()

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
                # [publish_ebay 全体の終了①] 全アカウント合計の出品数が上限(MAX_LISTINGS)に到達。
                # stop_all=True は内側・外側どちらの while ループも同時に止める
                # ＝このアカウントだけでなくプロセス全体を終了させる合図。
                # 現状 MAX_LISTINGS = 10**9 のため、実運用では事実上到達しない。
                stop_all = True

            return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                    fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)

        # listing_id未返却
        rec["listing_head"] = "出品失敗"
        rec["listing_detail"] = "listing_id未返却"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        fail_other_delta += 1

        return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)

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

        # eBay APIが実際にListingLimitErrorを返した唯一の箇所。
        # ここでだけ listing_limit_hit=True にする（acct_targetsはもう触らない）。
        # acct_targets は「残り投稿件数」の表示専用として、実際の値のまま残す。
        listing_limit_hit = True

        return (
            acct_targets,
            acct_success,
            total_listings,
            stop_all,
            writes_since_commit,
            fail_other_delta,
            image_mode,
            image_error_count,
            cdn_mode_until,
            listing_limit_hit,
        )

    except ApiHandledError as e:
        err_msg = str(e) or ""
        print(f"❌ 出品失敗(API): acct={acct} SKU={sku} reason={err_msg}")

        # ── 25604: Product not found → model_us でリトライ ────────
        if e.code == 25604 and _model_us and _model_us != _model_jp:
            print("######################################################################")
            print("# 🚨 CAMERA RETRY 25604 🚨")
            print(f"# SKU       : {sku}")
            print(f"# JP MODEL  : {_model_jp}")
            print(f"# US MODEL  : {_model_us}")
            print("######################################################################")
            try:
                item_id_ebay = _attempt_post(image_mode, model_name=_model_us)
                if item_id_ebay:
                    print(f"✅ 出品成功(model_us retry): acct={acct} SKU={sku} listing_id={item_id_ebay}")
                    record_ebay_listing(item_id_ebay, acct, sku, vendor_name, start_price_usd)
                    with conn.cursor() as cursor:
                        if is_collectibles:
                            cursor.execute("""
                                UPDATE mst.ebay_accounts
                                SET additional_listing_left = additional_listing_left - 1
                                WHERE account = ?
                            """, acct)
                        else:
                            cursor.execute("""
                                UPDATE mst.ebay_accounts
                                SET listing_left = listing_left - 1
                                WHERE account = ?
                            """, acct)
                    conn.commit()
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
                            fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = "model_us retry: listing_id未返却"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                fail_other_delta += 1
                return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                        fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)
            except ApiHandledError as e2:
                print(f"[25604 retry] 失敗: errorId={e2.code} {e2.message}")
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = f"25604→model_us retry failed: {e2.message}"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                fail_other_delta += 1
                return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                        fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)
            except Exception as e2:
                print(f"[25604 retry] 例外: {e2}")
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = f"25604→model_us retry exception: {e2}"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                fail_other_delta += 1
                return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                        fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)

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
                item_id_ebay = _attempt_post("CDN", model_name=_model_jp)
                if item_id_ebay:
                    print(f"✅ 出品成功(CDN retry): acct={acct} SKU={sku} listing_id={item_id_ebay}")
                    record_ebay_listing(item_id_ebay, acct, sku, vendor_name, start_price_usd)

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
                            fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)

                # retryでもlisting_id未返却
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = "CDN retry: listing_id未返却"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                fail_other_delta += 1

                return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                        fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)

            except Exception as e2:
                print(f"[IMG_ERR] CDN retry failed: {e2}")
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = f"CDN retry failed: {e2}"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                fail_other_delta += 1

                return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                        fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)

        # ★ 通常のAPI失敗確定
        policy = classify_ebay_error(err_msg)

        if policy:
            rec["listing_head"] = policy
            rec["listing_detail"] = ""
        else:
            rec["listing_head"] = "出品失敗"
            rec["listing_detail"] = err_msg

        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        fail_other_delta += 1

        return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)

    except Exception as e:
        print(f"❌ 出品失敗(未分類): acct={acct} SKU={sku} reason={e}")
        rec["listing_head"] = "出品失敗(未分類)"
        rec["listing_detail"] = str(e)
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        fail_other_delta += 1

        return (acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                fail_other_delta, image_mode, image_error_count, cdn_mode_until, listing_limit_hit)


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
          AND A.close_reason IS NULL
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
            if close_reason == "LIMIT":
                # LIMITを書き込む瞬間に、当日COUNTを再確認してから決める。
                # target到達済みならLIMITではなくDONEを確定する
                # （Python側でclose_reason="LIMIT"と判断した時点と、
                # 実際にDBへ書き込む時点との間に他workerの投稿が確定する
                # 可能性があるため、書き込み文自身で最新のCOUNTを見て判定する）。
                cur.execute("""
                    UPDATE mst.ebay_accounts
                    SET close_reason = CASE
                            WHEN (
                                SELECT COUNT(*) FROM trx.listings
                                WHERE account = ?
                                  AND CAST(start_time AS DATE) = CAST(GETDATE() AS DATE)
                                  AND is_deleted = 0
                            ) >= post_target
                            THEN 'DONE'
                            ELSE 'LIMIT'
                        END
                    WHERE account = ?
                """, (account_name, account_name))
            else:
                cur.execute("""
                    UPDATE mst.ebay_accounts
                    SET close_reason = ?
                    WHERE account = ?
                """, (close_reason, account_name))

        # 2. PCの占有解除 (account を NULL に戻す)
        cur.execute("""
            UPDATE mst.execute_pcs
            SET account = NULL
            WHERE execute_pc = ?
        """, (current_pc,))

        conn.commit()


SQL_SELECT_HAS_LIMIT_ACCOUNT = """
SELECT TOP 1 account
FROM mst.ebay_accounts
WHERE close_reason = 'LIMIT'
"""


def has_limit_accounts(conn):
    """
    close_reason='LIMIT' のアカウントが1件でも残っているか確認する。

    LIMITは「終了」ではなく「一時停止」。publish_manager.py が出品枠を確保して
    close_reason をクリアすれば再び取得できるようになるため、
    今すぐ処理できるアカウントが無くても、LIMIT中のアカウントが残っている間は
    publish_ebay を終了せず待機する（呼び出し側で判定に使う）。
    """

    with conn.cursor() as cur:
        cur.execute(SQL_SELECT_HAS_LIMIT_ACCOUNT)
        row = cur.fetchone()

    return row is not None


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
    【TVF委譲版】
    候補抽出は dbo.fn_take_one_candidates(@preset_group) に全委譲。
    この関数は TVF が返した候補の中から1件をロック・確保して返すだけ。
    """

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
            c.preset,
            c.mode,
            c.default_brand_en,
            c.category_id_ebay,
            c.department,
            c.type_ebay,
            c.category_group,
            c.low_jpy_target,
            c.high_jpy_target,
            inserted.item_condition_id,
            c.is_ok_logic,
            c.is_collectibles
        FROM trx.vendor_item v WITH (UPDLOCK, READPAST, ROWLOCK)
        INNER JOIN dbo.fn_take_one_candidates(?) c
            ON c.vendor_name    = v.vendor_name
           AND c.vendor_item_id = v.vendor_item_id
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
    # publish_ebay 全体のエントリポイント。
    # アカウントの終了理由は DONE（目標達成）/ EMPTY（在庫枯渇）/ LIMIT（eBay側の出品制限）の3種類。
    # DONE・EMPTYは当日の処理が本当に終わったことを意味するが、LIMITは「一時停止」であり、
    # publish_manager.py が出品枠を確保してclose_reasonをクリアすれば再開できる。
    # そのため「全アカウントの処理が終わった」と判断するのは、下の外側 while ループ内で
    # fetch_next_account_and_lock() が処理可能なアカウントを1件も返さず、かつ
    # LIMIT中（一時停止中）のアカウントも1件も残っていない時だけ。
    # LIMIT中のアカウントが残っている間は、プロセスを終了せず待機しながら再確認を続ける。
    # それまでは、1アカウントが終わるたびに次のアカウントを取得して処理を続ける
    # （＝1回のプロセス実行で複数アカウントを順番に処理し得る）。
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
    # LIMIT中のアカウントが残っている間、次の確認までどれくらい待つか（秒）。
    # publish_manager.py が出品枠を確保して close_reason をクリアするまでの
    # 待ち時間なので、数秒〜数十秒程度で十分（短すぎるとDBへの問い合わせが無駄に増える）。
    LIMIT_WAIT_SECONDS = 10
    conn = get_sql_server_connection()
    summary_success = {}
    driver = None

    try:
        from selenium.webdriver.chrome.options import Options
        presets = fetch_active_presets(conn)
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # ★ Lazy Initialization: driver はここでは生成しない。
        # メルカリshops商品に遭遇した時だけ生成する（下の row ループ内）。

        with sync_playwright() as p:

            

            # ===== 外側ループ: publish_ebay 全体の終了を決めるループ =====
            # アカウントの状態は DONE / EMPTY / LIMIT の3つ。
            #   DONE  : 当日の目標件数を達成 → 今日はもう処理しない（終了）
            #   EMPTY : 出品候補が無くなった → 今日はもう処理しない（終了）
            #   LIMIT : eBay側の出品制限 → 「一時停止」であり終了ではない。
            #           publish_manager.py が古い出品を削除して出品枠を確保し、
            #           close_reason をクリアすれば、そのアカウントは再び取得できる。
            # そのため「今すぐ処理できるアカウントが無い」だけでは終了と判断せず、
            # LIMIT中（一時停止中）のアカウントが残っていないかも合わせて確認する。
            #
            # ここを抜ける = publish_ebay プロセス全体の終了。抜ける条件は2つだけ:
            #   ① fetch_next_account_and_lock() が None を返し、かつ
            #      LIMIT中のアカウントも1件も無い（下のif not acct内のelse）
            #   ② stop_all = True （MAX_LISTINGS到達。現状 10**9 なので実運用では事実上起こらない）
            while not stop_all:
                # ===== アカウント取得 =====
                # close_reason IS NULL かつ sent_count<post_target のアカウントを1件確保する。
                # DONE/EMPTY/LIMITいずれも close_reason が非NULLになるため、
                # ここで None が返るのは「今すぐ処理できるアカウントが無い」状態。
                acct = fetch_next_account_and_lock(conn, current_pc)

                if not acct:
                    # ===== LIMIT待機 =====
                    # 今すぐ処理できるアカウントは無いが、LIMIT（一時停止）中のアカウントが
                    # 残っているなら、publish_managerの対応で近いうちに復帰する可能性がある。
                    # ここで終了してしまうとLIMIT解除後に誰も出品を再開できなくなるため、
                    # 少し待ってからもう一度アカウント取得を試みる（プロセスは終了しない）。
                    if has_limit_accounts(conn):
                        print(f"[INFO] 今すぐ処理できるアカウントはありませんが、LIMIT中のアカウントが残っています。{LIMIT_WAIT_SECONDS}秒待って再確認します。")
                        time.sleep(LIMIT_WAIT_SECONDS)
                        continue

                    # ===== 終了判定 =====
                    # 処理できるアカウントが無く、かつLIMIT中のアカウントも1件も無い。
                    # これでようやく「全アカウントの処理が終わった」と言えるため、
                    # ここで初めてプロセス全体を終了させる。
                    print("[INFO] 実行可能なアカウントがなく、LIMIT中のアカウントもありません。終了します。")
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

                # ===== 内側ループ: 1アカウント分の処理ループ =====
                # ここを抜けても外側ループは終わらない（次のアカウントへ進むだけ）。
                # 抜ける理由は close_reason で区別する:
                #   "DONE"  : 目標達成（今日は終了）
                #   "EMPTY" : 出品候補が無い（今日は終了）
                #   "LIMIT" : eBay側の出品制限（一時停止。publish_managerが解除すれば再開）
                while not stop_all:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT COUNT(*) 
                            FROM trx.listings 
                            WHERE account = ?
                            AND CAST(start_time AS DATE) = CAST(GETDATE() AS DATE)
                            AND is_deleted = 0
                        """, (acct.account,))
                        
                        sent_now = cur.fetchone()[0]

                    # [アカウント終了①: DONE] 当日の post_target に到達済み（DB上の実件数で判定）
                    # → close_reason="DONE"。今日はもう処理しない
                    #   （LIMITと違って一時停止ではなく、当日分の目標達成による正式な終了）。
                    if sent_now >= acct.post_target:
                        close_reason = "DONE"
                        print(f"✅ {acct.account} 当日目標数に達しました。")
                        break

                    if image_mode == "CDN" and cdn_mode_until and datetime.now() > cdn_mode_until:
                        print("[IMG_ERR] CDN timeout reached. Resetting to NORMAL.")
                        image_mode = "NORMAL"
                        image_error_count = 0
                        cdn_mode_until = None

                    row = take_one_vendor_item(conn, acct.preset_group, processing_by, acct.account)
                    if row:
                        print(f"[DEBUG] picked SKU={row['vendor_item_id']} price={row['price']} shipping_days={row['shipping_days']} 出品状況={row.get('出品状況')}", datetime.now().strftime("%H:%M:%S"))
                    # [アカウント終了②] このアカウントの担当レンジ(preset_group)に
                    # 出品candidate が無くなった（在庫枯渇）
                    # → close_reason="EMPTY"。
                    #   当日はこのアカウントは再取得されなくなる。
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

                    print(f"[DEBUG] url={item_url}", datetime.now().strftime("%H:%M:%S"))

                    # ===== Selenium driver の遅延生成（メルカリshopsの時だけ） =====
                    if driver is None and vendor_name == "メルカリshops":
                        driver = webdriver.Chrome(options=options)
                        print("[Selenium] driver created for Mercari Shops")

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
                            row["default_brand_en"], row["category_id_ebay"], row["department"], row["type_ebay"],row["category_group"],
                            {}, writes_since_commit, row["low_jpy_target"], row["high_jpy_target"],driver,row["item_condition_id"] 
                        )

                        if heavy:
                            (
                                acct_targets, acct_success, total_listings, stop_all, writes_since_commit,
                                _, image_mode, image_error_count, cdn_mode_until, listing_limit_hit,
                            ) = post_to_ebay(
                                conn=conn, p=None, acct=acct.account, heavy=heavy,
                                acct_targets=acct_targets, acct_success=acct_success,
                                acct_policies_map=acct_policies_map,
                                total_listings=total_listings, MAX_LISTINGS=MAX_LISTINGS,
                                stop_all=stop_all, writes_since_commit=writes_since_commit,
                                BATCH_COMMIT=BATCH_COMMIT, image_mode=image_mode,
                                image_error_count=image_error_count, cdn_mode_until=cdn_mode_until,
                                r2=r2, r2_bucket=r2_bucket_name, r2_public_base=r2_public_base,
                                cdn_cache=cdn_cache, now_dt=datetime.now(),preset=row["preset"],
                                is_collectibles=row.get("is_collectibles"),
                            )
                            if stop_all:
                                # [publish_ebay 全体の終了①つづき] MAX_LISTINGS到達を検知。
                                # このアカウントの内側ループだけでなく、外側の
                                # `while not stop_all` も条件を満たさなくなるため
                                # プロセス全体がこのあと終了する。
                                break

                            # [アカウント終了③] eBayのListingLimitErrorが実際に発生した場合だけ
                            # close_reason="LIMIT" にする。
                            # acct_targets（残り投稿件数のカウントダウン）とLIMIT判定は完全に分離した。
                            # 目標件数ちょうどまで投稿できた場合はacct_targetsが0になるだけで
                            # listing_limit_hitはFalseのままなので、ここには来ない
                            # （その場合は次のループ先頭のsent_now>=post_targetチェックで正常終了する）。
                            if listing_limit_hit:
                                print(f"🚫 {acct.account} APIリミットを検知しました。")
                                close_reason = "LIMIT"
                                break

                        if writes_since_commit > 0:
                            conn.commit()

                    except FatalRendererError:
                        # [publish_ebay 全体の終了③] ブラウザ(renderer)側の致命的クラッシュ。
                        # 個別アカウント終了ではなく、プロセスそのものを即座に異常終了させる
                        # （sys.exit(1)。sys.exit(10)の正常終了とは区別されるコード）。
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

                # このアカウントの処理はここで終わり（close_reason: DONE/EMPTY/LIMITのいずれか）。
                # release_pc_and_close_account が close_reason を書き込む。
                # DONE/EMPTYは今日はもう再取得されない。LIMITも同様にclose_reasonが立つが、
                # 「終了」ではなく「一時停止」であり、publish_manager.py が出品枠を確保して
                # close_reason をクリアすれば、このアカウントは再び取得できるようになる。
                # 外側 while はここでは終わらず、次のアカウントを取りに戻る。
                release_pc_and_close_account(conn, current_pc, acct.account, close_reason)
                summary_success[acct.account] = summary_success.get(acct.account, 0) + acct_success.get(acct.account, 0)
                print(f"🏁 アカウント終了: {acct.account} (Reason: {close_reason})")


        # [publish_ebay 全体の終了②] 外側 while を抜けた＝
        #   ・処理可能なアカウントが無く、かつLIMIT中のアカウントも1件も無い（通常はこちら）
        #   ・または stop_all=True（MAX_LISTINGS到達）
        # のどちらか。LIMIT中のアカウントが残っている間はここに来ない
        # （while内でsleepして待機し続ける）。ここに来て初めて
        # 「全アカウントの処理が終わった」ことになり、プロセス自体が終了する（sys.exit(10)）。
        print(f"[EXIT] 処理完了（合計出品数: {total_listings}）")
        sys.exit(10)

    finally:
        # 正常終了(sys.exit(10))・異常終了(sys.exit(1)含む例外)いずれの経路でも、
        # このプロセスが握っていたPCスロット(mst.execute_pcs)は必ず解放する後片付け。
        try:
            release_pc_and_close_account(conn, current_pc)
        except:
            pass
        if driver is None:
            print("[Selenium] driver was not required in this run")
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