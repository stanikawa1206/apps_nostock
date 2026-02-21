# -*- coding: utf-8 -*-
# publish_ebay_new.py — listings / vendor_item 対応（Shops/通常 両対応・processing_by方式, Py3.8/3.9互換）

from __future__ import annotations

# =========================
# Standard library
# =========================
import random
import re
import sys
import time
import socket  # ★ NEW
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from selenium.common.exceptions import TimeoutException, WebDriverException
import os
from dotenv import load_dotenv
import boto3
from datetime import timedelta  # 追加（mainのstateで使う）
from dataclasses import dataclass


# =========================
# Third-party
# =========================
import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# =========================
# sys.path bootstrap: file-direct run safe
# =========================
# このファイル: D:\apps_nostock\apps\publish\publish_ebay_new.py
# プロジェクトルート: D:\apps_nostock  ← parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# =========================
# Local (project)
# =========================
from apps.common.utils import (
    compute_start_price_usd,
    compute_cost_range_jpy_from_usd_range,
    generate_ebay_description,
    get_sql_server_connection,
    send_mail,
    translate_to_english,
    contains_risky_word,
    build_driver,
    get_openai_client,
)

from apps.adapters.ebay_api import ApiHandledError, ListingLimitError, post_one_item
from apps.adapters.mercari_search import fetch_active_presets
from apps.adapters.mercari_item_status import (
    MercariItemUnavailableError,
    detect_status_from_mercari,
    detect_status_from_mercari_shops,
    handle_listing_delete,
    mark_vendor_item_unavailable,
)

# ========= 固定値／運用設定 =========
IMG_LIMIT     = 10
BATCH_COMMIT  = 100

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

# ========= UI 補助 =========
def _close_any_modal(driver):
    """同意/閉じる系のボタンがあれば雑に閉じる。"""
    try:
        js = """
          return Array.from(document.querySelectorAll('button,[role=button]')).find(b=>{
            const t=(b.innerText||'').trim();
            return ['同意','閉じる','OK','Accept','Close','許可しない'].some(k=>t.includes(k));
          });
        """
        btn = driver.execute_script(js)
        if btn:
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.2)
    except Exception:
        pass

def extract_mercari_description_from_dom(driver, timeout: int = 10) -> str:
    """
    現在表示中のメルカリ(通常/shops 共通)の商品ページから
    <pre data-testid="description"> のテキストを取得する。
    見つからなければ空文字。
    """
    try:
        pre = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "pre[data-testid='description']")
            )
        )
        return (pre.text or "").strip()
    except TimeoutException:
        return ""
    except Exception:
        return ""

def _try_extract_title(driver, vis_timeout=8.0) -> str:
    """通常メルカリ詳細からタイトル抽出（最低限）。"""
    sels: List[Tuple[str, str]] = [
        (By.CSS_SELECTOR, '#item-info h1'),
        (By.CSS_SELECTOR, '[data-testid="item-name"]'),
        (By.CSS_SELECTOR, 'h1[role="heading"]'),
        (By.CSS_SELECTOR, 'h1'),
    ]
    for by, sel in sels:
        try:
            el = WebDriverWait(driver, vis_timeout).until(EC.visibility_of_element_located((by, sel)))
            t = (el.text or "").strip()
            if t:
                return t
        except Exception:
            continue
    try:
        og = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
        t = (og.get_attribute("content") or "").strip()
        return t
    except Exception:
        return ""

def _find_seller_info(driver, url: str):
    """
    通常メルカリ商品の seller_id / seller_name / rating_count を取得する。
    ※ driver.get(url) は呼び出し側で済んでいる前提
    """
    try:
        a = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[href*='/user/profile/']")
            )
        )
    except TimeoutException:
        print(f"[DBG] seller link not found: {url}")
        return None, None, None

    href = (a.get_attribute("href") or "").strip()

    seller_name = (a.get_attribute("aria-label") or a.text or "").strip()
    if "," in seller_name:
        seller_name = seller_name.split(",", 1)[0].strip()

    if not href:
        return None, None, None

    seller_id = href.rstrip("/").split("/")[-1]
    if not seller_id:
        return None, None, None

    rating_count = None
    try:
        container = driver.find_element(By.CSS_SELECTOR, "[data-testid='seller-link']")
        for span in container.find_elements(By.TAG_NAME, "span"):
            txt = (span.text or "").strip().replace(",", "")
            if txt.isdigit():
                rating_count = int(txt)
                break
    except Exception:
        pass

    return seller_id, seller_name, rating_count

# ========= Shops向けセラー抽出・画像収集 =========
def _extract_shops_seller(driver) -> Tuple[str, str, int]:
    """ShopsのセラーID/名前/評価数を取得。"""
    a = WebDriverWait(driver, 6).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, 'a[data-testid="shops-profile-link"]'))
    )
    href = (a.get_attribute("href") or "").strip()
    seller_id = href.rstrip("/").split("/")[-1] if href else ""

    block = (a.text or "").strip()
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    name = lines[0] if lines else ""

    m = re.search(r"(\d[\d,]*)", block)
    rating = int(m.group(1).replace(",", "")) if m else 0

    return seller_id, name, rating

_RE_IMAGE_N = re.compile(r"^image-(\d+)$")

def collect_images_shops(driver, limit: int = IMG_LIMIT) -> List[Optional[str]]:
    """
    メルカリShopsの商品画像URLを取得（カルーセル内の img[src] のみ）
    """
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    carousel = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="carousel"]'))
    )

    t_end = time.time() + 5
    while time.time() < t_end:
        if carousel.find_elements(By.CSS_SELECTOR, "img[src]"):
            break
        time.sleep(0.2)

    urls: List[str] = []
    seen = set()

    for el in carousel.find_elements(By.CSS_SELECTOR, "img[src]"):
        src = (el.get_attribute("src") or "").strip()
        if not src:
            continue
        if src in seen:
            continue
        seen.add(src)
        urls.append(src)
        if len(urls) >= limit:
            break

    if not urls:
        img_count = len(carousel.find_elements(By.CSS_SELECTOR, "img"))
        img_src_count = len(carousel.find_elements(By.CSS_SELECTOR, "img[src]"))
        indicator = ""
        try:
            indicator = carousel.find_element(By.CSS_SELECTOR, '[data-testid="page-indicator-numeric"]').text
        except Exception:
            pass
        raise RuntimeError(
            f"[collect_images_shops] urls empty. img={img_count}, img[src]={img_src_count}, indicator={indicator!r}"
        )

    out: List[Optional[str]] = urls[:limit]
    out += [None] * (limit - len(out))
    return out

# ========= 詳細解析（Shops / 通常） =========
def parse_detail_shops(driver, url: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """メルカリShopsの商品詳細を解析し、必要最低限の情報を返す。"""
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    _close_any_modal(driver)

    status, _ = detect_status_from_mercari_shops(driver)
    if status != "販売中":
        raise MercariItemUnavailableError(status)

    description_jp = extract_mercari_description_from_dom(driver)

    title, price, last_updated_str = "", 0, ""

    try:
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="product-title-section"]'))
        )

        for h in container.find_elements(By.TAG_NAME, "h1"):
            t = (h.text or "").strip()
            if t:
                title = t
                break

        if not title:
            snippet = (container.text or "").replace("\n", " ")[:80]
            print(f"[DBG_SHOPS_TITLE] url={url}  h1空 or なし  snippet={snippet!r}")
    except Exception as e:
        print(f"[DBG_SHOPS_TITLE] url={url}  タイトル取得失敗: {e}")
        title = ""

    try:
        box = driver.find_element(By.CSS_SELECTOR, '[data-testid="product-price"]').text
        price = int(re.sub(r"[^\d]", "", box))
    except Exception:
        pass
    try:
        dt_el = driver.find_element(By.CSS_SELECTOR, '#product-info > section:nth-child(2) > p')
        last_updated_str = (dt_el.text or "").strip()
    except Exception:
        pass

    shipping_region = ""
    shipping_days = ""
    try:
        el = driver.find_element(By.CSS_SELECTOR, 'span[data-testid="発送元の地域"]')
        shipping_region = (el.text or "").strip()
    except Exception:
        pass

    try:
        el = driver.find_element(By.CSS_SELECTOR, 'span[data-testid="発送までの日数"]')
        shipping_days = (el.text or "").strip()
    except Exception:
        pass

    try:
        seller_id, seller_name, rating_count = _extract_shops_seller(driver)
    except Exception:
        seller_id, seller_name, rating_count = "", "", 0

    images = collect_images_shops(driver, limit=IMG_LIMIT)

    return {
        "vendor_name": vendor_name,
        "item_id": url.rstrip("/").split("/")[-1],
        "title_jp": title,
        "title_en": "",
        "price": price,
        "last_updated_str": last_updated_str,
        "shipping_region": shipping_region,
        "shipping_days": shipping_days,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "rating_count": rating_count,
        "images": images,
        "preset": preset,
        "description": description_jp,
        "description_en": "",
    }

LAST_UPDATED_RE = re.compile(
    r"(?:\d+\s*(?:秒|分|時間|日|か月|年)\s*前|半年以上前)",
    flags=re.UNICODE,
)

def extract_last_updated_personal(driver, timeout: float = 8.0, tries: int = 3) -> str:
    """#item-info配下から「◯分前/◯時間前/◯日前/◯秒前/◯か月前/◯年前/半年以上前」を位置非依存で抽出。"""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#item-info"))
        )
    except TimeoutException:
        pass

    selectors = [
        "#item-info p",
        "#item-info time",
        "#item-info span",
        "#item-info div",
    ]

    for _ in range(tries):
        for sel in selectors:
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    txt = (el.text or "").strip()
                    if not txt:
                        continue
                    m = LAST_UPDATED_RE.search(txt)
                    if m:
                        return m.group(0)
            except Exception:
                continue
        try:
            all_text = driver.execute_script(
                "return (document.querySelector('#item-info')?.innerText"
                " || document.body.innerText || '')"
            ) or ""
            m = LAST_UPDATED_RE.search(all_text)
            if m:
                return m.group(0)
        except Exception:
            pass
        time.sleep(0.4 + random.uniform(0.0, 0.2))

    return ""

def collect_images_personal(driver, limit: int = IMG_LIMIT) -> List[Optional[str]]:
    """
    通常メルカリ（personal）の商品画像URLを取得する。
    - data-testid="carousel" 内の img[src] のみ取得
    """
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    carousel = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="carousel"]'))
    )

    t_end = time.time() + 5
    while time.time() < t_end:
        if carousel.find_elements(By.CSS_SELECTOR, "img[src]"):
            break
        time.sleep(0.2)

    urls: List[str] = []
    seen = set()

    for el in carousel.find_elements(By.CSS_SELECTOR, "img[src]"):
        src = (el.get_attribute("src") or "").strip()
        if not src:
            continue
        if src in seen:
            continue
        seen.add(src)
        urls.append(src)
        if len(urls) >= limit:
            break

    if not urls:
        img_count = len(carousel.find_elements(By.CSS_SELECTOR, "img"))
        img_src_count = len(carousel.find_elements(By.CSS_SELECTOR, "img[src]"))
        raise RuntimeError(
            f"[collect_images_personal] urls empty. img={img_count}, img[src]={img_src_count}"
        )

    out: List[Optional[str]] = urls[:limit]
    out += [None] * (limit - len(out))
    return out

def parse_detail_personal(driver, url: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """通常メルカリの商品詳細を解析し、必要最低限の情報を返す。"""
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    _close_any_modal(driver)

    status, _ = detect_status_from_mercari(driver)
    if status != "販売中":
        raise MercariItemUnavailableError(status)

    title = _try_extract_title(driver)
    price = 0
    last_updated_str = ""

    try:
        element = driver.find_element(By.CSS_SELECTOR, '[data-testid*="price"]')
        price = int(re.sub(r"[^\d]", "", (element.text or "")))
    except Exception:
        pass

    try:
        last_updated_str = extract_last_updated_personal(driver)
    except Exception:
        pass

    description_jp = extract_mercari_description_from_dom(driver)

    shipping_region = ""
    shipping_days = ""
    try:
        el = driver.find_element(By.CSS_SELECTOR, 'span[data-testid="発送元の地域"]')
        shipping_region = (el.text or "").strip()
    except Exception:
        pass

    try:
        el = driver.find_element(By.CSS_SELECTOR, 'span[data-testid="発送までの日数"]')
        shipping_days = (el.text or "").strip()
    except Exception:
        pass

    seller_id, seller_name, rating_count = _find_seller_info(driver, url)

    if not seller_id:
        try:
            _ = driver.title
            _ = driver.execute_script(
                "return (document.body.innerText || '').slice(0, 300);"
            )
        except Exception as e:
            print(f"[DBG_PAGE_WHEN_NO_SELLER_ERR] url={url} err={e}")

    images = collect_images_personal(driver, IMG_LIMIT)

    return {
        "vendor_name": vendor_name,
        "item_id": url.rstrip("/").split("/")[-1],
        "title_jp": title,
        "title_en": "",
        "price": price,
        "last_updated_str": last_updated_str,
        "shipping_region": shipping_region,
        "shipping_days": shipping_days,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "rating_count": rating_count,
        "images": images,
        "preset": preset,
        "description": description_jp,
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
MERGE INTO [trx].[vendor_item] AS tgt
USING (
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        SYSDATETIME(),
        SYSDATETIME(),
        NULL,
        N'販売中',
        COALESCE(src.listing_head, N''),
        COALESCE(src.listing_detail, N''),
        CASE
          WHEN src.listing_head IN (N'古い更新', N'計算価格が範囲外', N'NG(セラー評価)') THEN SYSDATETIME()
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
    imgs = (imgs + [None] * 10)[:10]

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
        rec["item_id"],

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
    conn, driver, item_url, sku, preset, vendor_name,
    p, debug_unavailable_dump, writes_since_commit,
):
    """
    ✅ ここでは「詳細解析」「NG判定」「翻訳生成」まで。
    ✅ 画像URLの最終決定（NORMAL/CDN）は post_to_ebay 側でやる（重要）
    """
    # === 1) scrape ===
    try:
        rec = (
            parse_detail_shops(driver, item_url, preset, vendor_name)
            if vendor_name == "メルカリshops"
            else parse_detail_personal(driver, item_url, preset, vendor_name)
        )
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
            "item_id": sku,
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
    if re.search(r'(半年以上前|\d+\s*[ヶか]月前|数\s*[ヶか]月前)', rec.get("last_updated_str") or ""):
        rec["listing_head"] = "古い更新"
        rec["listing_detail"] = rec.get("last_updated_str") or ""
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 4) 計算価格（NG） ===
    start_price_usd = compute_start_price_usd(
        rec.get("price"), p["mode"], p["low_usd_target"], p["high_usd_target"]
    )
    if not start_price_usd:
        rec["listing_head"] = "計算価格が範囲外"
        rec["listing_detail"] = f"{p['low_usd_target']}–{p['high_usd_target']}USD"
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
    if p["mode"] == "GA":
        is_ng, reason = has_color_touchup_or_repair(
            jp_title=jp_title,
            jp_description=desc_jp,
        )
        if is_ng:
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
        expected_brand_en = p.get("default_brand_en")
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
            expected_brand_en = p.get("default_brand_en")
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
            "category_id": p["category_id_ebay"],
            "C:Brand": p["default_brand_en"],
            "department": p["department"],
            "C:Color": "Multicolor",
            "C:Type": p["type_ebay"],
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


def fetch_accounts_for_pc(conn, current_pc):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT account, preset_group, post_target
            FROM mst.ebay_accounts
            WHERE execute_pc = ?
              AND ISNULL(is_excluded,0) = 0
            ORDER BY account
        """, (current_pc,))

        rows = cur.fetchall()

    return [
        Account(
            account=r[0].strip(),
            preset_group=r[1].strip(),
            post_target=r[2]
        )
        for r in rows
    ]

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

def take_one_vendor_item_by_preset(
    conn,
    preset,
    processing_by,
    start_time,
    low_cost,
    high_cost,
):
    """
    preset単位で1件確保（古い順）
    """
    sql = """
    ;WITH cte AS (
        SELECT TOP (1) vendor_item_id
        FROM dbo.vw_vendor_item_ready
        WHERE preset = ?
          AND price BETWEEN ? AND ?
        ORDER BY created_at ASC
    )
    UPDATE v
    SET processing_by = ?,
        processing_at = ?
    OUTPUT inserted.vendor_item_id,
           inserted.vendor_name,
           inserted.price,
           inserted.shipping_region,
           inserted.shipping_days,
           inserted.preset
    FROM trx.vendor_item v
    JOIN cte ON v.vendor_item_id = cte.vendor_item_id;
    """

    with conn.cursor() as cur:
        cur.execute(
            sql,
            preset,
            low_cost,
            high_cost,
            processing_by,
            start_time,
        )
        row = cur.fetchone()
        if not row:
            return None
        return row


def main():
    print("26022121_Final_Fixed") # バージョンを更新

    # --- 修正ポイント1: .env の場所を絶対パスで指定 ---
    # プロジェクトルートにある .env を確実に読み込むようにします
    from pathlib import Path
    _PROJECT_ROOT = Path(__file__).resolve().parents[2] # 階層に合わせて調整してください
    env_path = _PROJECT_ROOT / ".env"
    
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=env_path)
    print(f"DEBUG: .env loaded from {env_path}")

    current_pc = socket.gethostname().strip()
    processing_by = get_processing_by()
    start_time = datetime.now()
    
    # --- 修正ポイント2: .strip() で目に見えないゴミを削除 ---
    # これにより SignatureDoesNotMatch を防ぎます
    r2_endpoint    = os.getenv("R2_ENDPOINT", "").strip()
    r2_access_key  = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    r2_secret_key  = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    r2_bucket_name = os.getenv("R2_BUCKET", "").strip()
    r2_public_base = os.getenv("R2_PUBLIC_BASE", "").strip()

    # 2. テストで成功したロジックをそのまま適用
    if r2_endpoint and r2_bucket_name and r2_endpoint.endswith("/" + r2_bucket_name):
        r2_endpoint = r2_endpoint.replace("/" + r2_bucket_name, "")

    # 3. テストで成功した Config 指定を適用
    from botocore.config import Config
    r2 = boto3.client(
        "s3",
        endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key,
        aws_secret_access_key=r2_secret_key,
        region_name="auto",
        config=Config(signature_version='s3v4') 
    )

    # 既存の変数への代入
    R2_BUCKET = r2_bucket_name
    R2_PUBLIC_BASE = r2_public_base

    # デバッグ出力（キーは一部隠して表示）
    print("ACCESS_KEY:", f"{r2_access_key[:5]}...")
    print("ENDPOINT:", r2_endpoint)
    print("BUCKET:", r2_bucket_name)

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
    driver = build_driver()

    try:
        # ===== アカウント取得 =====
        accounts = fetch_accounts_for_pc(conn, current_pc)
        presets = fetch_active_presets(conn)

        # ===== policies事前ロード =====
        acct_policies_map = {}
        with conn.cursor() as cur:
            for acct in accounts:
                cur.execute("""
                    SELECT fulfillment_policy_id,
                           payment_policy_id,
                           return_policy_id
                    FROM mst.ebay_accounts
                    WHERE account = ?
                """, acct.account)
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(f"policy未設定: {acct.account}")

                acct_policies_map[acct.account] = {
                    "fulfillment_policy_id": str(row[0]),
                    "payment_policy_id": str(row[1]),
                    "return_policy_id": str(row[2]),
                    "merchant_location_key": "Default",
                }

        acct_success = {acct.account: 0 for acct in accounts}
        acct_targets = {acct.account: acct.post_target for acct in accounts}

        # ===== 第一階層 アカウントループ =====
        # このPCに割り当てられた複数アカウントを順番に処理する
        for acct in accounts:

            print(f"[ACCOUNT START] {acct.account}")

            # --- quota判定（acct_targets を参照）---
            def has_quota(a: Account) -> bool:
                t = acct_targets.get(a.account)
                if t is None:
                    return True
                return t > 0

            group_presets = [
                p for p in presets
                if (p.get("preset_group") or "").strip() == acct.preset_group
            ]

            if not group_presets:
                continue

            # ===== 第二階層 アカウント内ループ =====
            # そのアカウントで「何件出品するか」を管理
            while has_quota(acct) and not stop_all:

                vendor_row = None
                selected_preset = None

                # ===== preset順次探索 =====
                for preset_obj in group_presets:

                    preset = preset_obj["preset"]

                    low_cost, high_cost = compute_cost_range_jpy_from_usd_range(
                        preset_obj["mode"],
                        preset_obj["low_usd_target"],
                        preset_obj["high_usd_target"],
                    )

                    row = take_one_vendor_item_by_preset(
                        conn,
                        preset,
                        processing_by,
                        start_time,
                        low_cost,
                        high_cost,
                    )

                    if row:
                        vendor_row = row
                        selected_preset = preset_obj
                        break

                if not vendor_row:
                    print(f"[INFO] {acct.account} 在庫枯渇")
                    break

                vendor_item_id, vendor_name, price, shipping_region, shipping_days, preset = vendor_row
                sku = vendor_item_id.strip()

                if vendor_name == "メルカリshops":
                    item_url = f"https://mercari-shops.com/products/{sku}"
                else:
                    item_url = f"https://jp.mercari.com/item/{sku}"

                # ===== heavy =====
                try:
                    heavy, _, writes_since_commit, _, _ = heavy_check_detail(
                        conn,
                        driver,
                        item_url,
                        sku,
                        preset,
                        vendor_name,
                        selected_preset,
                        {},
                        writes_since_commit
                    )
                except FatalRendererError:
                    print("[RECOVERY] Renderer crash")
                    driver.quit()
                    time.sleep(3)
                    driver = build_driver()
                    continue

                if not heavy:
                    continue

                # ===== 出品 =====
                (
                    acct_targets,
                    acct_success,
                    total_listings,
                    stop_all,
                    writes_since_commit,
                    _,
                    image_mode,
                    image_error_count,
                    cdn_mode_until,
                ) = post_to_ebay(
                    conn=conn,
                    p=selected_preset,
                    acct=acct.account,
                    heavy=heavy,
                    acct_targets=acct_targets,
                    acct_success=acct_success,
                    acct_policies_map=acct_policies_map,
                    total_listings=total_listings,
                    MAX_LISTINGS=MAX_LISTINGS,
                    stop_all=stop_all,
                    writes_since_commit=writes_since_commit,
                    BATCH_COMMIT=BATCH_COMMIT,
                    image_mode=image_mode,
                    image_error_count=image_error_count,
                    cdn_mode_until=cdn_mode_until,
                    r2=r2,
                    r2_bucket=r2_bucket_name,
                    r2_public_base=r2_public_base,
                    cdn_cache=cdn_cache,
                    now_dt=datetime.now(),
                )

 

        if writes_since_commit > 0:
            conn.commit()

        # ===== 完了メール =====
        end_time = datetime.now()
        elapsed = end_time - start_time

        lines = [f"{acct}: 成功 {acct_success.get(acct,0)}"
                 for acct in acct_success.keys()]

        body = (
            f"PC: {current_pc}\n"
            f"開始: {start_time}\n終了: {end_time}\n処理時間: {elapsed}\n\n"
            + "\n".join(lines)
        )

        send_mail(
            "✅ eBay出品処理 完了通知",
            body
        )

    finally:
        driver.quit()
        conn.close()




def main_test():
    print("=== TAKE_ONE_VENDOR_ITEM_SQL テスト開始 ===")

    preset = "ヴィトン長財布MS"
    worker_name = "TEST_WORKER"

    from datetime import datetime, timedelta
    lock_timeout = datetime.now() - timedelta(minutes=10)

    conn = get_sql_server_connection()
    cur = conn.cursor()

    # --------------------------------------
    # ① preset情報取得
    # --------------------------------------
    cur.execute("""
        SELECT mode, low_usd_target, high_usd_target
        FROM mst.v_presets
        WHERE preset = ?
    """, preset)

    row = cur.fetchone()
    if not row:
        print("presetが見つかりません")
        return

    p = {
        "mode": row[0],
        "low_usd_target": row[1],
        "high_usd_target": row[2],
    }

    # --------------------------------------
    # ② 本番と同じ価格レンジ算出
    # --------------------------------------
    low_cost, high_cost = compute_cost_range_jpy_from_usd_range(
        p["mode"],
        p["low_usd_target"],
        p["high_usd_target"],
    )

    print(f"価格レンジ: {low_cost} ～ {high_cost}")

    # --------------------------------------
    # ③ ready view 件数確認
    # --------------------------------------
    TEST_SQL = """
    SELECT COUNT(*)
    FROM vw_vendor_item_ready v
    WHERE
        v.preset = ?
        AND v.price BETWEEN ? AND ?
        AND (
            v.processing_at IS NULL
            OR v.processing_at < ?
            OR (
                v.processing_by = ?
                AND v.processing_at < ?
            )
        )
    """

    cur.execute(
        TEST_SQL,
        preset,
        low_cost,
        high_cost,
        lock_timeout,
        worker_name,
        lock_timeout,
    )

    count = cur.fetchone()[0]

    print(f"取得可能件数 = {count}")

    cur.close()
    conn.close()

    print("=== テスト終了 ===")




if __name__ == "__main__":
    main()
    #main_test()