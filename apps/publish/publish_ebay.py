# -*- coding: utf-8 -*-
# new_publish_ebay.py — listings / vendor_item 対応（Shops/通常 両対応・簡潔版, Py3.8/3.9互換）

from __future__ import annotations

# =========================
# Standard library
# =========================
import random
import re
import sys
import time
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
from selenium.common.exceptions import TimeoutException

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
)

from apps.adapters.ebay_api import ApiHandledError, ListingLimitError, post_one_item
from apps.adapters.mercari_search import calc_cost_range_from_usd_range, fetch_active_presets
from apps.adapters.mercari_item_status import (
    MercariItemUnavailableError,
    detect_status_from_mercari,
    detect_status_from_mercari_shops,
    handle_listing_delete,
    mark_vendor_item_unavailable,
)

# ========= 固定値／運用設定 =========
IMG_LIMIT     = 10            # 画像の最大拾得枚数
BATCH_COMMIT  = 100

# ========= NG打刻・スキップ関連定義 =========
# last_ng_at を打刻するのは「古い更新」「計算価格が範囲外」だけ
NG_HEADS_FOR_TIMESTAMP: Set[str] = {
    "古い更新",
    "計算価格が範囲外",
}

# 7日スキップ対象（last_ng_at と組み合わせて除外）
HEADS_FOR_7DAY_SKIP: Set[str] = {
    "古い更新",
    "計算価格が範囲外",
}

# ========= WebDriver =========
def build_driver():
    """Selenium ChromeDriver を headless/eager で起動。"""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--lang=ja-JP,ja")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119 Safari/537.36")
    opts.page_load_strategy = "eager"
    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_window_size(1400, 1000)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver

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
        # セラーリンクが出るまで待つ
        a = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[href*='/user/profile/']")
            )
        )
    except TimeoutException:
        print(f"[DBG] seller link not found: {url}")
        return None, None, None

    href = (a.get_attribute("href") or "").strip()

    # aria-label に「Zoo Zoo, 1166件のレビュー…」が入っているので、
    # そこから店名だけ抜く or テキストを使う
    seller_name = (a.get_attribute("aria-label") or a.text or "").strip()
    if "," in seller_name:
        seller_name = seller_name.split(",", 1)[0].strip()

    if not href:
        return None, None, None

    # 末尾が 998054173 なので、split でIDだけ取り出す
    seller_id = href.rstrip("/").split("/")[-1]
    if not seller_id:
        return None, None, None

    # 評価数（1166など）も取れれば取る（取れなくても致命的ではない）
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

    # ★ 店名は “先頭行” のみ（評価数やバッジ文言を混ぜない）
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    name = lines[0] if lines else ""

    # 評価数は従来通り：ブロック内の数字から抜く
    m = re.search(r"(\d[\d,]*)", block)
    rating = int(m.group(1).replace(",", "")) if m else 0

    return seller_id, name, rating

_RE_IMAGE_N = re.compile(r"^image-(\d+)$")

def collect_images_shops(driver, limit: int = IMG_LIMIT) -> List[Optional[str]]:
    """
    メルカリShopsの商品画像URLを取得（カルーセル内の img[src] のみ）
    - data-testid(image-x) に依存しない
    - 右側の商品一覧などは混ざらない（carousel内限定）
    """
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    carousel = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="carousel"]'))
    )

    # JSで遅れてsrcが入ることがあるので、短く待つ
    t_end = time.time() + 5
    while time.time() < t_end:
        if carousel.find_elements(By.CSS_SELECTOR, "img[src]"):
            break
        time.sleep(0.2)

    urls: List[str] = []
    seen = set()

    # ★カルーセル内のimg[src]をDOM順で取る（7枚なら7枚、10枚なら10枚）
    for el in carousel.find_elements(By.CSS_SELECTOR, "img[src]"):
        src = (el.get_attribute("src") or "").strip()
        if not src:
            continue
        # 重複排除（サムネとメインが同じsrcの場合がある）
        if src in seen:
            continue
        seen.add(src)
        urls.append(src)
        if len(urls) >= limit:
            break

    if not urls:
        # ここに来たら構造変更 or ブロックが別、原因明確化のため落とす
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

    # ★ ここでメルカリ側の状態をチェック（削除・売切れなど）
    status, _ = detect_status_from_mercari_shops(driver)
    if status != "販売中":
        raise MercariItemUnavailableError(status)

    description_jp = extract_mercari_description_from_dom(driver)

    title, price, last_updated_str = "", 0, ""

    # --- タイトル取得（コンテナ→h1 スキャン方式） ---
    try:
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, '[data-testid="product-title-section"]')
            )
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

# --- 先頭付近のimportの下あたりに追加 ---
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
    - shops と同一思想
    """
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    # ★ personal も carousel は data-testid="carousel"
    carousel = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="carousel"]'))
    )

    # JS遅延対策（src が後から入る）
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
            f"[collect_images_personal] urls empty. "
            f"img={img_count}, img[src]={img_src_count}"
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

# ★ 統合版：scrape結果 + 判定結果 + last_ng_at制御（古い更新/計算価格が範囲外のみ打刻、他はNULL）
# ★ 部分取得は「NULLで上書きしない」ため、UPDATE側は COALESCE(src, tgt) に寄せる
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
            WHEN src.listing_head IN (N'古い更新', N'計算価格が範囲外') THEN SYSDATETIME()
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
        src.title_en,   -- title_en_bk（同値でOK）
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
          WHEN src.listing_head IN (N'古い更新', N'計算価格が範囲外') THEN SYSDATETIME()
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
    """
    1件の vendor_item を MERGE。
    - scrape結果（title/price/shipping等）も
    - 判定結果（出品状況/詳細）も
    まとめて1回で更新する。
    """
    imgs = (rec.get("images") or [])
    imgs = (imgs + [None] * 10)[:10]

    preset_val  = _none_if_blank(rec.get("preset"))
    vendor_page = rec.get("vendor_page")  # 0,1,2,... or None

    # 部分取得：空文字は None に寄せて「NULLで上書き」を避ける
    title_jp = _none_if_blank(rec.get("title_jp"))
    title_en = _none_if_blank(rec.get("title_en"))
    desc_jp  = _none_if_blank(rec.get("description"))
    desc_en  = _none_if_blank(rec.get("description_en"))

    last_updated_str = _none_if_blank(rec.get("last_updated_str"))
    shipping_region  = _none_if_blank(rec.get("shipping_region"))
    shipping_days    = _none_if_blank(rec.get("shipping_days"))
    seller_id        = _none_if_blank(rec.get("seller_id"))

    # price は None も許容（部分取得でpriceが取れないケース対策）
    price_val = rec.get("price")
    if price_val is not None:
        try:
            price_val = int(price_val)
        except Exception:
            price_val = None

    listing_head   = _none_if_blank(rec.get("listing_head"))
    listing_detail = _none_if_blank(rec.get("listing_detail"))

    params = (
        rec["vendor_name"],             # vendor_name
        rec["item_id"],                 # vendor_item_id

        title_jp,                       # title_jp
        title_en,                       # title_en
        desc_jp,                        # description
        desc_en,                        # description_en

        price_val,                      # price
        last_updated_str,               # last_updated_str
        shipping_region,                # shipping_region
        shipping_days,                  # shipping_days
        seller_id,                      # seller_id

        preset_val,                     # preset
        vendor_page,                    # vendor_page

        *imgs,                          # image_url1..10

        listing_head,                   # listing_head
        listing_detail,                 # listing_detail
    )

    with conn.cursor() as cur:
        cur.execute(UPSERT_VENDOR_ITEM_SQL, params)
        _ = cur.fetchall()
    # commit は呼び出し側でまとめて

def record_ebay_listing(listing_id: str, account_name: str, vendor_item_id: str, vendor_name: str):
    """eBayで発行された listing_id を trx.listings に記録（MERGE）。"""
    if not listing_id:
        return
    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
MERGE INTO [trx].[listings] AS tgt
USING (SELECT ? AS listing_id, ? AS account, ? AS vendor_item_id, ? AS vendor_name) AS src
ON (tgt.listing_id = src.listing_id OR (tgt.vendor_item_id = src.vendor_item_id AND src.vendor_item_id <> ''))
WHEN MATCHED THEN
    UPDATE SET
        tgt.account        = src.account,
        tgt.vendor_item_id = src.vendor_item_id,
        tgt.vendor_name    = src.vendor_name,
        tgt.start_time     = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT ([listing_id], [start_time], [account], [vendor_item_id], [vendor_name])
    VALUES (src.listing_id, SYSDATETIME(), src.account, src.vendor_item_id, src.vendor_name);
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
    """
    eBay出品前に、URLやメールアドレスなど
    「外部取引誘導とみなされそうな文字列」をざっくり除去する。
    """
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
    """書き込み回数に応じて commit。コミットしたらカウンタを0に戻す。"""
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

def collect_snapshot_items(conn, preset, mode,
                           low_usd_target, high_usd_target,
                           max_page=None, days=7):
    """
    出品候補となる vendor_item_id を順に yield する collector。
    """

    # USDレンジ → 仕入れ円レンジ
    min_cost_jpy, max_cost_jpy = calc_cost_range_from_usd_range(
        mode=mode,
        low_usd_target=low_usd_target,
        high_usd_target=high_usd_target,
    )

    # 7日内スキップ対象（SQL IN を組む）
    heads = list(HEADS_FOR_7DAY_SKIP)
    in_placeholders = ",".join(["?"] * len(heads)) if heads else "NULL"  # heads空対策

    # 共通WHERE（COUNTも本体も同じ条件にする）
    base_sql = f"""
FROM trx.vendor_item AS v
LEFT JOIN mst.seller AS s
  ON s.vendor_name = v.vendor_name
 AND s.seller_id   = v.seller_id
CROSS APPLY (
  SELECT interval_days =
    CASE
      WHEN s.seller_id IS NULL THEN NULL
      WHEN v.vendor_name = N'メルカリshops' THEN
        CASE
          WHEN s.rating_count >= 20 THEN 0
          WHEN s.rating_count >= 18 THEN 1
          WHEN s.rating_count >= 12 THEN 7
          WHEN s.rating_count >= 5  THEN 14
          ELSE 30
        END
      ELSE
        CASE
          WHEN s.rating_count >= 50 THEN 0
          WHEN s.rating_count >= 45 THEN 1
          WHEN s.rating_count >= 30 THEN 7
          WHEN s.rating_count >= 10 THEN 14
          ELSE 30
        END
    END
) AS ci

 WHERE v.preset = ?
  AND v.status = N'販売中'
  AND ISNULL(v.出品不可flg, 0) = 0

  -- ★ 配送条件NGは永久除外（このプログラムでは二度と拾わない）
  AND ISNULL(v.[出品状況], N'') <> N'配送条件NG'

  -- ★ 放置（古い更新 × 2〜5か月前 / 半年以上前）は除外
  AND NOT (
        ISNULL(v.[出品状況], N'') = N'古い更新'
    AND ISNULL(v.[出品状況詳細], N'') IN (
          N'2か月前',
          N'3か月前',
          N'4か月前',
          N'5か月前',
          N'半年以上前'
        )
  )
  -- mst.sellerが無い場合は必ず通す。
  -- mst.sellerがある場合は既存の条件を適用する。
  AND (
        s.seller_id IS NULL
        OR (
             ISNULL(s.is_ng, 0) = 0
             AND (
                   -- 優良セラー：常に通す
                   ci.interval_days = 0

                   -- それ以外：待ち期間が過ぎた時だけ通す（直近なら除外）
                   OR s.last_checked_at IS NULL
                   OR s.last_checked_at < DATEADD(DAY, -ci.interval_days, GETDATE())
                 )
           )
      )

  AND NOT EXISTS (
        SELECT 1
          FROM trx.listings AS l
         WHERE l.vendor_name    = v.vendor_name
           AND l.vendor_item_id = v.vendor_item_id
  )
    """

    params = [preset]

    # 7日内スキップ除外（headsがある時だけ）
    if heads:
        base_sql += f"""
         AND NOT (
              v.[出品状況] IN ({in_placeholders})
          AND v.last_ng_at IS NOT NULL
          AND v.last_ng_at >= DATEADD(DAY, -?, GETDATE())
         )
        """
        params.extend([*heads, days])

    # 価格条件
    if min_cost_jpy is not None and max_cost_jpy is not None:
        base_sql += " AND v.price BETWEEN ? AND ?\n"
        params.extend([min_cost_jpy, max_cost_jpy])
    elif min_cost_jpy is not None:
        base_sql += " AND v.price >= ?\n"
        params.append(min_cost_jpy)
    elif max_cost_jpy is not None:
        base_sql += " AND v.price <= ?\n"
        params.append(max_cost_jpy)

    # vendor_page 制限
    if max_page is not None:
        base_sql += " AND v.vendor_page BETWEEN 0 AND ?\n"
        params.append(max_page)

    # --- 件数だけ先に取得（条件完全一致） ---
    count_sql = "SELECT COUNT(1)\n" + base_sql

    # --- デバッグ用にCOUNT SQLを出力 ---
    debug_sql = debug_render_sql(count_sql, params)
    print("\n===== DEBUG SQL (COUNT) =====")
    print(debug_sql)
    print("================================\n")

    with conn.cursor() as cur:
        cur.execute(count_sql, params)
        total_count = int(cur.fetchone()[0] or 0)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE mst.presets
            SET snapshot_candidates_count = ?
            WHERE preset = ?
            """,
            (total_count, preset)
        )

    # --- 本体SELECT（streamでyield） ---
    select_sql = (
        "SELECT v.vendor_item_id, v.shipping_region, v.shipping_days\n" + base_sql +
        """
        ORDER BY
          CASE WHEN v.vendor_page IS NULL THEN 1 ELSE 0 END,
          v.vendor_page ASC;
        """
    )

    with conn.cursor() as cur:
        cur.execute(select_sql, params)
        rows = cur.fetchall()

    for r in rows:
        vendor_item_id = r[0]
        ship_region    = r[1]
        ship_days      = r[2]
        yield (vendor_item_id, ship_region, ship_days)

def _check_shipping_condition_values(region: Optional[str], days: Optional[str]) -> Tuple[bool, bool]:
    """
    shipping_region / shipping_days の値から配送NGかどうかを判定する共通ロジック。

    戻り値:
      (is_ng, has_info)
        - is_ng   : True = 配送条件NG, False = NGではない or 判定不能
        - has_info: True = region / days のどちらかに有効値があった
    """
    region = (region or "").strip()
    days   = (days or "").strip()

    # 両方とも空なら「判定材料なし」
    if not region and not days:
        return False, False

    bad_days = {"8〜14日で発送", "4〜7日で発送", "4~7日で発送"}

    if region == "海外":
        return True, True
    if days in bad_days:
        return True, True

    return False, True

def postprocess_common_title(jp_title: str, desc_jp: str, title_en: str) -> str:
    """
    ブランド共通の危険ワード・誤認ワード除去
    """
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
    """
    いまはブランド共通の安全側補正だけを行う。
    """
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

def seller_exists_in_mst(conn, vendor_name: str, seller_id: str) -> bool:
    sql = """
        SELECT 1
          FROM mst.seller WITH (NOLOCK)
         WHERE vendor_name = ? AND seller_id = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql, (vendor_name, seller_id))
        return cur.fetchone() is not None

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
    rating_count = rec.get("rating_count")  # int or None

    with conn.cursor() as cur:
        cur.execute(SQL_UPSERT_MST_SELLER, (vendor_name, seller_id, seller_name, rating_count))

def _truncate_for_db(s: str, max_len: int = 200) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\r\n", "\n")
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."



def heavy_check_detail(conn, driver, item_url, sku, preset, vendor_name,
                      p, debug_unavailable_dump, writes_since_commit):
    """
    方針:
      - 詳細scrapeを行い、NG/失敗なら trx.vendor_item を 1回 upsert して終わる
      - OKなら、出品に必要な情報（title_en/description_en 等）を rec に詰めて返す
        → 最終確定（出品/出品失敗）時に post_to_ebay 側で upsert 1回
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

        handle_listing_delete(conn, sku)
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0


    except Exception as e:
        # 解析失敗は last_ng_at を必ずクリア（upsert側CASEでNULL）
        rec_fail = {
            "vendor_name": vendor_name,
            "item_id": sku, 
            "listing_head": "解析失敗",
            "listing_detail": _truncate_for_db(str(e), 200),
        }   
        upsert_vendor_item(conn, rec_fail)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 0, 1


    # ★ メルカリ説明が空なら即NG
    if not (rec.get("description") or "").strip():
        rec["listing_head"] = "説明文なし"
        rec["listing_detail"] = "メルカリ商品説明が空"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # seller 必須
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

    # === 2) 最優先：配送条件NG（初回判定） ===
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

    # === 4.5) セラー判定（NG） =========================================
    seller_id = (rec.get("seller_id") or "").strip()
    rating_count = rec.get("rating_count")

    # 閾値
    threshold = 20 if vendor_name == "メルカリshops" else 50

    # rating_count が取れていない → 判定不能でNG
    if rating_count is None:
        rec["listing_head"] = "解析失敗"
        rec["listing_detail"] = "rating_countが取得できない"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 0, 1

    # mst.seller の is_ng を確認（DB側NGは即落とす）
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT is_ng
            FROM mst.seller
            WHERE vendor_name = ?
              AND seller_id = ?
            """,
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

    # 評価数が閾値未満 → NG（ただし再評価用に last_ng_at を打刻）
    if rating_count < threshold:
        rec["listing_head"] = "NG(セラー評価)"
        rec["listing_detail"] = f"rating_count={rating_count} < threshold={threshold}"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 4.9) 危険素材（エキゾチック等）判定 ===
    jp_title = (rec.get("title_jp") or "").strip()
    desc_jp = (rec.get("description") or "").strip()

    if contains_risky_word(jp_title, desc_jp):
        rec["listing_head"] = "NG(危険素材)"
        rec["listing_detail"] = "エキゾチック/危険素材キーワード検出"
        upsert_vendor_item(conn, rec)
        writes_since_commit += 1
        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
        return None, debug_unavailable_dump, writes_since_commit, 1, 0

    # === 5) 画像整形 ===
    imgs_ok = [
        u.strip().split("?")[0].split("#")[0]
        for u in (rec.get("images") or [])
        if isinstance(u, str) and u.strip().startswith("http")
    ][:12]

    # === 6) 翻訳/整形（OKルート：DB更新は確定時に1回） ===
    existing_en = fetch_existing_title_en(conn, vendor_name, sku)
    if existing_en:
        rec["title_en"] = clean_for_ebay(existing_en)
    else:
        expected_brand_en = p.get("default_brand_en")  # ★ mst.v_presets 由来の確定ブランド
        title_en_raw = translate_to_english(
            rec.get("title_jp") or "",
            rec.get("description") or "",
            expected_brand_en=expected_brand_en,  # ★ここが肝
        ) or ""

        if not title_en_raw.strip():
            rec["listing_head"] = "翻訳空返し"
            rec["listing_detail"] = ""
            upsert_vendor_item(conn, rec)  # ここで1回確定（出品不可）
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

    # description
    desc_jp = (rec.get("description") or "").strip()
    desc_en = ""
    if desc_jp:
        try:
            expected_brand_en = p.get("default_brand_en")  # ★同じく確定ブランド
            desc_en_raw = generate_ebay_description(
                rec.get("title_en") or "",
                desc_jp,
                expected_brand_en=expected_brand_en,  # ★ここが肝
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
        "imgs_ok": imgs_ok,
    }
    return heavy, debug_unavailable_dump, writes_since_commit, 0, 0

def post_to_ebay(conn, p, target_accounts, heavy,
                 acct_targets, acct_success, acct_policies_map,
                 total_listings, MAX_LISTINGS, stop_all,
                 writes_since_commit, BATCH_COMMIT):
    """
    - 出品結果（出品/出品失敗/出品停止等）を rec に入れて upsert_vendor_item で 1回確定
    """
    vendor_name = heavy["vendor_name"]
    sku = heavy["sku"]
    rec = heavy["rec"]
    start_price_usd = heavy["start_price_usd"]
    imgs_ok = heavy["imgs_ok"]

    fail_other_delta = 0

    for acct in target_accounts:
        t = acct_targets[acct]
        if t == 0:
            continue
        if t is not None and t <= 0:
            continue

        payload = {
            "CustomLabel": sku,
            "*Title": rec["title_en"],
            "*StartPrice": start_price_usd,
            "*Quantity": 1,
            "PicURL": "|".join(imgs_ok),
            "*Description": rec.get("description_en") or "",
            "category_id": p["category_id_ebay"],
            "C:Brand": p["default_brand_en"],
            "department": p["department"],
            "C:Color": "Multicolor",
            "C:Type": p["type_ebay"],
            "C:Country of Origin": "France",
        }

        try:
            item_id_ebay = post_one_item(payload, acct, acct_policies_map[acct])

            if item_id_ebay:
                print(f"✅ 出品成功: acct={acct} SKU={sku} listing_id={item_id_ebay}")
                record_ebay_listing(item_id_ebay, acct, sku, vendor_name)

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

            else:
                print(f"❌ 出品失敗(listing_id未返却): acct={acct} SKU={sku}")
                rec["listing_head"] = "出品失敗"
                rec["listing_detail"] = "listing_id未返却"
                upsert_vendor_item(conn, rec)
                writes_since_commit += 1
                writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

                fail_other_delta += 1

            break  # 元コード通り：他のアカウントには出品しない

        except ListingLimitError as e:
            print(f"🚫 出品停止(ListingLimit): acct={acct} SKU={sku} reason={e}")
            rec["listing_head"] = "出品停止(ListingLimit)"
            rec["listing_detail"] = str(e)
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, 1)

            fail_other_delta += 1
            acct_targets[acct] = 0
            continue

        except ApiHandledError as e:
            err_msg = str(e) or ""
            print(f"❌ 出品失敗(API): acct={acct} SKU={sku} reason={err_msg}")

            rec["listing_head"] = "出品失敗"
            rec["listing_detail"] = err_msg
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

            fail_other_delta += 1
            break

        except Exception as e:
            print(f"❌ 出品失敗(未分類): acct={acct} SKU={sku} reason={e}")
            rec["listing_head"] = "出品失敗(未分類)"
            rec["listing_detail"] = str(e)
            upsert_vendor_item(conn, rec)
            writes_since_commit += 1
            writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

            fail_other_delta += 1
            break

    return acct_targets, acct_success, total_listings, stop_all, writes_since_commit, fail_other_delta

def main():
    print("### publish_ebay.py 起動（preset_group → account → items） ###")
    start_time = datetime.now()

    conn = get_sql_server_connection()
    conn.autocommit = False
    driver = build_driver()

    writes_since_commit = 0
    skip_count = 0
    skip_detail_count = 0
    fail_other = 0

    MAX_LISTINGS = 10000
    total_listings = 0
    stop_all = False

    debug_unavailable_dump = 0
    DEBUG_UNAVAILABLE_DUMP_MAX = 5

    try:
        global TITLE_RULES
        TITLE_RULES = load_title_rules(conn)

        # ----- ebay_accounts をロードして group ごとのアカウント一覧を作る -----
        group_accounts_map: Dict[str, List[str]] = {}
        with conn.cursor() as cur:
            cur.execute("""
                SELECT account, preset_group
                FROM [nostock].[mst].[ebay_accounts]
                WHERE ISNULL(is_excluded, 0) = 0
            """)
            for acct, grp in cur.fetchall():
                grp = (grp or "").strip()
                acct = (acct or "").strip()
                if grp and acct:
                    group_accounts_map.setdefault(grp, []).append(acct)

        # ----- 各アカウントの post_target をロード -----
        acct_targets: Dict[str, Optional[int]] = {}
        with conn.cursor() as cur:
            cur.execute("""
                SELECT account,
                    CASE WHEN post_target = 0 THEN 0
                            WHEN post_target IS NULL THEN NULL
                            ELSE post_target END AS target
                FROM [nostock].[mst].[ebay_accounts]
                WHERE ISNULL(is_excluded, 0) = 0
            """)
            for acct, tgt in cur.fetchall():
                acct = (acct or "").strip()
                if acct:
                    acct_targets[acct] = tgt

        acct_success = {acct: 0 for acct in acct_targets.keys()}

        # ----- 各アカウントの policies を事前にロード -----
        acct_policies_map: Dict[str, Dict[str, str]] = {}
        with conn.cursor() as cur:
            for acct in acct_targets.keys():
                cur.execute("""
                    SELECT fulfillment_policy_id, payment_policy_id, return_policy_id
                    FROM [mst].[ebay_accounts]
                    WHERE LTRIM(RTRIM(account)) = LTRIM(RTRIM(?))
                """, (acct,))
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(f"mst.ebay_accounts にアカウントがありません: {acct}")
                acct_policies_map[acct] = {
                    "fulfillment_policy_id": str(row[0]),
                    "payment_policy_id": str(row[1]),
                    "return_policy_id": str(row[2]),
                    "merchant_location_key": "Default",
                }

        presets = fetch_active_presets(conn)

        # ===== preset_group をサマリー =====
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT LTRIM(RTRIM(preset_group)) AS preset_group
                FROM [nostock].[mst].[ebay_accounts]
                WHERE ISNULL(is_excluded, 0) = 0
                AND preset_group IS NOT NULL
                AND LTRIM(RTRIM(preset_group)) <> ''
                ORDER BY LTRIM(RTRIM(preset_group));
            """)
            preset_groups = [r[0] for r in cur.fetchall()]

        def has_quota(acct: str) -> bool:
            t = acct_targets[acct]
            if t is None:
                return True
            return t > 0

        # ===== ループ順：preset_group → account → items =====
        for preset_group in preset_groups:
            print(f"[DEBUG][GROUP] start preset_group={preset_group}")
            if stop_all:
                break

            target_accounts = group_accounts_map.get(preset_group, [])
            if not target_accounts:
                continue

            group_presets = [p for p in presets if (p.get("preset_group") or "").strip() == preset_group]
            if not group_presets:
                continue

            if not any(has_quota(a) for a in target_accounts):
                continue

            def iter_group_items():
                for p in group_presets:
                    for vendor_item_id, ship_region, ship_days in collect_snapshot_items(
                        conn,
                        p["preset"],
                        p["mode"],
                        p["low_usd_target"],
                        p["high_usd_target"],
                        p["max_page"],
                    ):
                        yield p, vendor_item_id, ship_region, ship_days

            items_it = iter_group_items()
            group_items_exhausted = False

            for acct in target_accounts:
                if stop_all:
                    break
                if not has_quota(acct):
                    continue

                print(
                    f"[DEBUG][ACCOUNT] preset_group={preset_group} "
                    f"account={acct} post_target={acct_targets[acct]}"
                )

                while has_quota(acct):
                    try:
                        p, vendor_item_id, ship_region, ship_days = next(items_it)
                    except StopIteration:
                        print(f"[INFO] preset_group={preset_group} items枯渇 → group終了")
                        group_items_exhausted = True
                        break

                    vendor_name = p["vendor_name"]
                    sku = vendor_item_id.strip()
                    preset = p["preset"]

                    if vendor_name == "メルカリshops":
                        item_url = f"https://mercari-shops.com/products/{sku}"
                    else:
                        item_url = f"https://jp.mercari.com/item/{sku}"

                    heavy, debug_unavailable_dump, writes_since_commit, d_skip_detail, d_fail = heavy_check_detail(
                        conn,
                        driver,
                        item_url,
                        sku,
                        preset,
                        vendor_name,
                        p,
                        debug_unavailable_dump,
                        writes_since_commit,
                    )

                    skip_detail_count += d_skip_detail
                    fail_other += d_fail
                    if heavy is None:
                        continue

                    acct_targets, acct_success, total_listings, stop_all, writes_since_commit, d_fail2 = post_to_ebay(
                        conn, p, [acct], heavy,
                        acct_targets, acct_success, acct_policies_map,
                        total_listings, MAX_LISTINGS, stop_all,
                        writes_since_commit, BATCH_COMMIT
                    )
                    fail_other += d_fail2

                    if stop_all:
                        break

                if group_items_exhausted:
                    break

        if writes_since_commit > 0:
            conn.commit()
        conn.autocommit = True

        end_time = datetime.now()
        elapsed = end_time - start_time

        try:
            subject = "✅ eBay出品処理 完了通知"
            lines = [f"{acct}: 成功 {acct_success.get(acct, 0)}" for acct in acct_success.keys()]
            body = (
                f"開始: {start_time}\n終了: {end_time}\n処理時間: {elapsed}\n"
                f"スキップ: {skip_count} / スキップ(詳細): {skip_detail_count} / 失敗: {fail_other}\n\n"
                + "\n".join(lines)
            )
            send_mail(subject, body)
        except Exception as e:
            print(f"[WARN] 完了メール送信失敗: {e}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
