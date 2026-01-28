# -*- coding: utf-8 -*-
# new_publish_ebay.py — listings / vendor_item 対応（Shops/通常 両対応・Python3.8/3.9互換）
# 依存：common/ebay_common.py（fetch_active_presets, make_search_url, iterate_search）

import concurrent.futures, sys, os, re, time, math, json, random
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

# === パス設定（common 配下を import 可能に） ===
_THIS_FILE = os.path.abspath(__file__)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_THIS_FILE))   # 例: D:\apps_nostock
_COMMON_DIR = os.path.join(_PROJECT_ROOT, "common")
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

# === サードパーティ ===
import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# === 共通ユーティリティ ===
import utils
from utils import get_sql_server_connection, compute_start_price_usd, send_mail, translate_to_english
from publish_ebay_adapter import post_one_item, ApiHandledError, ListingLimitError

# 共通化関数
from ebay_common import fetch_active_presets, make_search_url, iterate_search

# 画像スクレイピング系（iterate_search内で利用される想定・ここでは未直接使用）
from scrape_utils import (
    scroll_until_stagnant_collect_items,
    scroll_until_stagnant_collect_shops,
)

# ========= 固定値／運用設定 =========
IMG_LIMIT          = 10              # 画像の最大拾得枚数
TEST_MODE          = False           # テスト時 True / 本番 False

# eBayアカウント
ACCOUNTS_PLAN = [
    {"account": "谷川②", "post_target": 10},
    {"account": "谷川③", "post_target": 0},
]

SHIPPING_JPY       = 3000

# ===== eBay 側：カテゴリ・スペック（LV財布想定のデフォルト）=====
CATEGORY_ID       = "45258"          # eBay: Women > Women's Accessories > Wallets
DEPARTMENT        = "Women"          # Item Specifics
DEFAULT_BRAND_EN  = "Louis Vuitton"  # Item Specifics

# ========= 価格帯→メルカリ検索レンジ 補助 =========
def invert_cost_jpy(target_usd: float,
                    usd_jpy_rate: float,
                    profit_rate: float,
                    ebay_fee_rate: float,
                    domestic_shipping_jpy: int) -> float:
    """eBay開始価格(USD)から、許容仕入れ上限(JPY)を逆算する補助関数。"""
    denom = 1.0 - profit_rate - ebay_fee_rate
    if denom <= 0:
        raise ValueError("PROFIT_RATE + EBAY_FEE_RATE が 1.0 以上です。")
    return (target_usd * usd_jpy_rate) * denom - domestic_shipping_jpy

def calc_price_range_jpy(low_usd: float,
                         high_usd: float,
                         usd_jpy_rate: float,
                         profit_rate: float,
                         ebay_fee_rate: float,
                         domestic_shipping_jpy: int) -> Tuple[int, int]:
    """目標USDレンジからメルカリ検索用の JPYレンジ(min,max) を算出。"""
    low_jpy  = invert_cost_jpy(low_usd,  usd_jpy_rate, profit_rate, ebay_fee_rate, domestic_shipping_jpy)
    high_jpy = invert_cost_jpy(high_usd, usd_jpy_rate, profit_rate, ebay_fee_rate, domestic_shipping_jpy)
    price_min = max(0, int(math.floor(low_jpy)))
    price_max = max(price_min, int(math.ceil(high_jpy)))
    return price_min, price_max

# ========= WebDriver =========
def build_driver():
    """Selenium ChromeDriver を headless/eager で起動し、安定化オプションを付与。"""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--log-level=3")
    opts.add_argument("--lang=ja-JP,ja")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119 Safari/537.36")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.page_load_strategy = "eager"

    service = Service()
    driver = webdriver.Chrome(service=service, options=opts)

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
        )
    except Exception:
        pass

    driver.set_window_size(1400, 1000)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    try:
        driver.command_executor.set_timeout(180)
    except Exception:
        pass
    return driver

# ========= デバッグ補助 =========
def _close_any_modal(driver):
    """同意/閉じる系のボタンがあれば雑に閉じる。"""
    try:
        js = """
          return Array.from(document.querySelectorAll('button, [role=button]')).find(b=>{
            const t=(b.innerText||'').trim();
            return ['同意','閉じる','許可しない','OK','Accept','Close'].some(k=>t.includes(k));
          });
        """
        btn = driver.execute_script(js)
        if btn:
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.3)
    except Exception:
        pass

def _try_extract_title(driver, vis_timeout=12.0):
    """通常メルカリの詳細ページからタイトルを堅牢に抜く。"""
    try:
        WebDriverWait(driver, vis_timeout).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "#item-info"))
        )
    except TimeoutException:
        pass

    sels: List[Tuple[str, str]] = [
        (By.XPATH, '//*[@id="item-info"]//h1'),
        (By.CSS_SELECTOR, '#item-info h1'),
        (By.CSS_SELECTOR, '[data-testid="item-name"]'),
        (By.CSS_SELECTOR, '[data-testid*="title"]'),
        (By.CSS_SELECTOR, 'h1[role="heading"]'),
        (By.CSS_SELECTOR, 'h1'),
    ]
    for by, sel in sels:
        try:
            el = WebDriverWait(driver, 3).until(EC.visibility_of_element_located((by, sel)))
            t = (el.text or "").strip()
            if t:
                return t
        except Exception:
            continue

    try:
        og = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
        t = (og.get_attribute("content") or "").strip()
        if t:
            return t
    except Exception:
        pass

    try:
        t = (driver.title or "").strip()
        if t:
            return t
    except Exception:
        pass
    return None

def _find_seller_info(driver, max_retry=3, wait_sec=0.8):
    """通常メルカリの詳細からセラーID/名前/評価数を抽出。"""
    selectors = [
        "#item-info a[href^='/user/profile/']",
        "a[href^='/user/profile/']",
        "[data-testid='seller-info'] a[href*='/user/']",
        "a[href*='/user/']",
    ]
    for _ in range(max_retry):
        for sel in selectors:
            try:
                el = WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, sel)))
                href = (el.get_attribute("href") or "").strip()
                if not href:
                    continue
                seller_id = href.rstrip("/").split("/")[-1]
                seller_name = (el.text or "").strip().splitlines()[0]

                rating_count = 0
                try:
                    near = ((el.get_attribute("outerText") or "") + " " + (el.get_attribute("innerText") or "")).strip()
                    m = re.search(r"(評価|reviews?|件)\D*?(\d{1,6})", near, flags=re.IGNORECASE)
                    if m:
                        rating_count = int(m.group(2))
                    else:
                        m2 = re.search(r"\d{1,6}", near)
                        if m2:
                            rating_count = int(m2.group())
                except Exception:
                    pass
                return seller_id, seller_name, rating_count
            except Exception:
                continue
        time.sleep(wait_sec)
    return None

# ========= Shops向けセラー抽出・画像収集 =========
def _parse_shops_seller_text(text: str) -> Tuple[str, int]:
    """Shopsのまとまりテキストから（店名, 評価数）を分解。"""
    t = (text or "").strip()
    t = re.sub(r"\s+", " ", t.replace("\u3000", " "))
    t = re.sub(r"\s*メルカリ\s*Shops\s*$", "", t, flags=re.IGNORECASE)
    m = re.search(r"(\d[\d,]*)\s*$", t)
    if m:
        rating = int(m.group(1).replace(",", ""))
        name = t[:m.start()].strip()
    else:
        rating = 0
        name = t.strip()
    return name, rating

def _extract_shops_seller(driver) -> Tuple[str, str, int]:
    """ShopsのセラーID/名前/評価数を（近傍要素→フォールバック）で取得。"""
    a = WebDriverWait(driver, 6).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, 'a[data-testid="shops-profile-link"]'))
    )
    href = (a.get_attribute("href") or "").strip()
    seller_id = href.rstrip("/").split("/")[-1] if href else ""
    try:
        root = a.find_element(By.XPATH, 'ancestor::div[contains(@class,"merUserObject") or contains(@data-testid,"shop")]')
    except Exception:
        root = a
    rating_count = 0
    try:
        rating_el = root.find_element(By.XPATH, './/span[starts-with(@class,"count_") or @data-testid="rating-count"]')
        txt = (rating_el.text or "").strip()
        if txt:
            rating_count = int(re.sub(r"[^\d]", "", txt))
    except Exception:
        pass
    block_text = (root.text or "").strip()
    name_from_block, rating_from_block = _parse_shops_seller_text(block_text)
    seller_name = name_from_block or (a.text or "").strip()
    if rating_count == 0:
        rating_count = rating_from_block
    return seller_id, seller_name, rating_count

def collect_images_shops(driver, limit: int = 10) -> List[Optional[str]]:
    """Shops詳細の画像URLを順番に取得（クリック不要）。不足分は None で埋める。"""
    urls: List[str] = []
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, '.slick-slide img[src]')
        for el in elements:
            src = (el.get_attribute("src") or "").strip()
            if src and src not in urls:
                urls.append(src)
            if len(urls) >= limit:
                break
    except Exception:
        pass
    urls = urls[:limit]
    out: List[Optional[str]] = list(urls)
    while len(out) < limit:
        out.append(None)
    return out

# ========= 詳細解析（Shops / 通常） =========
def parse_detail_shops(driver, url: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """メルカリShopsの商品詳細を解析し、必要最低限の情報を返す（簡易版）。"""
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.3 + random.uniform(0.0, 0.2))

    for _ in range(2):
        _close_any_modal(driver)
        time.sleep(0.15)

    # --- タイトル ---
    try:
        title_el = driver.find_element(By.CSS_SELECTOR, '[data-testid="product-title-section"] h1')
        title = (title_el.text or "").strip()
    except Exception:
        title = ""

    # --- 価格 ---
    price = 0
    try:
        box = driver.find_element(By.CSS_SELECTOR, '[data-testid="product-price"]')
        m = re.search(r'(\d[\d,]*)', (box.text or ""))
        if m:
            price = int(m.group(1).replace(",", ""))
    except Exception:
        pass

    # --- 更新日 ---
    try:
        dt_el = driver.find_element(By.CSS_SELECTOR, '#product-info > section:nth-child(2) > p')
        last_updated_str = (dt_el.text or "").strip()
    except Exception:
        last_updated_str = ""

    # --- セラー ---
    try:
        seller_id, seller_name, rating_count = _extract_shops_seller(driver)
    except Exception:
        seller_id, seller_name, rating_count = "", "", 0

    # --- 画像 ---
    images = collect_images_shops(driver, limit=IMG_LIMIT)

    return {
        "vendor_name": vendor_name,
        "item_id": url.rstrip("/").split("/")[-1],
        "title_jp": title,
        "title_en": "",
        "price": price,
        "last_updated_str": last_updated_str,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "rating_count": rating_count,
        "images": images,
        "preset": preset,
    }


def parse_detail_personal(driver, url: str, preset: str, vendor_name: str) -> Dict[str, Any]:
    """通常メルカリの商品詳細を解析し、必要最低限の情報を返す（簡易版）。"""
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    time.sleep(0.5 + random.uniform(0.0, 0.3))

    for _ in range(2):
        _close_any_modal(driver)
        time.sleep(0.15)

    # --- タイトル ---
    try:
        title = _try_extract_title(driver, vis_timeout=10.0)
    except Exception:
        title = ""

    # --- 価格 ---
    price = 0
    try:
        element = driver.find_element(By.CSS_SELECTOR, '[data-testid*="price"]')
        price_text = (element.text or "").strip()
        price = int(re.sub(r"[^\d]", "", price_text))
    except Exception:
        pass

    # --- 更新日 ---
    try:
        sec = driver.find_element(By.CSS_SELECTOR, '#item-info section')
        last_updated_str = (sec.text or "").splitlines()[-1].strip()
    except Exception:
        last_updated_str = ""

    # --- セラー ---
    try:
        seller_id, seller_name, rating_count = _find_seller_info(driver)
    except Exception:
        seller_id, seller_name, rating_count = "", "", 0

    # --- 画像 ---
    images = []
    for img in driver.find_elements(By.CSS_SELECTOR, "article img[src], article source[srcset]"):
        src = None
        try:
            if img.tag_name.lower() == "img":
                src = img.get_attribute("src")
            else:
                srcset = img.get_attribute("srcset") or ""
                parts = [p.strip().split(" ")[0] for p in srcset.split(",") if p.strip()]
                if parts:
                    src = parts[-1]
        except Exception:
            pass
        if src:
            images.append(src)
        if len(images) >= IMG_LIMIT:
            break
    while len(images) < IMG_LIMIT:
        images.append(None)

    return {
        "vendor_name": vendor_name,
        "item_id": url.rstrip("/").split("/")[-1],
        "title_jp": title,
        "title_en": "",
        "price": price,
        "last_updated_str": last_updated_str,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "rating_count": rating_count,
        "images": images,
        "preset": preset,
    }

# ========= DB I/O =========
UPSERT_VENDOR_ITEM_SQL = """
MERGE INTO [trx].[vendor_item] AS tgt
USING (
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
) AS src (
    vendor_name, vendor_item_id, title_jp, title_en, price, last_updated_str, seller_id,
    preset,
    image_url1, image_url2, image_url3, image_url4, image_url5,
    image_url6, image_url7, image_url8, image_url9, image_url10
)
ON (tgt.vendor_name = src.vendor_name AND tgt.vendor_item_id = src.vendor_item_id)
WHEN MATCHED THEN
    UPDATE SET
        title_jp         = src.title_jp,
        title_en         = src.title_en,
        last_updated_str = src.last_updated_str,
        image_url1       = src.image_url1,
        image_url2       = src.image_url2,
        image_url3       = src.image_url3,
        image_url4       = src.image_url4,
        image_url5       = src.image_url5,
        image_url6       = src.image_url6,
        image_url7       = src.image_url7,
        image_url8       = src.image_url8,
        image_url9       = src.image_url9,
        image_url10      = src.image_url10,
        prev_price       = CASE WHEN tgt.price <> src.price THEN tgt.price ELSE tgt.prev_price END,
        price            = src.price,
        status           = N'販売中',
        preset           = src.preset,
        last_checked_at  = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        vendor_name, vendor_item_id, title_jp, title_en, price, last_updated_str, seller_id,
        preset,
        image_url1, image_url2, image_url3, image_url4, image_url5,
        image_url6, image_url7, image_url8, image_url9, image_url10,
        created_at, last_checked_at, prev_price, status
    )
    VALUES (
        src.vendor_name, src.vendor_item_id, src.title_jp, src.title_en, src.price, src.last_updated_str, src.seller_id,
        src.preset,
        src.image_url1, src.image_url2, src.image_url3, src.image_url4, src.image_url5,
        src.image_url6, src.image_url7, src.image_url8, src.image_url9, src.image_url10,
        SYSDATETIME(), SYSDATETIME(), NULL, N'販売中'
    )
OUTPUT
    $action                 AS action,
    inserted.vendor_item_id AS vendor_item_id,
    deleted.price           AS old_price,
    inserted.price          AS new_price,
    inserted.status         AS status;
"""

def upsert_vendor_item(conn, rec: Dict[str, Any]):
    """1件の vendor_item を MERGE（画像10本まで・prev_price更新含む）。"""
    imgs = (rec.get("images") or [])
    imgs = (imgs + [None]*10)[:10]
    preset_val = rec.get("preset") or ""
    params = (
        rec["vendor_name"], rec["item_id"], rec["title_jp"], rec.get("title_en",""),
        rec["price"], rec["last_updated_str"], rec["seller_id"],
        preset_val,
        *imgs
    )
    with conn.cursor() as cur:
        cur.execute(UPSERT_VENDOR_ITEM_SQL, params)
        _ = cur.fetchall()
    conn.commit()

UPSERT_SELLER_SQL = (
    "MERGE INTO [mst].[seller] AS tgt "
    "USING (VALUES (?, ?, ?, ?)) AS src ( "
    "  seller_id, vendor_name, seller_name, rating_count "
    ") "
    "ON (tgt.vendor_name = src.vendor_name AND tgt.seller_id = src.seller_id) "
    "WHEN MATCHED THEN "
    "  UPDATE SET seller_name = src.seller_name, rating_count = src.rating_count, last_checked_at = SYSDATETIME() "
    "WHEN NOT MATCHED THEN "
    "  INSERT (seller_id, vendor_name, seller_name, rating_count, last_checked_at) "
    "  VALUES (src.seller_id, src.vendor_name, src.seller_name, src.rating_count, SYSDATETIME());"
)

def upsert_seller(conn, vendor_name: str, seller_id: str, seller_name: str, rating_count: int):
    """mst.seller をセラーID単位でUPSERT（評価数も更新）。"""
    with conn.cursor() as cur:
        cur.execute(UPSERT_SELLER_SQL, (seller_id, vendor_name, seller_name, rating_count))
    conn.commit()

def is_already_listed(conn, vendor_name: str, sku: str) -> bool:
    vendor_item_id = (sku or "").strip()
    vendor = (vendor_name or "").strip()
    if not vendor_item_id or not vendor:
        return False
    sql = """
        SELECT TOP 1 1
          FROM [trx].[listings] WITH (NOLOCK)
         WHERE vendor_name = ? AND vendor_item_id = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql, (vendor, vendor_item_id))
        return cur.fetchone() is not None

def is_blocked_item(conn, vendor_name: str, vendor_item_id: str) -> bool:
    """仕入れNGなどのフラグ（trx.vendor_item.出品不可flg）が立っているかの判定。"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ISNULL(出品不可flg, 0)
              FROM trx.vendor_item WITH (NOLOCK)
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (vendor_name, vendor_item_id))
        row = cur.fetchone()
    return bool(row and row[0])

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

# 出品状況（vendor_item）書き戻し（見出し・詳細）
UPSERT_LISTING_STATUS_SQL = """
MERGE INTO [trx].[vendor_item] AS tgt
USING (SELECT ? AS vendor_name, ? AS vendor_item_id) AS src
ON (tgt.vendor_name = src.vendor_name AND tgt.vendor_item_id = src.vendor_item_id)
WHEN MATCHED THEN
    UPDATE SET
        [出品日]        = CAST(SYSDATETIME() AS date),
        vendor_page     = ?,
        [出品状況]      = ?,
        [出品状況詳細]  = ?,
        last_checked_at = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (vendor_name, vendor_item_id, [出品日], vendor_page, [出品状況], [出品状況詳細],
            created_at, last_checked_at, status)
    VALUES (src.vendor_name, src.vendor_item_id, CAST(SYSDATETIME() AS date), ?, ?, ?,
            SYSDATETIME(), SYSDATETIME(), N'販売中');
"""

def _truncate_for_db(s: str, limit: int) -> str:
    """DBカラム長に合わせて省略（末尾に'…'）。"""
    s = (s or "").strip()
    return s if len(s) <= limit else s[:max(0, limit-1)] + "…"

def mark_listing_status_head_detail(conn,
                                    vendor_name: str,
                                    vendor_item_id: str,
                                    vendor_page: Optional[int],
                                    status_head: str,
                                    status_detail: str = ""):
    """vendor_item に出品状況の見出し・詳細を保存（MERGE）。"""
    vpage: Optional[int] = int(vendor_page) if vendor_page is not None else None
    head   = _truncate_for_db(status_head,   100)
    detail = _truncate_for_db(status_detail, 255)
    with conn.cursor() as cur:
        cur.execute(UPSERT_LISTING_STATUS_SQL,
                    (vendor_name, vendor_item_id, vpage, head, detail,
                                     vpage, head, detail))
    conn.commit()

# タイトル置換ルール（大文字小文字無視のリテラル）
TITLE_RULES: List[Tuple[str, str]] = []

def load_title_rules(conn) -> List[Tuple[str, str]]:
    """mst.title_replace_rules から (pattern, replacement) を順に読込。"""
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

def _replace_literal_ignorecase(text: str, old: str, new: str) -> str:
    """正規表現を使わない大小無視のリテラル置換。"""
    if not text or not old:
        return text or ""
    pattern = re.compile(re.escape(old), flags=re.IGNORECASE)
    return pattern.sub(new, text)

def apply_title_rules_literal_ci(title_en: str, rules: List[Tuple[str, str]]) -> str:
    """置換ルールを順適用し、空白を整形。"""
    s = title_en or ""
    for pat, rep in rules:
        s = _replace_literal_ignorecase(s, pat, rep)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def shipping_usd_from_jpy(jpy: int, usd_jpy_rate: float) -> str:
    """JPY送料をUSD文字列へ変換（小数2桁）。"""
    usd = (Decimal(jpy) / Decimal(str(usd_jpy_rate))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{usd:.2f}"

def smart_truncate80(s: str) -> str:
    """英題を80文字にスマート切詰め（単語途中で切らない）。"""
    s = (s or "").strip()
    if len(s) <= 80:
        return s
    cut = s[:77]
    if " " in cut and not cut.endswith(" "):
        cut = cut[: cut.rfind(" ")]
    return cut.rstrip() + "..."

def fetch_existing_title_en(conn, vendor_name: str, vendor_item_id: str) -> Optional[str]:
    """既存の英題（title_en）を vendor_item から取得。なければ None。"""
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

def get_seller_gate(conn, vendor_name: str, seller_id: str) -> Tuple[bool, int]:
    """セラー評価閾値（Shops=20 / 通常=50）を満たすか判定。"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(rating_count,0)
              FROM [mst].[seller]
             WHERE vendor_name = ? AND seller_id = ?
        """, (vendor_name, seller_id))
        row = cur.fetchone()
    rating = int(row[0]) if row else 0
    min_threshold = 20 if vendor_name == "メルカリshops" else 50
    return (rating >= min_threshold, rating)

# === 追加：検索結果ページからIDを集める（Shops/通常 切替） ===
def collect_non_pr_urls(driver, base_search_url: str, preset: str):
    page_idx = 0
    seen = set()
    while True:
        url = base_search_url if page_idx == 0 else f"{base_search_url}&page_token=v1%3A{page_idx}"
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

        # 通常メルカリ（個人）用：スクロールしてカードを取り切る
        items = scroll_until_stagnant_collect_items(driver, pause=0.45, stagnant_times=3)
        item_ids = [it[0] for it in items if it]
        new_ids = [iid for iid in item_ids if iid not in seen]

        for iid in new_ids:
            seen.add(iid)
            yield page_idx, iid, f"https://jp.mercari.com/item/{iid}", preset

        if not new_ids:
            return
        page_idx += 1

def collect_shops_urls(driver, base_search_url: str, preset: str):
    page_idx = 0
    seen = set()
    while True:
        url = base_search_url if page_idx == 0 else f"{base_search_url}&page_token=v1%3A{page_idx}"
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

        # メルカリShops用：shopsカードを取り切る
        items = scroll_until_stagnant_collect_shops(driver, pause=0.45, stagnant_times=3)  # [(product_id, title, price), ...]
        product_ids = [it[0] for it in items if it]
        new_ids = [pid for pid in product_ids if pid not in seen]

        for pid in new_ids:
            seen.add(pid)
            yield page_idx, pid, f"https://jp.mercari.com/shops/product/{pid}", preset

        if not new_ids:
            return
        page_idx += 1

def _looks_like_ebay_ok_image(url: str) -> bool:
    if not url: 
        return False
    u = url.strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        return False
    # 許容拡張子（クエリはOK。path部分だけ見る）
    from urllib.parse import urlparse
    path = urlparse(u).path.lower()
    return path.endswith((".jpg", ".jpeg", ".png", ".gif"))

def sanitize_mercari_images(urls: list[str], limit: int = 12) -> list[str]:
    """メルカリ画像のシンプルフィルター"""
    base = "https://static.mercdn.net/item/detail/orig/photos/"
    allow_ext = (".jpg", ".jpeg", ".png", ".gif")

    clean = []
    seen = set()

    for u in urls:
        if not isinstance(u, str):
            continue
        u = u.strip()
        # 1) item/detail/orig/photos/ で始まるものだけ
        if not u.startswith(base):
            continue
        # 2) 拡張子チェック（クエリの前で判定）
        path = u.split("?")[0].split("#")[0]
        if not path.lower().endswith(allow_ext):
            continue
        # 3) クエリを除去
        u = path
        # 4) 重複スキップ
        if u in seen:
            continue
        seen.add(u)
        clean.append(u)
        if len(clean) >= limit:
            break

    return clean

# ========= メイン =========
def main():
    """プリセット外側→アカウント内側。presetを順に消化してアカウントのquotaを満たしていく方式。"""
    start_time = datetime.now()
    conn = get_sql_server_connection()
    driver = build_driver()

    global TITLE_RULES
    TITLE_RULES = load_title_rules(conn)

    # アカウントごとの残り枠
    acct_targets = {plan["account"]: int(plan.get("post_target", 0)) or 0 for plan in ACCOUNTS_PLAN}
    acct_success = {plan["account"]: 0 for plan in ACCOUNTS_PLAN}
    skip_count = 0
    fail_other = 0

    try:
        # 1) プリセットは最初に1回だけフェッチ
        presets = fetch_active_presets(conn)

        # 2) 各アカウントのポリシーを先に読み込んでおく
        acct_policies_map: Dict[str, Dict[str, str]] = {}
        with conn:
            with conn.cursor() as cur:
                for plan in ACCOUNTS_PLAN:
                    acct = plan["account"]
                    cur.execute("""
                        SELECT fulfillment_policy_id, payment_policy_id, return_policy_id
                          FROM [mst].[ebay_accounts]
                         WHERE LTRIM(RTRIM(account)) = LTRIM(RTRIM(?))
                    """, (acct,))
                    row = cur.fetchone()
                    if not row:
                        raise RuntimeError(f"[mst].[ebay_accounts] にアカウントが見つかりません: {acct}")
                    acct_policies_map[acct] = {
                        "fulfillment_policy_id": str(row[0]),
                        "payment_policy_id": str(row[1]),
                        "return_policy_id": str(row[2]),
                        "merchant_location_key": "Default",
                    }

        ship_usd = shipping_usd_from_jpy(SHIPPING_JPY, utils.USD_JPY_RATE)
        print(f"🚚 送料(参考): {SHIPPING_JPY} JPY ≒ {ship_usd} USD")

        # 3) プリセットを外側に回す
        for p in presets:
            # すでに全アカウントが満了なら抜ける
            if all(rem <= 0 for rem in acct_targets.values()):
                break

            preset      = p["preset"]
            vendor_name = p["vendor_name"]
            url = make_search_url(
                vendor_name=vendor_name,
                brand_id=p["brand_id"],
                category_id=p["category_id"],
                status="on_sale",
            )
            print(f"\n====================")
            print(f"▶ preset={preset} vendor={vendor_name}")
            print(f"🔍 search URL: {url}")
            print(f"====================")

            collector = collect_shops_urls if vendor_name == "メルカリshops" else collect_non_pr_urls

            # このプリセットで拾える商品を順に取り出す
            for page_idx, item_id, _item_url, _preset in collector(driver, url, preset):
                # 全アカウント満了なら抜ける
                if all(rem <= 0 for rem in acct_targets.values()):
                    break

                item_url = (
                    f"https://jp.mercari.com/shops/product/{item_id}"
                    if vendor_name == "メルカリshops"
                    else f"https://jp.mercari.com/item/{item_id}"
                )
                sku = (item_id or "").strip()

                # 既存出品/出品不可チェックは共通
                if is_already_listed(conn, vendor_name, sku):
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "既存出品あり", "")
                    skip_count += 1
                    continue
                if is_blocked_item(conn, vendor_name, sku):
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "出品不可フラグ", "")
                    skip_count += 1
                    continue

                # 詳細解析（共通）
                try:
                    if vendor_name == "メルカリshops":
                        rec = parse_detail_shops(driver, item_url, preset, vendor_name)
                    else:
                        rec = parse_detail_personal(driver, item_url, preset, vendor_name)

                    existing_en = fetch_existing_title_en(conn, vendor_name, sku)
                    if existing_en:
                        rec["title_en"] = existing_en
                    else:
                        title_en_raw = translate_to_english(rec["title_jp"]) or ""
                        if not title_en_raw.strip():
                            mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "翻訳空返し", "")
                            skip_count += 1
                            continue
                        rec["title_en"] = smart_truncate80(
                            apply_title_rules_literal_ci(title_en_raw, TITLE_RULES)
                        )
                except RuntimeError as e:
                    if "Translation failed" in str(e):
                        print(f"ABEND: {e}")
                        try: driver.quit()
                        except Exception: pass
                        try: conn.close()
                        except Exception: pass
                        sys.exit(2)
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "解析失敗(Runtime)", str(e))
                    skip_count += 1
                    continue
                except Exception as e:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "解析失敗", str(e))
                    skip_count += 1
                    continue

                # 翻訳・古い更新・DB記録・価格計算・セラー閾値は共通
                title_en_raw = (rec.get("title_en") or "").strip()
                if not title_en_raw:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "翻訳空返し", "")
                    skip_count += 1
                    continue
                if re.search(r'(半年以上前|\d+\s*[ヶか]月前|数\s*[ヶか]月前)', rec.get("last_updated_str") or ""):
                    head = "古い更新"; detail = rec.get("last_updated_str") or ""
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, head, detail)
                    skip_count += 1
                    continue

                upsert_seller(conn, vendor_name, rec["seller_id"], rec["seller_name"], rec["rating_count"])
                upsert_vendor_item(conn, rec)

                start_price_usd = compute_start_price_usd(rec["price"])
                if not start_price_usd:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "計算価格が範囲外",
                                                    f"{utils.LOW_USD_TARGET}–{utils.HIGH_USD_TARGET}USD")
                    skip_count += 1
                    continue

                is_ok, rating = get_seller_gate(conn, vendor_name, rec["seller_id"])
                if not is_ok:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "セラー条件未達", f"rating={rating}")
                    skip_count += 1
                    continue

                imgs = [u for u in rec["images"] if u][:12]
                # 4) この商品を「まだ枠が残っている最初のアカウント」に割当
                for plan in ACCOUNTS_PLAN:
                    acct = plan["account"]
                    if acct_targets[acct] <= 0:
                        continue
                    imgs_ok = sanitize_mercari_images(imgs)
                    # フィルター後にPicURLを作る
                    payload: Dict[str, Any] = {
                        "CustomLabel": str(rec["item_id"]).strip(),
                        "*Title": rec.get("title_en") or "",
                        "*StartPrice": start_price_usd,
                        "*Quantity": 1,
                        "PicURL": "|".join(imgs_ok),  # ← フィルター済みの画像だけを送る
                        "*Description": (
                            f"{rec.get('title_en') or ''}\n\n"
                            "Please contact us via eBay messages for details.\n"
                            "Ships from Japan with tracking."
                        ),
                        "category_id": CATEGORY_ID,
                        "C:Brand": DEFAULT_BRAND_EN,
                        "C:Color": "Multicolor",
                        "C:Type": "Wallet",
                        "department": DEPARTMENT,
                        "C:Country of Origin": "France",
                    }

                    print(f"🛒 出品リクエスト送信: acct={acct}  SKU={payload['CustomLabel']}  "
                          f"Title='{payload['*Title']}'  Price(USD)={payload['*StartPrice']}")

                    try:
                        if TEST_MODE:
                            print("\n=== 📦 出品テストモード（eBay未送信） ===")
                            print(json.dumps(payload, indent=2, ensure_ascii=False))
                            # テスト時も割当はカウントしない方が安全。必要ならここで +1 してもOK
                            break

                        item_id_ebay = post_one_item(payload, acct, acct_policies_map[acct])
                        if item_id_ebay:
                            print(f"✅ 出品完了 listing_id={item_id_ebay}")
                            record_ebay_listing(item_id_ebay, acct, payload["CustomLabel"], vendor_name)
                            mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "出品", "")
                            acct_success[acct] += 1
                            acct_targets[acct] -= 1
                        else:
                            head = "出品失敗"; detail = "listing_id未返却"
                            print(f"❌ {head}: {detail}")
                            mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, head, detail)
                            fail_other += 1
                        break  # この商品は1アカウントにだけ割当。次の商品へ。

                    except (ListingLimitError, ApiHandledError) as e:
                        head = "出品失敗"; detail = str(e)
                        print(f"❌ {head}: {detail}")
                        mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, head, detail)
                        fail_other += 1
                        # 次のアカウントに同商品を回す？→通常は回さず break。回したいなら continue に変更可。
                        break
                    except Exception as e:
                        head = "出品失敗(未分類)"; detail = str(e)
                        print(f"❌ {head}: {detail}")
                        mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, head, detail)
                        fail_other += 1
                        break  # 同上

        # 完了通知
        end_time = datetime.now()
        elapsed = end_time - start_time
        subject = "✅ eBay出品処理 完了通知（preset優先）"
        lines = [f"{acct}: 成功 {acct_success[acct]} / 目標 {acct_success[acct] + acct_targets[acct]}" for acct in acct_success]
        body = (
            f"開始時刻: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"終了時刻: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"処理時間: {elapsed}\n"
            f"スキップ: {skip_count} / 失敗: {fail_other}\n"
            "内訳:\n" + "\n".join(lines) + "\n"
            f"実行スクリプト:\n{Path(__file__).name}\n"
        )
        try:
            send_mail(subject, body)
        except Exception as e:
            print(f"[WARN] 完了メール送信に失敗: {e}")
        print("=== 🎉 全ての処理が完了しました（preset優先） ===")

    finally:
        try: driver.quit()
        except Exception: pass
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    main()
