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
from deep_translator import GoogleTranslator

# === 共通ユーティリティ ===
import utils
from utils import get_sql_server_connection, compute_start_price_usd, send_mail
from publish_ebay_adapter import post_one_item, ApiHandledError, ListingLimitError

# ★ ここが今回のポイント：共通化関数を使用
from ebay_common import fetch_active_presets, make_search_url, iterate_search

# 画像スクレイピングで共通関数を利用
from scrape_utils import (
    scroll_until_stagnant_collect_items,   # iterate_search内部で利用
    scroll_until_stagnant_collect_shops,   # iterate_search内部で利用（shopsカード収集）
)

# ========= 固定値／運用設定 =========
VENDOR_NAME        = "メルカリ"     # 既存互換のためのデフォルト（Shops時は都度上書き）
IMG_LIMIT          = 10             # 画像の最大拾得枚数
POST_TARGET        = 200            # 出品できたら打ち切る目標件数
TEST_MODE          = False          # テスト時 True / 本番 False

# eBayアカウント
EBAY_ACCOUNT       = "谷川②"
SHIPPING_JPY       = 3000

# ===== eBay 側：カテゴリ・スペック（LV財布運用前提のデフォルト）=====
CATEGORY_ID       = "45258"          # eBay: Women > Women's Accessories > Wallets
DEPARTMENT        = "Women"          # Item Specifics
DEFAULT_BRAND_EN  = "Louis Vuitton"  # Item Specifics

# ========= 価格帯→メルカリ検索レンジ（計算の補助：必要なら利用） =========
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

# ========= 翻訳（強制タイムアウト＆リトライ） =========
def translate_to_english(
    text_jp: str,
    per_attempt_timeout: float = 8.0,
    attempts: int = 3,
    backoff_base: float = 1.0
) -> str:
    """deep_translator をタイムアウト＋リトライ付きで呼ぶ。空返しは呼び出し側でハンドリング。"""
    if not text_jp:
        return ""
    last_err = None

    def _call():
        return GoogleTranslator(source='ja', target='en').translate(text_jp) or ""

    for i in range(1, attempts + 1):
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_call)
                out = fut.result(timeout=per_attempt_timeout)
            time.sleep(0.4 + random.uniform(0, 0.4))
            return out
        except concurrent.futures.TimeoutError as te:
            last_err = te
            wait = backoff_base * (2 ** (i - 1))
            print(f"[translate] timeout ({i}/{attempts}) retry in {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            last_err = e
            wait = backoff_base * (2 ** (i - 1))
            print(f"[translate] error ({i}/{attempts}): {e!r} retry in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(
        f"Translation failed after {attempts} attempts "
        f"(per_attempt_timeout={per_attempt_timeout}s). Last error: {last_err!r}"
    )

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
DEBUG_SAVE_FAILURES = True
def _save_debug(driver, prefix="debug_title_fail"):
    """失敗時にスクショ/HTMLを保存（トラブル解析用）。"""
    if not DEBUG_SAVE_FAILURES:
        return
    try:
        ts = time.strftime("%Y%m%d_%H%M%S")
        driver.save_screenshot(f"{prefix}_{ts}.png")
        with open(f"{prefix}_{ts}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print(f"[debug] saved {prefix}_{ts}.png / .html")
    except Exception:
        pass

def _page_has_error_banner(driver) -> bool:
    """詳細ページがエラー/非公開の典型文言を含むか簡易判定。"""
    try:
        txt = (driver.execute_script("return document.body ? document.body.innerText : ''") or "").strip()
    except Exception:
        txt = ""
    ERR_PATTERNS = ("見つかりません", "エラーが発生しました", "アクセスが集中", "しばらくしてから", "権限がありません", "この商品は削除")
    return any(p in txt for p in ERR_PATTERNS)

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

# ========= 詳細解析（Shops / 通常） =========
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

def parse_detail_shops(driver, url: str, preset: str) -> Dict[str, Any]:
    """メルカリShopsの商品詳細を解析し、DB書込み用レコードdictを返す。"""
    driver.get(url)
    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.3 + random.uniform(0.0, 0.2))
    if _page_has_error_banner(driver):
        _save_debug(driver, "debug_title_errpage_shops")
        raise ValueError("詳細ページがエラー/非公開の可能性 (shops)")

    for _ in range(2):
        _close_any_modal(driver)
        time.sleep(0.15)

    try:
        title_el = WebDriverWait(driver, 6).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-testid="product-title-section"] h1'))
        )
        title = (title_el.text or "").strip()
    except Exception as e:
        _save_debug(driver, "debug_title_shops_missing")
        raise ValueError(f"❌ Shopsタイトル取得失敗 url={url} e={e}")

    price = 0
    try:
        box = WebDriverWait(driver, 6).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-testid="product-price"]'))
        )
        m = re.search(r'(\d[\d,]*)', (box.text or ""))
        if m:
            price = int(m.group(1).replace(",", ""))
    except Exception:
        pass

    last_updated_str = ""
    try:
        dt_el = WebDriverWait(driver, 6).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, '#product-info > section:nth-child(2) > p'))
        )
        last_updated_str = (dt_el.text or "").strip()
    except Exception:
        last_updated_str = ""

    seller_id, seller_name, rating_count = "", "", 0
    try:
        seller_id, seller_name, rating_count = _extract_shops_seller(driver)
    except Exception:
        pass

    images = collect_images_shops(driver, limit=IMG_LIMIT)

    return {
        "vendor_name": VENDOR_NAME,
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

def parse_detail_personal(driver, url: str, preset: str) -> Dict[str, Any]:
    """通常メルカリの商品詳細を解析し、DB書込み用レコードdictを返す。"""
    driver.get(url)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    time.sleep(0.6 + random.uniform(0.0, 0.4))
    if _page_has_error_banner(driver):
        _save_debug(driver, "debug_title_errpage_personal")
        raise ValueError("詳細ページがエラー/非公開の可能性 (personal)")

    for _ in range(3):
        _close_any_modal(driver)
        time.sleep(0.2)

    title = None
    for attempt in range(3):
        title = _try_extract_title(driver, vis_timeout=15.0 if attempt == 0 else 10.0)
        if title:
            break
        if attempt < 2:
            time.sleep(0.5 + random.uniform(0.0, 0.3))
            try:
                driver.refresh()
                WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                time.sleep(0.4 + random.uniform(0.0, 0.3))
            except Exception:
                pass
    if not title:
        _save_debug(driver, "debug_title_missing_personal")
        raise ValueError(f"❌ タイトル取得失敗 (personal) url={url}")

    price = None
    for selector in ('[data-testid*="price"]', 'span[data-testid="price"]', '[class*="price"]'):
        try:
            element = WebDriverWait(driver, 4).until(EC.visibility_of_element_located((By.CSS_SELECTOR, selector)))
            price_text = (element.text or "").strip()
            price = int(re.sub(r"[^\d]", "", price_text))
            break
        except Exception:
            continue
    if price is None:
        try:
            yen_elements = driver.find_elements(By.XPATH, "//span[contains(text(), '¥')]")
            if yen_elements:
                price_text = (yen_elements[0].text or "").strip()
                price = int(re.sub(r"[^\d]", "", price_text))
