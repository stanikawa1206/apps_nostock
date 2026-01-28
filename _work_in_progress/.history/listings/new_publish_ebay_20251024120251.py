# -*- coding: utf-8 -*-
# publish_ebay.py — シンプル最小運用版（listings / vendor_item 対応, presets駆動, 共通read利用）

import concurrent.futures, sys, os, re, time, math, json, random
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

_THIS_FILE = os.path.abspath(__file__)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_THIS_FILE))   # 例: D:\apps_nostock
_COMMON_DIR = os.path.join(_PROJECT_ROOT, "common")
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

# ====== 3rd party ======
import pyodbc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from deep_translator import GoogleTranslator

# ====== ローカル ======
import utils
from utils import get_sql_server_connection, compute_start_price_usd, send_mail
from publish_ebay_adapter import post_one_item, ApiHandledError, ListingLimitError

# ★ 共通化した「読取系」ユーティリティ
from ebay_common import (
    fetch_active_presets,   # mst.presets から is_active=1 を読む
    make_search_url,        # vendor_name/brand_id/category_id/status から検索URLを作る
    iterate_search,         # 検索結果をページングしながら収集（shops/personal 両対応）
    build_driver,           # 安定化オプション付き Driver
    safe_quit,              # 安全終了
)

# ========= 固定値 =========
IMG_LIMIT    = 10          # 画像の最大拾得枚数
POST_TARGET  = 1         # 出品できたら打ち切る目標件数
TEST_MODE    = False       # ← テスト時 True / 本番は False

# eBay出品設定
EBAY_ACCOUNT = "谷川②"     # 出品先アカウント名（DB参照に使用）
SHIPPING_JPY = 3000        # 国際送料（参考出力用）

# eBayカテゴリ固有
CATEGORY_ID       = "45258"          # Women > Women's Accessories > Wallets
DEPARTMENT        = "Women"
DEFAULT_BRAND_EN  = "Louis Vuitton"
PRESET_DEFAULT    = "ヴィトン長財布M"

# ========= 翻訳 =========
def translate_to_english(
    text_jp: str,
    per_attempt_timeout: float = 8.0,   # 1回あたりの“待つ上限”秒数
    attempts: int = 3,                  # 最大試行回数
    backoff_base: float = 1.0           # リトライ間隔のベース（指数バックオフ）
) -> str:
    """
    GoogleTranslator をハード・タイムアウト付きで呼び出す。
    ・フォールバックはしない（失敗したら例外で呼び出し側へ）
    ・最大 attempts 回まで再試行
    ・各試行は per_attempt_timeout 秒で必ず打ち切る
    """
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
            wait = backoff_base * (2 ** (i - 1))  # 1,2,4,…
            print(f"[translate] timeout ({i}/{attempts}, {per_attempt_timeout}s). retry in {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            last_err = e
            wait = backoff_base * (2 ** (i - 1))
            print(f"[translate] error ({i}/{attempts}): {e!r}. retry in {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError(
        f"Translation failed after {attempts} attempts "
        f"(per_attempt_timeout={per_attempt_timeout}s). Last error: {last_err!r}"
    )

# ========= 共通：ページ内ユーティリティ =========
def _page_has_error_banner(driver) -> bool:
    try:
        txt = (driver.execute_script("return document.body ? document.body.innerText : ''") or "").strip()
    except Exception:
        txt = ""
    ERR_PATTERNS = ("見つかりません", "エラーが発生しました", "アクセスが集中", "しばらくしてから", "権限がありません", "この商品は削除")
    return any(p in txt for p in ERR_PATTERNS)

def _close_any_modal(driver):
    # よくある同意/閉じるボタンを片っ端からクリック（存在すれば）
    js_contains = """
      return Array.from(document.querySelectorAll('button, [role=button]')).find(b=>{
        const t=(b.innerText||'').trim();
        return ['同意','閉じる','許可しない','OK','Accept','Close'].some(k=>t.includes(k));
      });
    """
    try:
        btn = driver.execute_script(js_contains)
        if btn:
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.3)
            return
    except Exception:
        pass

# ========= セラー/画像 取得補助 =========
def _try_extract_title(driver, vis_timeout: float = 12.0) -> Optional[str]:
    try:
        WebDriverWait(driver, vis_timeout).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "#item-info"))
        )
    except TimeoutException:
        pass

    sels = [
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

def _find_seller_info(driver, max_retry: int = 3, wait_sec: float = 0.8):
    selectors = [
        "#item-info a[href^='/user/profile/']",
        "a[href^='/user/profile/']",
        "[data-testid='seller-info'] a[href*='/user/']",
        "a[href*='/user/']",
    ]
    for _ in range(max_retry):
        for sel in selectors:
            try:
                el = WebDriverWait(driver, 3).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, sel))
                )
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

def _extract_shops_seller(driver) -> tuple[str, str, int]:
    a = WebDriverWait(driver, 6).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, 'a[data-testid="shops-profile-link"]'))
    )
    href = (a.get_attribute("href") or "").strip()
    seller_id = href.rstrip("/").split("/")[-1] if href else ""

    try:
        root = a.find_element(
            By.XPATH,
            'ancestor::div[contains(@class,"merUserObject") or contains(@data-testid,"shop")]'
        )
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
    name, rating_from_block = _parse_shops_seller_text(block_text)
    if rating_count == 0:
        rating_count = rating_from_block
    seller_name = name or (a.text or "").strip()
    return seller_id, seller_name, rating_count

def _parse_shops_seller_text(text: str) -> tuple[str, int]:
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

def collect_images_shops(driver, limit: int = 10) -> List[Optional[str]]:
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
    out: List[Optional[str]] = urls[:]
    while len(out) < limit:
        out.append(None)
    return out

# ========= 詳細ページ解析 =========
def parse_detail(driver, url, preset):
    if "/shops/product/" in url:
        return parse_detail_shops(driver, url, preset)
    else:
        return parse_detail_personal(driver, url, preset)

def parse_detail_shops(driver, url: str, preset: str) -> Dict[str, Any]:
    driver.get(url)
    WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.3 + random.uniform(0.0, 0.2))

    if _page_has_error_banner(driver):
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
        raise ValueError(f"Shopsタイトル取得失敗 url={url} e={e}")

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

    try:
        seller_id, seller_name, rating_count = _extract_shops_seller(driver)
    except Exception:
        seller_id = ""; seller_name = ""; rating_count = 0

    images = collect_images_shops(driver, limit=IMG_LIMIT)

    return {
        "vendor_name": "メルカリshops",
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

def parse_detail_personal(driver, url: str, preset: str) -> Dict[str, Any]:
    driver.get(url)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    time.sleep(0.6 + random.uniform(0.0, 0.4))

    if _page_has_error_banner(driver):
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
        raise ValueError(f"タイトル取得失敗 (personal) url={url}")

    title_en = ""

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
        except Exception:
            price = 0
    if price is None:
        price = 0

    try:
        last_updated_str = driver.find_element(By.XPATH, '//*[@id="item-info"]/section[2]/p').text.strip()
    except Exception:
        try:
            sec = driver.find_element(By.CSS_SELECTOR, '#item-info section')
            last_updated_str = (sec.text or "").splitlines()[-1].strip()
        except Exception:
            last_updated_str = ""

    info = _find_seller_info(driver)
    if not info:
        raise ValueError("セラー情報なし (personal)")
    seller_id, seller_name, rating_count = info

    images = []
    for img in driver.find_elements(By.CSS_SELECTOR, "article img[src], article source[srcset]"):
        src = None
        try:
            if img.tag_name.lower() == "img":
                src = img.get_attribute("src")
            else:
                srcset = img.get_attribute("srcset") or ""
                parts = [p.strip().split(" ")[0] for p in srcset.split(",") if p.strip()]
                if parts
