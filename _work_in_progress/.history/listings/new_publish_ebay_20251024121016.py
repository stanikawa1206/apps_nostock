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
POST_TARGET  = 200         # 出品できたら打ち切る目標件数
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
        "vendor_name": "メルカリ",
        "item_id": url.rstrip("/").split("/")[-1],
        "title_jp": title,
        "title_en": title_en,
        "price": price,
        "last_updated_str": last_updated_str,
        "seller_id": seller_id,
        "seller_name": seller_name,
        "rating_count": rating_count,
        "images": images,
        "preset": preset,
    }

# ========= DB書き込み =========
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
    imgs = (rec.get("images") or [])
    imgs = (imgs + [None]*10)[:10]
    preset_val = rec.get("preset") or PRESET_DEFAULT
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
    with conn.cursor() as cur:
        cur.execute(UPSERT_SELLER_SQL, (seller_id, vendor_name, seller_name, rating_count))
    conn.commit()

# ========= そのほか小物 =========
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
    return cut.rstrip() + "…"

def get_seller_gate(conn, vendor_name: str, seller_id: str) -> Tuple[bool, int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT rating_count
              FROM [mst].[seller]
             WHERE vendor_name = ? AND seller_id = ?
        """, (vendor_name, seller_id))
        row = cur.fetchone()
    rating_count = int(row[0]) if row else 0
    min_threshold = 20 if vendor_name == "メルカリshops" else 50
    is_ok = rating_count >= min_threshold
    return is_ok, rating_count

# ========= eBay記録（trx.listings） =========
def record_ebay_listing(listing_id: str, account_name: str, vendor_item_id: str, vendor_name: str):
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

# ========= メイン =========
def main():
    start_time = datetime.now()
    conn   = get_sql_server_connection()
    driver = build_driver()

    # タイトル置換ルール（任意）：運用している場合だけ有効化
    TITLE_RULES: List[tuple[str, str]] = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pattern, replacement
                  FROM mst.title_replace_rules
                 WHERE pattern IS NOT NULL AND LTRIM(RTRIM(pattern)) <> N''
                 ORDER BY rule_id
            """)
            TITLE_RULES = [( (r[0] or "").strip(), (r[1] or "") ) for r in cur.fetchall() if (r[0] or "").strip()]
    except Exception:
        TITLE_RULES = []

    success_count = 0
    skip_count = 0
    fail_other = 0

    try:
        # アカウント設定取得
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT fulfillment_policy_id, payment_policy_id, return_policy_id
                      FROM [mst].[ebay_accounts]
                     WHERE LTRIM(RTRIM(account)) = LTRIM(RTRIM(?))
                """, (EBAY_ACCOUNT,))
                row = cur.fetchone()
        if not row:
            raise RuntimeError(f"[mst].[ebay_accounts] にアカウントが見つかりません: {EBAY_ACCOUNT}")

        acct_policies = {
            "fulfillment_policy_id": str(row[0]),
            "payment_policy_id":     str(row[1]),
            "return_policy_id":      str(row[2]),
            "merchant_location_key": "Default",
        }

        ship_usd = shipping_usd_from_jpy(SHIPPING_JPY, utils.USD_JPY_RATE)
        print(f"🚚 送料(参考): {SHIPPING_JPY} JPY ≒ {ship_usd} USD")

        # === mst.presets から is_active=1 を取得 ===
        presets = fetch_active_presets(conn)   # [{'preset','vendor_name','brand_id','category_id'}, ...]

        for p in presets:
            preset      = p["preset"]
            vendor_name = p["vendor_name"]
            brand_id    = p["brand_id"]
            category_id = p["category_id"]

            # このスクリプトは「販売中」固定
            url = make_search_url(
                vendor_name=vendor_name,
                brand_id=brand_id,
                category_id=category_id,
                status="on_sale",
            )
            print(f"\n====================\n▶ {preset}（{vendor_name}）\nURL={url}\n====================")

            # 共通イテレータ（shops/personal 自動対応）
            for page_idx, item_id, item_url, _ in iterate_search(
                driver, url, preset, mode="ids", pause=0.45, stagnant_times=3
            ):
                # URLで vendor_name を確定（shopsなら メルカリshops）
                vendor_for_checks = "メルカリshops" if "/shops/product/" in item_url else "メルカリ"
                if success_count >= POST_TARGET:
                    break

                sku = (item_id or "").strip().lower()

                # 既存出品チェック
                if is_already_listed(conn, vendor_for_checks, sku):
                    head = "既存出品あり"
                    print(f"⏭️ スキップ: {head} sku='{sku}'")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, "")
                    skip_count += 1
                    continue

                # 出品不可フラグ
                if is_blocked_item(conn, vendor_for_checks, sku):
                    head = "出品不可フラグ"
                    print(f"⛔ スキップ: {head} {sku}")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, "")
                    skip_count += 1
                    continue

                # 詳細解析
                print(f"\n[DETAIL] {item_url}")
                try:
                    rec = parse_detail(driver, item_url, preset)
                    rec["vendor_name"] = vendor_for_checks

                    # 既存英訳があれば流用
                    existing_en = fetch_existing_title_en(conn, vendor_for_checks, sku)
                    if existing_en:
                        rec["title_en"] = existing_en
                    else:
                        title_en_raw = translate_to_english(rec["title_jp"]) or ""
                        if not title_en_raw.strip():
                            head = "翻訳空返し"
                            detail = "title_en is empty (deep_translator returned empty)"
                            print(f"⏭️ スキップ: {head}: {detail}")
                            mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                            skip_count += 1
                            continue

                        # タイトル置換（リテラル/大文字小文字無視）
                        for pat, rep in TITLE_RULES:
                            title_en_raw = re.compile(re.escape(pat), re.IGNORECASE).sub(rep, title_en_raw)
                        # 80字丸め
                        rec["title_en"] = smart_truncate80(re.sub(r"\s+", " ", title_en_raw).strip())

                except RuntimeError as e:
                    if "Translation failed" in str(e):
                        print(f"ABEND: {e}")
                        raise
                    else:
                        head = "解析失敗(Runtime)"
                        detail = str(e)
                        print(f"⏭️ スキップ: {head}: {detail}")
                        mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                        skip_count += 1
                        continue
                except Exception as e:
                    head = "解析失敗"
                    detail = str(e)
                    print(f"⏭️ スキップ: {head}: {detail}")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                    skip_count += 1
                    continue

                # 最終ガード（空返し）
                if not (rec.get("title_en") or "").strip():
                    head = "翻訳空返し"
                    detail = "title_en empty after replacement"
                    print(f"⏭️ スキップ: {head}: {detail}")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                    skip_count += 1
                    continue

                # 更新が古い → skip
                if re.search(r'(半年以上前|\d+\s*[ヶか]月前|数\s*[ヶか]月前)', rec.get("last_updated_str") or ""):
                    head = "古い更新"
                    detail = rec.get("last_updated_str") or ""
                    print(f"⏭️ スキップ: {head} {detail}")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                    skip_count += 1
                    continue

                # DB記録
                upsert_seller(conn, vendor_for_checks, rec["seller_id"], rec["seller_name"], rec["rating_count"])
                upsert_vendor_item(conn, rec)

                # 価格逆算（範囲外はスキップ）
                start_price_usd = compute_start_price_usd(rec["price"])
                if not start_price_usd:
                    head = "計算価格が範囲外"
                    detail = f"{utils.LOW_USD_TARGET}–{utils.HIGH_USD_TARGET}USD"
                    print(f"⏭️ スキップ: {head} ({detail})")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                    skip_count += 1
                    continue

                # セラー条件
                ok_gate, rating = get_seller_gate(conn, vendor_for_checks, rec["seller_id"])
                if not ok_gate:
                    head = "セラー条件未達"
                    detail = f"rating={rating}"
                    print(f"⏭️ スキップ: {head} ({detail})")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                    skip_count += 1
                    continue

                # 出品ペイロード
                imgs = [u for u in rec["images"] if u][:12]
                payload = {
                    "CustomLabel":   sku,
                    "*Title":        rec.get("title_en") or "",
                    "*StartPrice":   start_price_usd,
                    "*Quantity":     1,
                    "PicURL":        "|".join(imgs),
                    "*Description":  f"{rec.get('title_en') or ''}\n\nPlease contact us via eBay messages for details.\nShips from Japan with tracking.",
                    "C:Brand":       DEFAULT_BRAND_EN,
                    "C:Color":       "Multicolor",
                    "C:Type":        "Wallet",
                    "category_id":   CATEGORY_ID,
                    "department":    DEPARTMENT,
                    "C:Country of Origin": "France",
                }

                print(f"🛒 出品: SKU={payload['CustomLabel']}  Title='{payload['*Title']}'  Price(USD)={payload['*StartPrice']}")

                try:
                    if TEST_MODE:
                        print("\n=== 📦 出品テストモード（eBay未送信） ===")
                        print(json.dumps(payload, indent=2, ensure_ascii=False))
                        continue

                    item_id_ebay = post_one_item(payload, EBAY_ACCOUNT, acct_policies)
                    if item_id_ebay:
                        print(f"✅ 出品完了 listing_id={item_id_ebay}")
                        record_ebay_listing(item_id_ebay, EBAY_ACCOUNT, payload["CustomLabel"], vendor_for_checks)
                        mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, "出品", "")
                        success_count += 1
                    else:
                        head = "出品失敗"
                        detail = "listing_id未返却"
                        print(f"❌ {head}: {detail}")
                        mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                        fail_other += 1
                except (ListingLimitError, ApiHandledError) as e:
                    head = "出品失敗"
                    detail = str(e)
                    print(f"❌ {head}: {detail}")
                    mark_listing_status_head_detail(conn, vendor_for_checks, sku, page_idx, head, detail)
                    fail_other += 1

            if success_count >= POST_TARGET:
                print(f"🎯 目標 {POST_TARGET} 件に到達したので処理を終了します。")
                break

        # まとめ
        print(f"\n✅ 処理終了: 成功{success_count} / スキップ{skip_count} / 失敗{fail_other}")
        if success_count < POST_TARGET:
            print(f"ℹ️ 目標 {POST_TARGET} 件に未達（在庫や条件で弾かれました）")

        # 完了メール
        end_time = datetime.now()
        subject = "✅ eBay出品処理 完了通知"
        body = (
            f"開始時刻: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"終了時刻: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"処理時間: {end_time - start_time}\n"
            f"結果: 成功{success_count} / スキップ{skip_count} / 失敗{fail_other}\n"
            f"実行スクリプト:\n{Path(__file__).name}\n"
        )
        try:
            send_mail(subject, body)
        except Exception as e:
            print(f"[WARN] 完了メール送信に失敗: {e}")

    finally:
        try:
            safe_quit(driver)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

# ===== エントリポイント =====
if __name__ == "__main__":
    main()
