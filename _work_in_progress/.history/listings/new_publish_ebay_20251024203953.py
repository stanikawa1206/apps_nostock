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

from ebay_common import fetch_active_presets, make_search_url, iterate_search

from scrape_utils import (
    scroll_until_stagnant_collect_items,
    scroll_until_stagnant_collect_shops,
)

# ========= 固定値／運用設定 =========
IMG_LIMIT          = 10
POST_TARGET        = 1
TEST_MODE          = False

# eBayアカウント
EBAY_ACCOUNT       = "谷川②"
SHIPPING_JPY       = 3000

# ===== eBay 側：カテゴリ・スペック =====
CATEGORY_ID       = "45258"
DEPARTMENT        = "Women"
DEFAULT_BRAND_EN  = "Louis Vuitton"

# ========= 価格帯計算補助 =========
def invert_cost_jpy(target_usd: float,
                    usd_jpy_rate: float,
                    profit_rate: float,
                    ebay_fee_rate: float,
                    domestic_shipping_jpy: int) -> float:
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
    low_jpy  = invert_cost_jpy(low_usd,  usd_jpy_rate, profit_rate, ebay_fee_rate, domestic_shipping_jpy)
    high_jpy = invert_cost_jpy(high_usd, usd_jpy_rate, profit_rate, ebay_fee_rate, domestic_shipping_jpy)
    price_min = max(0, int(math.floor(low_jpy)))
    price_max = max(price_min, int(math.ceil(high_jpy)))
    return price_min, price_max

# ========= 翻訳 =========
def translate_to_english(
    text_jp: str,
    per_attempt_timeout: float = 8.0,
    attempts: int = 3,
    backoff_base: float = 1.0
) -> str:
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

# ========= 詳細解析（Shops / 通常） =========
def parse_detail_shops(driver, url: str, preset: str, vendor_name: str) -> Dict[str, Any]:
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

    # 以下、略：中身は変更なし。vendor_name のみ rec に追加。
    price = 0
    try:
        element = WebDriverWait(driver, 4).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-testid*="price"]')))
        price_text = (element.text or "").strip()
        price = int(re.sub(r"[^\d]", "", price_text))
    except Exception:
        pass

    try:
        last_updated_str = driver.find_element(By.XPATH, '//*[@id="item-info"]/section[2]/p').text.strip()
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

def mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, head, detail):
    """
    出品処理のステータスを記録する共通関数。
    head: ステータスの概要（例：「既存出品あり」「翻訳空返し」など）
    detail: 補足情報
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trx.listing_status_log (vendor_name, sku, page_idx, head, detail, created_at)
            VALUES (?, ?, ?, ?, ?, GETDATE())
            """,
            (vendor_name, sku, page_idx, head, detail)
        )
    conn.commit()

def is_already_listed(conn, vendor_name: str, sku: str) -> bool:
    """既に trx.listings に同SKUの出品記録があるか判定。"""
    vendor_item_id = (sku or "").strip().lower()
    vendor = (vendor_name or "").strip()
    if not vendor_item_id or not vendor:
        return False
    sql = """
        SELECT TOP 1 1
          FROM [trx].[listings] WITH (NOLOCK)
         WHERE vendor_name = ? AND LOWER(vendor_item_id) = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql, (vendor, vendor_item_id))
  

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

def shipping_usd_from_jpy(jpy: int, usd_jpy_rate: float) -> str:
    """JPY送料をUSD文字列へ変換（小数2桁）。"""
    usd = (Decimal(jpy) / Decimal(str(usd_jpy_rate))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{usd:.2f}"

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

# ========= main（変更ポイント） =========
def main():
    start_time = datetime.now()
    conn = get_sql_server_connection()
    driver = build_driver()

    global TITLE_RULES
    TITLE_RULES = load_title_rules(conn)

    success_count = 0
    skip_count = 0
    fail_other = 0

    try:
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
            "payment_policy_id": str(row[1]),
            "return_policy_id": str(row[2]),
            "merchant_location_key": "Default",
        }

        ship_usd = shipping_usd_from_jpy(SHIPPING_JPY, utils.USD_JPY_RATE)
        print(f"🚚 送料(参考): {SHIPPING_JPY} JPY ≒ {ship_usd} USD")

        presets = fetch_active_presets(conn)

        for p in presets:
            preset = p["preset"]
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

            for page_idx, item_id, _item_url, _preset in iterate_search(
                driver, url, preset, mode="ids", pause=0.45, stagnant_times=3
            ):
                if success_count >= POST_TARGET:
                    break

                item_url = (
                    f"https://jp.mercari.com/shops/product/{item_id}"
                    if vendor_name == "メルカリshops"
                    else f"https://jp.mercari.com/item/{item_id}"
                )

                sku = (item_id or "").strip().lower()

                if is_already_listed(conn, vendor_name, sku):
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "既存出品あり", "")
                    skip_count += 1
                    continue

                if is_blocked_item(conn, vendor_name, sku):
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "出品不可フラグ", "")
                    skip_count += 1
                    continue

                try:
                    rec = parse_detail_shops(driver, item_url, preset, vendor_name) \
                        if vendor_name == "メルカリshops" else \
                        parse_detail_personal(driver, item_url, preset, vendor_name)

                    existing_en = fetch_existing_title_en(conn, vendor_name, sku)
                    if existing_en:
                        rec["title_en"] = existing_en
                    else:
                        title_en_raw = translate_to_english(rec["title_jp"]) or ""
                        if not title_en_raw.strip():
                            mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "翻訳空返し", "")
                            skip_count += 1
                            continue
                        rec["title_en"] = smart_truncate80(apply_title_rules_literal_ci(title_en_raw, TITLE_RULES))

                except Exception as e:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "解析失敗", str(e))
                    skip_count += 1
                    continue

                title_en_raw = (rec.get("title_en") or "").strip()
                if not title_en_raw:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "翻訳空返し", "")
                    skip_count += 1
                    continue

                upsert_seller(conn, vendor_name, rec["seller_id"], rec["seller_name"], rec["rating_count"])
                upsert_vendor_item(conn, rec)

                start_price_usd = compute_start_price_usd(rec["price"])
                if not start_price_usd:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "計算価格が範囲外", "")
                    skip_count += 1
                    continue

                is_ok, rating = get_seller_gate(conn, vendor_name, rec["seller_id"])
                if not is_ok:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "セラー条件未達", f"rating={rating}")
                    skip_count += 1
                    continue

                imgs = [u for u in rec["images"] if u][:12]
                payload: Dict[str, Any] = {
                    "CustomLabel": str(rec["item_id"]).strip().lower(),
                    "*Title": rec.get("title_en") or "",
                    "*StartPrice": start_price_usd,
                    "*Quantity": 1,
                    "PicURL": "|".join(imgs),
                    "*Description": f"{rec.get('title_en') or ''}\n\nPlease contact us via eBay messages for details.\nShips from Japan with tracking.",
                    "category_id": CATEGORY_ID,
                    "C:Brand": DEFAULT_BRAND_EN,
                    "C:Type": "Wallet",
                    "C:Department": DEPARTMENT,
                    "C:Country of Origin": "France",
                }

                print(f"🛒 出品: SKU={payload['CustomLabel']} Title='{payload['*Title']}'")

                try:
                    if TEST_MODE:
                        print("=== TEST MODE ===")
                        print(json.dumps(payload, indent=2, ensure_ascii=False))
                        continue

                    item_id_ebay = post_one_item(payload, EBAY_ACCOUNT, acct_policies)
                    if item_id_ebay:
                        record_ebay_listing(item_id_ebay, EBAY_ACCOUNT, payload["CustomLabel"], vendor_name)
                        mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "出品", "")
                        success_count += 1
                    else:
                        mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "出品失敗", "listing_id未返却")
                        fail_other += 1
                except (ListingLimitError, ApiHandledError) as e:
                    mark_listing_status_head_detail(conn, vendor_name, sku, page_idx, "出品失敗", str(e))
                    fail_other += 1

                if success_count >= POST_TARGET:
                    break

        end_time = datetime.now()
        elapsed = end_time - start_time
        subject = "✅ eBay出品処理 完了通知"
        body = (
            f"開始時刻: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"終了時刻: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"処理時間: {elapsed}\n"
            f"結果: 成功{success_count} / スキップ{skip_count} / 失敗{fail_other}\n"
            f"実行スクリプト:\n{Path(__file__).name}\n"
        )
        try:
            send_mail(subject, body)
        except Exception as e:
            print(f"[WARN] 完了メール送信に失敗: {e}")
        print("=== 🎉 全ての処理が完了しました ===")

    finally:
        try: driver.quit()
        except Exception: pass
        try: conn.close()
        except Exception: pass

if __name__ == "__main__":
    main()
