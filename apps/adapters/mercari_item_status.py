from __future__ import annotations
import re
import time
from typing import Literal, Optional, Tuple, Dict, Any
import requests
from typing import Optional
import json
from playwright.sync_api import sync_playwright

import pyodbc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timezone, timedelta

# ★ eBay削除API ← 波線エラーの正体はコレ
from apps.adapters.ebay_api import delete_item_from_ebay
from apps.adapters.ebay_api import update_ebay_price_rest

# ================================
# 例外定義  
# ================================
class MercariItemUnavailableError(Exception):
    """
    メルカリ側で「販売中でない」状態（売切れ・削除・非公開など）を表す例外。
    state: '売り切れ', '削除', '判定不可' など detect_status_* が返す状態文字列。
    """
    def __init__(self, state: str, message: Optional[str] = None) -> None:
        self.state = state
        super().__init__(message or state)

# ================================
# メルカリ商品状態（共通定義）
# ================================
Status = Literal["販売中", "売り切れ", "削除", "オークション", "判定不可"]

# タイムアウト
TIMEOUT = 12

# 価格抽出用
PRICE_RE = re.compile(r"[¥￥]\s*([0-9,]+)")


# ================================
# 価格抽出
# ================================
def extract_price_jpy_from(main: BeautifulSoup) -> Optional[int]:
    """画面に出ている販売価格（¥12,345 など）を素朴に抽出。"""
    for sel in ["#main", '[data-testid="price"]', '[class*="price"]',
                "h2", "p", "span", "div"]:
        for el in main.select(sel):
            t = el.get_text(" ", strip=True)
            m = PRICE_RE.search(t)
            if m:
                return int(m.group(1).replace(",", ""))
    t = main.get_text(" ", strip=True)
    m = PRICE_RE.search(t)
    return int(m.group(1).replace(",", "")) if m else None


# ================================
# 通常メルカリ
# ================================


def fetch_json_core(page, url, match_func):

    storage = {"json": None}

    # api きたら拾う
    def handle_response(response):
        if match_func(response):
            try:
                storage["json"] = response.json()
            except Exception:
                pass

    page.on("response", handle_response) # 監視 start

    try:
        # 1. ページ開く
        print(f"[{datetime.now().strftime('%H:%M:%S')}] domcontentloaded")
        # page.goto(url, wait_until="networkidle", timeout=30000)
        # page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.goto(url, wait_until="domcontentloaded", timeout=10000)
        print("goto end", datetime.now().strftime("%H:%M:%S"))

        # --- ▼追加①：API発火を促すためにスクロール ---
        # （メルカリはスクロールでAPIが発火することがある）
        # --- ▼追加：scrollは例外で落とさない ---
        print("scroll", datetime.now().strftime("%H:%M:%S"))
        try:
            print("evaluate", datetime.now().strftime("%H:%M:%S"))
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            print("[WARN] scroll失敗（無視して続行）")

        # --- ▼追加②：API発火待ちのため少し待機 ---
        # （即evaluateするとAPIがまだ来てないことがある）
        page.wait_for_timeout(1000)

        #  responseイベントでAPIが取得されるのを待機する（このループ自体は取得処理ではない）
        start = time.time()
        for _ in range(40):  # 0.2秒ごとに × 40回のAPI待ち = 最大8秒 
            if storage["json"] is not None:
                break
            # API待機が無限ループ化するのを防ぐため、
            # 最大待機時間（ここでは10秒）を超えたら強制的に抜ける
            if time.time() - start > 10:  # 最大10秒
                #break  timeoutはerror処理すべき
                raise TimeoutError("API来ない")
            page.wait_for_timeout(200)

        print("json取得", datetime.now().strftime("%H:%M:%S"))
        # return storage["json"], page.content()   page.content()はHTML（ページの中身全部）　HTMLは不要 26/03/25
        return storage["json"]
    except Exception  as e:
        # return None, ""  エラーとして処理すべき
        print("[ERROR fetch]", e)
        raise SystemExit("fetch失敗 → worker終了")
    finally:
        page.remove_listener("response", handle_response)

def fetch_mercari_api_data(page, url):
    json_data = fetch_json_core(
        page,
        url,
        lambda r: "items/get?id=" in r.url
                  and "application/json" in r.headers.get("content-type", "")
    )
    return json_data, None

def fetch_shops_api_data(page, url):
    json_data = fetch_json_core(
        page,
        url,
        lambda r: "api.mercari.jp/v1/marketplaces/shops/products" in r.url
                  and "application/json" in r.headers.get("content-type", "")
    )
    return json_data, None


def _parse_status_from_res(res) -> Tuple[str, Optional[int]]:
    # --- 1. 削除の確定判定 ---
    if res and res.get("result") == "error":
        errors = res.get("errors", [])
        if any(e.get("code") == "InvisibleItemException" for e in errors):
            return "削除", None

    # --- 2. 正常データの解析 ---
    if res and res.get("result") == "OK":
        item = res.get("data", {})
        if isinstance(item, list):
            if not item:
                return "判定不可", None
            item = item[0]

        # オークション判定
        auction_info = item.get("auction_info")
        if auction_info and auction_info.get("id"):
            return "オークション", None

        # 通常ステータス判定
        status = item.get("status")
        if status == "on_sale":
            return "販売中", item.get("price")
        elif status in ["trading", "sold_out", "finished"]:
            return "売り切れ", None

        return "判定不可", None

    return "判定不可", None

def check_rakuma_purchase_request(page):

    page.locator("text=購入に進む").click()
    page.wait_for_timeout(3000)
    if page.locator("a#dialog-btn").count() > 0:
        return True

    return False


def detect_status_from_rakuma(page, url):

    page.goto(
        url,
        wait_until="domcontentloaded",
        timeout=60000
    )
    html = page.content()

    # ページ削除
    if "お探しのページは見つかりませんでした" in html:
        return "削除", None


    # 売り切れ
    if "SOLD OUT" in html:
        return "売り切れ", None

    # 販売中
    if "購入に進む" in html:
        is_request = check_rakuma_purchase_request(page)

        if is_request:
            return "購入申請あり", None

        price_text = page.locator(".item__price").inner_text()

        price_jpy = int(
            price_text
            .replace("¥", "")
            .replace(",", "")
            .strip()
        )

        return "販売中", price_jpy

    return "判定不可", None

def detect_status_from_mercari(page, url: str) -> Tuple[str, Optional[int]]:
    max_retries = 2
    
    for attempt in range(max_retries):
        res, html_content = fetch_mercari_api_data(page, url)
        
        # ===== DEBUG LOG START =====
        try:
            item_id = url.split("/")[-1]

            if res is None:
                print(f"[DEBUG] item_id={item_id} res=None")
            else:
                result = res.get("result")
                error_codes = [e.get("code") for e in res.get("errors", [])] if res.get("errors") else []

                print(f"[DEBUG] item_id={item_id} result={result} errors={error_codes}")
        except Exception as e:
            print(f"[DEBUG ERROR] {e}")
        # ===== DEBUG LOG END =====

        status, price = _parse_status_from_res(res)

        if status != "判定不可":
            return status, price

        # --- リトライ ---
        if attempt < max_retries - 1:
            print("retry")
            time.sleep(1)
            page.close()
            page = page.context.new_page()
            continue
        break

    return "判定不可", None


def detect_status_from_mercari_bk(driver) -> tuple[str, Optional[int]]:
    # 1. ページ読み込み待機 (価格要素が出るまで)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='price']"))
        )
    except:
        # タイムアウト時にページが存在しないか確認
        if "ページが見つかりません" in driver.page_source:
            return "削除", None
        return "判定不可", None

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # --- 共通：価格の取得 ---
    price_tag = soup.find(attrs={"data-testid": "price"})
    price = int(re.sub(r'\D', '', price_tag.get_text())) if price_tag else None

    # --- ステータス判定（ボタンの属性を直接見る） ---

    # A. オークション判定
    bid_button_container = soup.find(attrs={"data-testid": "bid-button"})
    if bid_button_container:
        return "オークション", price

    # B. 通常販売 or 売り切れ判定
    checkout_button_container = soup.find(attrs={"data-testid": "checkout-button"})
    if checkout_button_container:
        # container内のbuttonタグを探す
        button_tag = checkout_button_container.find("button")
        
        if button_tag:
            # disabled属性があれば「売り切れ」、なければ「販売中」
            if button_tag.has_attr("disabled"):
                return "売り切れ", None
            else:
                return "販売中", price

    # C. どちらのボタンも見当たらない場合（削除済みなど）
    if "ページが見つかりません" in soup.get_text():
        return "削除", None

    return "判定不可", None

# ================================
# Shops 用
# ================================
def _wait_product_price_ready(driver: webdriver.Chrome, timeout: int = TIMEOUT) -> None:
    sel = (By.CSS_SELECTOR, '[data-testid="product-price"]')

    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located(sel)
    )

    def _has_price_text(drv: webdriver.Chrome) -> bool:
        try:
            el = drv.find_element(*sel)
            t = el.text or el.get_attribute("textContent") or ""
            return bool(re.search(r"[¥￥]\s*[0-9,]+", t))
        except Exception:
            return False

    WebDriverWait(driver, timeout).until(_has_price_text)


def detect_status_from_mercari_shops(
    driver: webdriver.Chrome,
    
) -> tuple[Status, Optional[int]]:

    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
    except Exception:
        pass

    try:
        _wait_product_price_ready(driver, timeout=TIMEOUT)
    except Exception:
        pass

    time.sleep(0.2)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    main = soup.select_one("#main") or soup

    whole = main.get_text(" ", strip=True)
    if any(t in whole for t in [
        "ページが見つかりませんでした",
        "該当する商品は削除されています。",
    ]):
        return "削除", None

    price_el = main.select_one('[data-testid="product-price"]')
    if not price_el:
        raise RuntimeError("価格要素（product-price）が見つかりませんでした")

    t = price_el.get_text(strip=True)
    m = PRICE_RE.search(t)
    if not m:
        raise RuntimeError(f"価格抽出失敗: {t}")

    price = int(m.group(1).replace(",", ""))

    # ===== 売り切れ（disabledボタン）判定 =====
    try:
        btn = driver.find_element(By.XPATH, "//button[normalize-space()='購入手続きへ']")

        # disabled 属性が付いていれば売り切れ
        if btn.get_attribute("disabled") is not None:
            return "売り切れ", None

        # disabled が無ければ販売中
        return "販売中", price

    except Exception:
        # ボタンが存在しない場合は販売不可
        return "売り切れ", None

# ================================
# vendor_item 更新
# ================================
def mark_vendor_item_unavailable(
    conn: pyodbc.Connection,
    vendor_name: str,
    vendor_item_id: str,
    status: Status,
) -> None:

    if status == "販売中":
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trx.vendor_item
               SET status            = ?,
                   [出品状況]        = NULL,
                   [出品状況詳細]    = NULL,
                   last_ng_at         = NULL,
                   last_checked_at    = SYSDATETIME()
             WHERE vendor_name       = ?
               AND vendor_item_id    = ?;
            """,
            (status, vendor_name, vendor_item_id),
        )


# ================================
# eBay 側削除 & trx.listings 論理削除
# ================================
def get_active_listing_for_action(
    conn,
    vendor_item_id: str,
    vendor_name: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    削除・価格変更共通。
    active listing を取得し、
    account が excluded なら None を返す。
    """

    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT l.listing_id, l.account
            FROM trx.listings l
            JOIN mst.ebay_accounts a
              ON l.account = a.account
            WHERE l.vendor_item_id = ?
              AND l.vendor_name = ?
              AND ISNULL(l.is_deleted, 0) = 0
              AND ISNULL(a.is_excluded, 0) = 0
        """, (vendor_item_id, vendor_name))

        row = cur.fetchone()
        if not row:
            return (None, None)

        return str(row[0]), str(row[1])

    finally:
        cur.close()

def handle_listing_delete(
    conn,
    vendor_item_id: str,
    vendor_name: str,
    reason: str,
    simulate: bool = False,
):
    listing_id, account = get_active_listing_for_action(
        conn,
        vendor_item_id,
        vendor_name,
    )

    if not listing_id:
        return

    if simulate:
        print(f"[SIMULATE DELETE] {listing_id=}")
        return

    res = delete_item_from_ebay(account, vendor_item_id)
    
    ok = bool(res.get("success")) or res.get("note") in {
        "already_deleted",
        "already_ended",
    }

    if ok:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trx.listings
                   SET is_deleted = 1,
                       deleted_at = ?,
                       delete_reason = ?
                 WHERE listing_id = ?
                   AND account = ?
            """, (datetime.now(),reason, listing_id, account))
        conn.commit()




def _is_transient_inventory_error(resp: Dict[str, Any]) -> bool:
    if not resp or resp.get("success"):
        return False
    raw = resp.get("raw") or {}
    errors = ((raw.get("putOffer") or {}).get("errors") or []) or raw.get("errors") or []
    msgs = " ".join(str(e.get("message","")) for e in errors if isinstance(e, dict)).lower()
    codes = {int(e.get("errorId")) for e in errors if isinstance(e, dict) and str(e.get("errorId","")).isdigit()}
    return (25001 in codes) or ("internal error" in msgs)

def handle_listing_price_update(
    conn,
    vendor_item_id: str,
    vendor_name: str,
    usd: float,
) -> None:
    """
    価格更新（listing_id 主語）
    - active listing を取得（excluded は get_active_listing_for_action 内で除外済み）
    - 取れなければ何もしない
    - 取れたら update_ebay_price を実行
    """

    listing_id, account = get_active_listing_for_action(
        conn,
        vendor_item_id,
        vendor_name,
    )

    if not listing_id:
        # そもそも出品していない / is_deleted=1 / excluded など
        return

    did_update = False
    resp: Optional[Dict[str, Any]] = None

    for wait in [0, 2, 6, 15]:
        if wait:
            time.sleep(wait)

        try:
            resp = update_ebay_price_rest(
                account,
                vendor_item_id,
                usd,
                debug=True,
            )

        except Exception as e:
            print(
                f"[RETRY] exception sku={vendor_item_id} wait={wait} err={type(e)} {e}",
                flush=True
            )
            resp = None
            continue  # ← 次のretryへ

        if resp and resp.get("success"):
            did_update = True
            break

        if not _is_transient_inventory_error(resp or {}):
            break

    if not did_update:
        raise RuntimeError(
            f"eBay価格更新失敗 sku={vendor_item_id} listing_id={listing_id} account={account}"
        )
    
if __name__ == "__main__":

    test_url = "https://fril.jp/item/00854aa1c581d2224423c0ce5cf149ad"

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=False)

        page = browser.new_page()

        result = detect_status_from_rakuma(
            page=page,
            url=test_url
        )

        print(result)

        input("enterで終了")

        browser.close()