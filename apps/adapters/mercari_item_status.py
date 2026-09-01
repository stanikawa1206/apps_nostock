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
from apps.common.utils import log_listings_change

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


def apply_default_route_filter(page) -> None:
    """
    通常メルカリの商品ページ用の共通 route フィルタ。
    - items/get は必ず通す（価格・状態判定に必須のため）
    - 画像/動画/フォント/CSSは遮断（軽量化）
    - それ以外はそのまま通す
    新規ページ作成時（初回・リトライ後どちらも）に必ず呼び出すこと。
    """
    page.route("**/*", lambda route:
        route.continue_() if "items/get" in route.request.url else
        route.abort() if route.request.resource_type in ["image", "media", "font", "stylesheet"] else
        route.continue_()
    )


def fetch_json_core(page, url, match_func, status_holder: Optional[dict] = None):
    """
    status_holder を渡すと、ページ遷移時のHTTPステータスを
    status_holder["http_status"] に書き込む（呼び出し側の任意利用）。
    戻り値は従来通り storage["json"] のみ（既存呼び出し元との互換性維持）。
    """

    storage = {"json": None, "http_status": None}

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
        nav_response = page.goto(url, wait_until="domcontentloaded", timeout=10000)
        storage["http_status"] = nav_response.status if nav_response else None
        if status_holder is not None:
            status_holder["http_status"] = storage["http_status"]
        print("goto end", datetime.now().strftime("%H:%M:%S"), "http_status=", storage["http_status"])

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
        # --- ▼修正：range(40)*200ms(=最大8秒)と elapsed>10秒 の閾値がズレており、
        # 10秒判定に実質到達しないバグがあったため、経過時間ベースのwhileに統一 ---
        start = time.time()
        while storage["json"] is None:
            elapsed = time.time() - start
            if elapsed > 10:  # 最大10秒
                print(f"[TIMEOUT] items/get 応答なし（{elapsed:.1f}秒経過） url={url}")
                break
            page.wait_for_timeout(200)

        if storage["json"] is not None:
            print("json取得", datetime.now().strftime("%H:%M:%S"))
        else:
            # ★ items/get が来ないのは「削除済み商品で404が返っている」ケースもあり得るため、
            # ここではエラー扱い（SystemExit）にせず、呼び出し側（404/HTMLフォールバック判定）に委ねる
            print(f"[WARN] items/get JSON未取得のまま終了 http_status={storage['http_status']}（呼び出し側でフォールバック判定へ）")

        # return storage["json"], page.content()   page.content()はHTML（ページの中身全部）　HTMLは不要 26/03/25
        return storage["json"]
    except Exception  as e:
        # ここに来るのは page.goto 失敗など、items/get 未取得とは別の予期しない異常のみ
        print("[ERROR fetch]", e)
        raise SystemExit("fetch失敗 → worker終了")
    finally:
        page.remove_listener("response", handle_response)

def fetch_mercari_api_data(page, url):
    status_holder: dict = {}
    json_data = fetch_json_core(
        page,
        url,
        lambda r: "items/get?id=" in r.url
                  and "application/json" in r.headers.get("content-type", ""),
        status_holder=status_holder,
    )
    return json_data, status_holder.get("http_status")

def fetch_shops_api_data(page, url):
    status_holder: dict = {}
    json_data = fetch_json_core(
        page,
        url,
        lambda r: "api.mercari.jp/v1/marketplaces/shops/products" in r.url
                  and "application/json" in r.headers.get("content-type", ""),
        status_holder=status_holder,
    )
    return json_data, status_holder.get("http_status")


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


# 削除済み/存在しない商品ページで実際に画面表示される文言
# ★ page.content() は Next.js のJSバンドル全体を含み、存在しないページ用の
#   文言も常にバンドルに埋め込まれているため、販売中ページでも "404" 等が
#   誤ヒットすることを実機検証で確認済み（false positiveでeBay削除が走る危険あり）。
#   そのため page.get_by_text() で「実際に画面に描画されているテキストか」を見る。
DELETED_PAGE_MARKERS = [
    "ページが見つかりませんでした",
    "お探しのページはURLが間違っているか",
    "商品が存在しません",
    "この商品は削除されました",
]


def _detect_deleted_from_html(page, item_id: str) -> bool:
    """
    items/get が取得できず、HTTPステータスからも確定できなかった場合の
    最終フォールバック判定。画面に実際に描画されているテキストのみを見る。
    """
    for marker in DELETED_PAGE_MARKERS:
        try:
            count = page.get_by_text(marker).count()
        except Exception as e:
            print(f"[FALLBACK] item_id={item_id} get_by_text({marker!r}) 判定失敗: {e}")
            continue

        if count > 0:
            print(f"[FALLBACK] item_id={item_id} 削除判定 marker='{marker}' (visible_count={count})")
            return True

    print(f"[FALLBACK] item_id={item_id} 削除マーカーなし（画面表示テキストで未検出）")
    return False


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
    item_id = url.split("/")[-1]

    for attempt in range(max_retries):
        res, http_status = fetch_mercari_api_data(page, url)

        # ===== DEBUG LOG START =====
        try:
            if res is None:
                print(f"[DEBUG] item_id={item_id} res=None http_status={http_status}")
            else:
                result = res.get("result")
                error_codes = [e.get("code") for e in res.get("errors", [])] if res.get("errors") else []

                print(f"[DEBUG] item_id={item_id} result={result} errors={error_codes}")
        except Exception as e:
            print(f"[DEBUG ERROR] {e}")
        # ===== DEBUG LOG END =====

        # ===== res=None のときは 404/削除フォールバック判定 =====
        if res is None:
            print(f"[FALLBACK] item_id={item_id} items/get 取得不可 → 削除判定フォールバック開始")

            # 1. まずページ遷移時のHTTPステータスで確定判定（最も確実）
            if http_status == 404:
                print(f"[RESULT] item_id={item_id} status=削除 (HTTPステータス404)")
                return "削除", None

            # 2. HTTPステータスで確定できない場合は、画面表示テキストで確認
            if _detect_deleted_from_html(page, item_id):
                print(f"[RESULT] item_id={item_id} status=削除 (画面表示テキスト判定)")
                return "削除", None

        status, price = _parse_status_from_res(res)

        if status != "判定不可":
            print(f"[RESULT] item_id={item_id} status={status} price={price}")
            return status, price

        # --- リトライ ---
        if attempt < max_retries - 1:
            print(f"[RETRY] item_id={item_id} attempt={attempt} → 再試行")
            time.sleep(1)
            page.close()
            page = page.context.new_page()
            apply_default_route_filter(page)
            print(f"[RETRY] item_id={item_id} route再設定完了")
            continue
        break

    print(f"[RESULT] item_id={item_id} status=判定不可 (最終)")
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
        log_listings_change("DELETE", account, listing_id, vendor_item_id, reason)




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

    if resp and resp.get("error") == "offer_not_found_for_sku":

        # trx.vendor_item.status = '判定不能'

        print(
            f"[判定不能] sku={vendor_item_id} "
            f"listing_id={listing_id} "
            f"account={account} "
            f"reason=offer_not_found_for_sku",
            flush=True,
        )

        return



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