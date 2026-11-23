from __future__ import annotations
import re
import time
from typing import Literal, Optional, Tuple, Dict, Any
import requests
from typing import Optional
import json
from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

import pyodbc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timezone, timedelta
from decimal import Decimal

# ★ eBay削除API ← 波線エラーの正体はコレ
from apps.adapters.ebay_api import delete_item_from_ebay
from apps.adapters.ebay_api import (
    _delete_listing_after_price_failure,
    delete_items_from_ebay_batch,
    end_listing_by_id_verified,
    get_listing_status,
    mark_listing_deleted_if_ended,
    update_ebay_price_rest,
)
from apps.common.utils import log_listings_change, send_mail
from apps.inventory import ops_stats

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

# fetch_json_core のスクロール(scrollTo)の上限（ミリ秒）。通常は即座に終わる処理。
SCROLL_TIMEOUT_MS = 5000

# _detect_deleted_from_html の削除マーカー1文言あたりの確認上限（ミリ秒）。
# 呼ばれる時点でページは読み込み済み（goto 後に items/get を最大10秒待った後）なので、
# 以前の count()（その瞬間の DOM を数えるだけ）と同じ判定に十分な時間。
DELETED_MARKER_TIMEOUT_MS = 2000

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
            # page.evaluate() はタイムアウト引数を持たず、ページが応答しないと永久に戻らない
            # （2026-09-13 に x162-43-39-209 で実際に発生）。wait_for_function は同じ関数を
            # ページ内で実行して true が返れば即座に戻り、応答が無ければ timeout で例外になる。
            page.wait_for_function(
                "() => { window.scrollTo(0, document.body.scrollHeight); return true; }",
                polling=100,
                timeout=SCROLL_TIMEOUT_MS,
            )
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

    以前の page.get_by_text(marker).count() はタイムアウトを持たず、ページが応答しないと
    永久に戻らなかった（2026-09-30 に x162-43-15-160 / x162-43-39-209 の check_remaining が
    ここで4時間以上停止）。そのため「その文言の要素が DOM に存在するか」（count() > 0 と同じ意味）を
    タイムアウト付きの wait_for(state="attached") で確認する。
    時間内に見つからなかった場合（ページが応答しない場合を含む）は削除とは判定しない。
    """
    for marker in DELETED_PAGE_MARKERS:
        try:
            page.get_by_text(marker).first.wait_for(state="attached", timeout=DELETED_MARKER_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            continue
        except Exception as e:
            print(f"[FALLBACK] item_id={item_id} get_by_text({marker!r}) 判定失敗: {e}")
            continue

        print(f"[FALLBACK] item_id={item_id} 削除判定 marker='{marker}'")
        return True

    print(f"[FALLBACK] item_id={item_id} 削除マーカーなし（画面表示テキストで未検出、または応答なし）")
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
            ops_stats.note_retry()
            time.sleep(1)
            page.close()
            page = page.context.new_page()
            apply_default_route_filter(page)
            print(f"[RETRY] item_id={item_id} route再設定完了")
            continue
        break

    print(f"[RESULT] item_id={item_id} status=判定不可 (最終)")
    ops_stats.note_cause("items/get 応答なし" if res is None else f"items/get 状態不明 http={http_status}")
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
    return judge_shops_product_html(driver.page_source, url=getattr(driver, "current_url", None))


# 購入ボタンの文言。数量を選べる商品（在庫が複数）は「購入へ」になる（2026-11-15 確認: 2JUgYuUVZySYRbdUCEPW8K、在庫残り3点）。
SHOPS_BUY_BUTTON_TEXTS = ("購入手続きへ", "購入へ")
# この商品自身の売り切れ表示（data-testid="out-of-stock" の要素）の文言（2026-11-15 確認）
SHOPS_SOLD_TEXTS = ("売り切れ", "在庫切れ")
SHOPS_DELETED_TEXTS = ("ページが見つかりませんでした", "該当する商品は削除されています。")
# 画面の幅が狭いときに購入ボタンが置かれる、画面下の固定欄（#main の外）（2026-11-16 確認、780×580）
SHOPS_STICKY_BUTTON_TESTID = "mobile-bottom-sticky-button"


def judge_shops_product_html(html: str, url: Optional[str] = None) -> tuple[Status, Optional[int]]:
    """
    メルカリShops の商品ページの HTML から状態を判定する（detect_status_from_mercari_shops の本体）。

    売り切れにするのは、この商品自身の売り切れ表示（data-testid="out-of-stock"、文言「売り切れ」「在庫切れ」）が
    あり、かつ有効な購入ボタンが無いときだけ。買えるかどうかが分からない場合は「判定不可」を返す
    （check_remaining は出品を維持して次回に再確認し、出品前の確認は出品しない）。
      - 削除表示（「ページが見つかりませんでした」等） → 削除
      - 価格が取れない → RuntimeError（従来どおり。出品前の確認では解析失敗）
      - 有効な購入ボタン（「購入手続きへ」「購入へ」）があり、売り切れ表示が無い → 販売中
      - 売り切れ表示があり、有効な購入ボタンが無い → 売り切れ（ボタンは無効表示か、無いか）
      - それ以外（ボタンも売り切れ表示も無い、両方ある、知らないボタン等） → 判定不可
    2026-11-15 までは「購入手続きへ」のボタンが見つからないと無条件に売り切れにしていたため、
    「購入へ」の販売中の商品を売り切れと誤判定していた（check_remaining では eBay の出品終了につながる）。
    「他の商品」欄（data-testid="shopOtherItem…"）は商品詳細の枠の中にあるが、別の商品なので判定に使わない。
    """
    soup = BeautifulSoup(html or "", "html.parser")
    main = soup.select_one("#main") or soup

    whole = main.get_text(" ", strip=True)
    if any(t in whole for t in SHOPS_DELETED_TEXTS):
        return "削除", None

    price_el = main.select_one('[data-testid="product-price"]')
    if not price_el:
        raise RuntimeError("価格要素（product-price）が見つかりませんでした")

    t = price_el.get_text(strip=True)
    m = PRICE_RE.search(t)
    if not m:
        raise RuntimeError(f"価格抽出失敗: {t}")

    price = int(m.group(1).replace(",", ""))

    for other in main.select('[data-testid^="shopOtherItem"]'):
        other.decompose()

    # 画面の幅が狭いと（出品側の Chrome は 780×580）、購入ボタンは #main の外の、
    # body 直下の画面下に固定された欄（data-testid="mobile-bottom-sticky-button"）に表示される。
    # この欄はこの商品自身のボタンなので #main と合わせて探す（ページ全体は探さない）。
    # 2026-11-16 は #main の中だけを探していたため、出品側では Shops の候補がすべて「判定不可」になった。
    button_areas = [main]
    if main is not soup:
        button_areas += soup.select(f'[data-testid="{SHOPS_STICKY_BUTTON_TESTID}"]')
    buy_buttons = [
        b for area in button_areas for b in area.select("button")
        if " ".join(b.get_text(" ", strip=True).split()) in SHOPS_BUY_BUTTON_TEXTS
    ]
    enabled = [
        b for b in buy_buttons
        if not b.has_attr("disabled") and (b.get("aria-disabled") or "").lower() != "true"
    ]
    sold_marks = [
        el for el in main.select('[data-testid="out-of-stock"]')
        if any(s in el.get_text(" ", strip=True) for s in SHOPS_SOLD_TEXTS)
    ]

    if enabled and not sold_marks:
        return "販売中", price
    if sold_marks and not enabled:
        return "売り切れ", None

    print(
        f"[SHOPS_STATUS] 判定不可 url={url} 購入ボタン={[b.get_text(' ', strip=True) for b in buy_buttons]} "
        f"有効={len(enabled)} 売り切れ表示={[el.get_text(' ', strip=True) for el in sold_marks]}",
        flush=True,
    )
    return "判定不可", None


def shops_api_out_of_stock(js: Optional[dict], product_id: str) -> Optional[bool]:
    """
    メルカリShops の商品詳細API（/v1/marketplaces/shops/products/{id}）の応答から在庫切れかを返す。
      True  … この商品のすべての種類(variants)が quantity=0 か soldOut=true（在庫切れと明確に分かる）
      False … quantity>0 かつ soldOut=false の種類が1つ以上ある（在庫あり）
      None  … 応答が無い・別の商品の応答・種類が無いなど、判断できない
    2026-11-19 確認: 在庫ありは quantity '1'〜'3'・soldOut false、ページの購入ボタンが押せず売り切れ表示も
    無い商品は quantity '0'（soldOut は true のことも false のこともある）。
    """
    if not js or js.get("name") != product_id:
        return None
    variants = (js.get("productDetail") or {}).get("variants") or []
    if not variants:
        return None
    if all(v.get("soldOut") is True or str(v.get("quantity")) == "0" for v in variants):
        return True
    if any(v.get("soldOut") is False and int(v.get("quantity") or 0) > 0 for v in variants):
        return False
    return None


def fetch_shops_product_json(page, product_id: str) -> Optional[dict]:
    """Playwright でShopsの商品ページを開き、その商品自身の商品詳細APIの応答だけを拾う（他の商品のAPIは拾わない）。"""
    return fetch_json_core(
        page,
        f"https://jp.mercari.com/shops/product/{product_id}",
        lambda r: f"/v1/marketplaces/shops/products/{product_id}?" in r.url
                  and "application/json" in r.headers.get("content-type", ""),
    )

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

    try:
        res = delete_item_from_ebay(account, vendor_item_id)
    except Exception as e:
        # 通信エラー等は従来どおり例外のまま（DB は変えず次回に再試行）。集計と未解決の記録だけ残す
        ops_stats.count("出品終了", vendor_name, "失敗", cause=type(e).__name__, account=account)
        ops_stats.issue("出品終了失敗", vendor_name, vendor_item_id, f"{reason}: {type(e).__name__}: {e}",
                        account=account, listing_id=listing_id)
        raise
    fail_cause = None

    ok = bool(res.get("success")) or res.get("note") in {
        "already_deleted",
        "already_ended",
    }

    if res.get("success") and not (
        res.get("offer_status") == "PUBLISHED" and str(res.get("offer_listing_id") or "") == listing_id
    ):
        # offer削除で出品が終了するのは、その出品に公開済み(PUBLISHED)の offer だけ。
        # publishOffer が一時エラーを返しても出品が作成されたケースは offer が UNPUBLISHED のままで、
        # offer削除だけでは eBay の出品が残り、DBだけ削除済みになっていた（2026-10-22 BUZZ② 800719496487）。
        # listing_id 指定で終了し、GetItem で終了を確認できた時だけ削除済みにする（確認できなければ管理対象に残す）。
        end_res = delete_items_from_ebay_batch(account, [listing_id])
        status = get_listing_status(account, listing_id)
        ok = bool(status.get("ended"))
        if not ok:
            fail_cause = f"offer削除後に終了未確認 status={status.get('listing_status') or status.get('error')}"
        print(
            f"[DELETE_{'VERIFIED' if ok else 'NG'}] listing_id={listing_id} account={account} sku={vendor_item_id} "
            f"reason={reason} offer_status={res.get('offer_status')} offer_listing_id={res.get('offer_listing_id')} "
            f"end_result={end_res.get('results') or end_res.get('error_code')!r} status={status!r}"
            f"{'' if ok else ' → DB変更なし'}",
            flush=True,
        )

    elif not ok:
        # REST版の削除は offer が既に無い(=eBay側で終了済みの可能性がある)場合も失敗を返し、
        # note(already_deleted)も返さないため、DBが出品中のまま残り再試行でも直らなかった
        # （2026-10-19調査）。GetItem で終了を確認できた時だけ削除済みにする。
        # offer が無いまま eBay で出品中の出品（キャンセル後に eBay で出品し直された等）は、
        # 出品番号を指定して終了する（2026-11-19 以前は確認だけで終了せず、137892055692 が72回 残った）。
        fb = end_listing_by_id_verified(account, listing_id, vendor_item_id)
        ok = fb["ended"]
        if not ok:
            fail_cause = f"offerなし・出品番号での終了 {fb['how']} offer={res.get('error_code')}"
        print(
            f"[DELETE_{'VERIFIED' if ok else 'NG'}] listing_id={listing_id} account={account} sku={vendor_item_id} "
            f"reason={reason} delete_result={res!r} how={fb['how']} end_result={fb.get('end_result')!r} "
            f"status={fb['status']!r}{'' if ok else ' → DB変更なし'}",
            flush=True,
        )

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
        ops_stats.count("出品終了", vendor_name, "成功", cause=reason, account=account)
    else:
        # 仕入先で終了系なのに eBay で出品中のまま。次回の終了処理で再試行される。2時間以上続けば inventory_monitor が通知する
        ops_stats.count("出品終了", vendor_name, "失敗", cause=(fail_cause or "不明")[:100], account=account)
        ops_stats.issue("出品終了失敗", vendor_name, vendor_item_id, f"{reason}: {fail_cause}",
                        account=account, listing_id=listing_id)




def _is_transient_inventory_error(resp: Dict[str, Any]) -> bool:
    # update_ebay_price_rest は raw を返さないため、以前の raw.errors を見る判定は常に False で
    # 再試行が働いていなかった。一時的な失敗かは update_ebay_price_rest 側で判定して transient で返す。
    return bool(resp) and not resp.get("success") and bool(resp.get("transient"))


PRICE_PENDING_PREFIX = "価格更新失敗(再試行待ち)"
RETRY_AFTER_MAX_SEC = 60
# 値下げの失敗は赤字にならないため初回は通知せず、最初の失敗からこの時間を過ぎても失敗した時に1回だけ通知する
# （2026-10-24 eBay障害の約7分間に、値下げ失敗15件で警告メールが15通送られていた）。
PRICE_DOWN_NOTIFY_AFTER_SEC = 6 * 3600
# 値上げの失敗のうち eBay 側のシステム障害と明確に判断できるものは、単発なら次の在庫管理の再試行で直るため初回は通知せず、
# 最初の失敗からこの時間を過ぎても失敗した時（次のサイクルの再試行でも失敗した時）に1回だけ通知する
# （2026-11-23 BUZZ② getOffers HTTP 500/25001 は4台・24秒だけの障害で、値上げの要確認メールが1通送られていた）。
PRICE_UP_OUTAGE_CATEGORY = "値上げ・eBay障害"
PRICE_UP_OUTAGE_NOTIFY_AFTER_SEC = 3 * 3600
# getOffers の HTTP 5xx・通信エラー、PUT/publish の 25001、本文の無い失敗。25002 は本文が "System error" の時だけ。
_EBAY_SYSTEM_ERROR_RE = re.compile(
    r"^(getoffers_http_5\d\d:|getoffers_request_error:|(inventory_put|publish)_error_25001:|(inventory_put|publish)_failed$)"
)
_EBAY_SYSTEM_ERROR_25002_RE = re.compile(r"^(inventory_put|publish)_error_25002:")


def _is_ebay_system_error(error) -> bool:
    """update_ebay_price_rest のエラーが eBay 側のシステム障害と明確に判断できるか（商品ごとの原因でないもの）。"""
    s = str(error or "")
    if _EBAY_SYSTEM_ERROR_RE.match(s):
        return True
    return bool(_EBAY_SYSTEM_ERROR_25002_RE.match(s)) and "system error" in s.lower()


def _pending_notify_tag(error_message) -> Optional[str]:
    """
    error_message = "価格更新失敗(再試行待ち)[通知状態]:エラー" の通知状態を返す。再試行待ちでなければ None。
    通知状態の無い旧形式は、初回失敗時に通知済みのため "通知済:旧形式" とみなす。
    """
    s = str(error_message or "")
    if not s.startswith(PRICE_PENDING_PREFIX):
        return None
    rest = s[len(PRICE_PENDING_PREFIX):]
    if rest.startswith("[") and "]" in rest:
        return rest[1:rest.index("]")]
    return "通知済:旧形式"


def _record_price_update_pending(
    conn,
    account: str,
    listing_id: str,
    sku: str,
    error: str,
    *,
    category: str,
    mail_info: str,
) -> None:
    """
    価格更新できなかった出品中の行に失敗内容を記録する（is_deleted は変えず管理対象に残す）。
    error_at は最初に失敗した時刻のまま保持する。
    通知: 値上げ等は初回から、値下げは最初の失敗から PRICE_DOWN_NOTIFY_AFTER_SEC 経過後の失敗時に通知する。
    値上げの eBay 障害(PRICE_UP_OUTAGE_CATEGORY)は最初の失敗から PRICE_UP_OUTAGE_NOTIFY_AFTER_SEC 経過後の失敗時に通知する。
    同じ分類(category)で通知済みなら再通知しない。
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT error_message, error_at
              FROM trx.listings
             WHERE account = ?
               AND listing_id = ?
               AND is_deleted = 0
        """, (account, listing_id))
        row = cur.fetchone()
    if not row:
        return

    prev_tag = _pending_notify_tag(row[0])
    first_at = row[1] if prev_tag is not None and row[1] else datetime.now()
    notified = prev_tag is not None and prev_tag.startswith("通知済")

    if category == "値下げ":
        notify = (
            prev_tag is not None
            and not notified
            and (datetime.now() - first_at).total_seconds() >= PRICE_DOWN_NOTIFY_AFTER_SEC
        )
    elif category == PRICE_UP_OUTAGE_CATEGORY:
        # 値上げの失敗として通知済み（旧形式・「値上げ・一時的な障害」等を含む）なら、同じ失敗の続きなので送らない
        notify = (
            prev_tag is not None
            and not prev_tag.startswith("通知済:値上げ")
            and prev_tag != "通知済:旧形式"
            and (datetime.now() - first_at).total_seconds() >= PRICE_UP_OUTAGE_NOTIFY_AFTER_SEC
        )
    else:
        notify = prev_tag != f"通知済:{category}"

    if notify:
        tag = f"通知済:{category}"
    elif notified:
        tag = prev_tag
    else:
        tag = f"未通知:{category}"

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trx.listings
               SET error_message = ?,
                   error_at = ?
             WHERE account = ?
               AND listing_id = ?
               AND is_deleted = 0
        """, (f"{PRICE_PENDING_PREFIX}[{tag}]:{error}"[:4000], first_at, account, listing_id))
    conn.commit()

    print(f"[PRICE_PENDING] account={account} sku={sku} listing_id={listing_id} tag={tag} notify={notify}", flush=True)
    if notify:
        send_mail(
            f"【要確認】eBay価格更新失敗({category}) {account} {sku}",
            "eBayの価格更新に失敗しました。出品は管理対象のまま残し、次回の価格変更検知・在庫チェックで再試行します。\n"
            "値上げの失敗は赤字販売の恐れがあるため、続く場合は出品の終了を検討してください。\n\n"
            f"account: {account}\nlisting_id: {listing_id}\nsku: {sku}\n{mail_info}"
            f"初回失敗: {first_at:%Y-%m-%d %H:%M:%S}\n",
        )


def _get_listing_start_price(conn, account: str, listing_id: str) -> Optional[Decimal]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT start_price FROM trx.listings WHERE account = ? AND listing_id = ?
        """, (account, listing_id))
        row = cur.fetchone()
    return Decimal(str(row[0])) if row and row[0] is not None else None


def _clear_price_update_pending(conn, account: str, listing_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE trx.listings
               SET error_message = NULL, error_at = NULL
             WHERE account = ?
               AND listing_id = ?
               AND is_deleted = 0
               AND error_message LIKE ?
        """, (account, listing_id, PRICE_PENDING_PREFIX + "%"))
    conn.commit()

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

    retry_after = 0
    tries = 0
    for wait in [0, 2, 6, 15]:
        # 429 の場合は Retry-After を尊重する（ワーカーを長時間止めないよう上限あり）
        wait = max(wait, retry_after)
        if wait:
            time.sleep(wait)
        tries += 1

        try:
            resp = update_ebay_price_rest(
                account,
                vendor_item_id,
                usd,
                debug=True,
                expected_listing_id=listing_id,
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
        retry_after = min(int(resp.get("retry_after") or 0), RETRY_AFTER_MAX_SEC)

    if resp and resp.get("error") == "offer_not_found_for_sku":

        # trx.vendor_item.status = '判定不能'

        print(
            f"[判定不能] sku={vendor_item_id} "
            f"listing_id={listing_id} "
            f"account={account} "
            f"reason=offer_not_found_for_sku",
            flush=True,
        )
        # offer が無い出品は eBay側で終了済みのことがある。終了を確認できた時だけ削除済みにする。
        if mark_listing_deleted_if_ended(
            account, listing_id, vendor_item_id, "eBay終了確認", "offer_not_found_for_sku"
        ):
            return

    if did_update:
        _clear_price_update_pending(conn, account, listing_id)
        ops_stats.count("価格更新", vendor_name, "成功", account=account, attempts=tries, retried=int(tries > 1))
        return

    resp = resp or {}
    error = resp.get("error") or "exception"
    # 集計の原因: eBay のエラー番号（あれば）と一時的な障害かどうか。個別商品のエラーと eBay 全体の障害を分けて数える
    _code = re.search(r"\b(2\d{4}|10007|518|5\d\d)\b", str(error))
    ops_stats.count("価格更新", vendor_name, "失敗",
                    cause=f"{'一時的' if resp.get('transient') or not resp else '個別'}:{_code.group(1) if _code else str(error)[:40]}",
                    account=account, attempts=tries, retried=int(tries > 1))

    # 値上げか値下げか: getOffer で取れたeBayの現在価格、取れなければDBの start_price と比べる。
    # どちらも無ければ値上げ扱い（赤字の恐れがある側）にする。
    if resp.get("old_price"):
        old_usd = Decimal(resp["old_price"])
    else:
        old_usd = _get_listing_start_price(conn, account, listing_id)
    is_up = old_usd is None or Decimal(str(usd)) > old_usd

    # 値上げが販売上限(25002)で失敗した場合は、まず LIMIT 対応と同じ基準で古い出品を1件ずつ終了して
    # 値上げを再試行する（make_listing_space.free_listing_space_for_price_up）。通らなければ下の終了処理へ進む。
    space = None
    if is_up and resp.get("selling_limit"):
        from apps.publish.make_listing_space import free_listing_space_for_price_up
        space = free_listing_space_for_price_up(
            account, listing_id, vendor_item_id,
            lambda: update_ebay_price_rest(account, vendor_item_id, usd, debug=True, expected_listing_id=listing_id),
        )
        if space["resp"] and space["resp"].get("success"):
            print(f"[PRICE_UP_SPACE][OK] account={account} sku={vendor_item_id} listing_id={listing_id} "
                  f"終了した古い出品={space['ended']} 再試行={space['attempts']}回", flush=True)
            _clear_price_update_pending(conn, account, listing_id)
            return
        if space["resp"]:
            resp = space["resp"]
            error = resp.get("error") or "exception"

    # 値上げが販売上限(25002)で失敗した場合は、既存の安全な終了処理で終了する（値下げなら出品維持）。
    # 重複出品(25002のうち eBay が本文で重複と明示したもの)は再試行しても通らず、価格が古いまま残るため、
    # 値上げ・値下げに関係なく終了する（2026-10-26 谷川② 227581813798 は値上げが反映されないまま再試行を繰り返していた）。
    end_reason = resp.get("end_reason")
    if is_up and resp.get("selling_limit"):
        end_reason = "販売上限"
        resp["db_deleted"] = _delete_listing_after_price_failure(account, vendor_item_id, error, "価格更新失敗")
    elif resp.get("duplicate_listing"):
        end_reason = "重複出品"
        resp["db_deleted"] = _delete_listing_after_price_failure(account, vendor_item_id, error, "価格更新失敗")

    mail_info = (
        f"旧価格(USD): {old_usd if old_usd is not None else '不明'}\n"
        f"新価格(USD): {usd}（{'値上げ' if is_up else '値下げ'}）\n"
        f"error: {error}\n"
    )
    if space is not None:
        mail_info += (
            f"販売上限対応: 古い出品(watchers=0)を{len(space['ended'])}件終了 {space['ended']} / "
            f"値上げ再試行{space['attempts']}回 / 待ち{space['waited_sec']}秒"
            f"{' / ' + space['note'] if space['note'] else ''}\n"
        )

    if resp.get("db_deleted"):
        # 終了済みofferのため価格更新(再出品)を行わず、GetItem で終了を確認してDBも削除済みにした。
        # こちらから終了した場合(販売上限・重複出品・IP違反)は結果を通知する。
        if end_reason:
            send_mail(
                f"【通知】eBay価格更新失敗のため出品終了({end_reason}) {account} {vendor_item_id}",
                "eBayの価格更新に失敗したため出品を終了し、GetItem で終了を確認してDBも削除済みにしました。\n\n"
                f"account: {account}\nlisting_id: {listing_id}\nsku: {vendor_item_id}\n{mail_info}"
                "出品終了: 成功\n",
            )
        return

    # 価格更新できず、eBayで出品中の可能性が残る場合は例外にする。
    # 呼び出し元は vendor_item.price を更新しないため、次回も価格変更として再試行される
    # （以前は listing_not_active / offer_not_found で例外にせず戻り、再試行されなかった）。
    if "db_deleted" in resp:
        error += " / eBayの出品終了を確認できずDB削除なし"
    if end_reason:
        category = "出品終了未確認"
        mail_info += f"出品終了: 失敗（{end_reason}のため終了を試みたが、eBayの終了を確認できずDB削除なし）\n"
    elif not is_up:
        category = "値下げ"
        mail_info += "出品終了: 未実施（出品維持・再試行待ち）\n"
    elif resp.get("transient") and _is_ebay_system_error(error):
        category = PRICE_UP_OUTAGE_CATEGORY
        mail_info += ("出品終了: 未実施（eBay側のシステム障害のため出品維持・再試行待ち。"
                      f"最初の失敗から{PRICE_UP_OUTAGE_NOTIFY_AFTER_SEC // 3600}時間以上 失敗が続いたため通知）\n")
    elif resp.get("transient"):
        category = "値上げ・一時的な障害"
        mail_info += "出品終了: 未実施（一時的な障害のため出品維持・再試行待ち）\n"
    else:
        category = "値上げ・要確認"
        mail_info += "出品終了: 未実施（原因を判断できないため出品維持・再試行待ち）\n"
    _record_price_update_pending(
        conn, account, listing_id, vendor_item_id, error, category=category, mail_info=mail_info
    )
    raise RuntimeError(
        f"eBay価格更新失敗 sku={vendor_item_id} listing_id={listing_id} account={account} error={error}"
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