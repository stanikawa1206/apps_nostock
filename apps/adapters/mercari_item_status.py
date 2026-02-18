from __future__ import annotations
import re
import time
from typing import Literal, Optional, Tuple, Dict, Any


import pyodbc
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timezone, timedelta

# ★ eBay削除API ← 波線エラーの正体はコレ
from apps.adapters.ebay_api import delete_item_from_ebay
from apps.adapters.ebay_api import update_ebay_price

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
def detect_status_from_mercari(driver: webdriver.Chrome) -> tuple[Status, Optional[int]]:
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
    except Exception:
        pass

    time.sleep(0.6)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    main = soup.select_one("#main") or soup

    # 1) 削除
    for el in main.select("p"):
        t = el.get_text(strip=True)
        if t in {"該当する商品は削除されています。", "ページが見つかりませんでした"}:
            return "削除", None

    # 2) 販売中
    for el in main.select('button, a, [role="button"]'):
        if el.get_text(" ", strip=True) == "購入手続きへ":
            return "販売中", extract_price_jpy_from(main)

    # 3) オークション
    for el in main.select('button, a, [role="button"]'):
        if el.get_text(" ", strip=True) == "入札する":
            return "オークション", None

    # 4) 売り切れ
    for el in main.select('button, a, [role="button"]'):
        if el.get_text(" ", strip=True) == "売り切れました":
            return "売り切れ", None

    # コメント不可でも売り切れ
    whole_text = soup.get_text(" ", strip=True)
    if "※売り切れのためコメントできません" in whole_text:
        return "売り切れ", None

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

    res = delete_item_from_ebay(account, listing_id)

    ok = bool(res.get("success")) or res.get("note") in {
        "already_deleted",
        "already_ended",
    }

    if ok:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE trx.listings
                   SET is_deleted = 1,
                       deleted_at = ?
                 WHERE listing_id = ?
                   AND account = ?
            """, (datetime.now(), listing_id, account))
        conn.commit()

        print(f"[LOGICAL DELETE] {listing_id=}")


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

        resp = update_ebay_price(
            account,
            listing_id,
            usd,
            sku=vendor_item_id,
            debug=True,
        )

        if resp and resp.get("success"):
            did_update = True
            break

        if not _is_transient_inventory_error(resp or {}):
            break

    if not did_update:
        print(
            f"[WARN] eBay価格更新失敗 sku={vendor_item_id} listing_id={listing_id} account={account} resp={resp}",
            flush=True
        )
