# filename: stock_checker.py
# -*- coding: utf-8 -*-

from __future__ import annotations
import json, re, time, random, argparse, sys, traceback, os
from typing import Literal, Optional, List, Dict

from bs4 import BeautifulSoup
from selenium import webdriver  # 型注釈用
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ===== UTF-8 出力の強制（絵文字/日本語の安全化） =====
if os.name == "nt" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
        sys.stderr.reconfigure(encoding="utf-8", line_buffering=True)
    except Exception:
        pass

# ===== 軽量ロガー & セーフクローズ =====
from datetime import datetime
def log_ctx(msg: str) -> None:
    """時刻付きログ（標準出力）。エンコード例外は潰す。"""
    try:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")
    except Exception:
        try:
            print(str(msg))
        except Exception:
            pass

def safe_quit(driver: Optional[webdriver.Chrome]) -> None:
    """Selenium driver を安全終了"""
    if driver is None:
        return
    try:
        driver.quit()
    except Exception:
        pass

# ===== パス設定 & インポート =====
# listings パッケージを解決するためのパス追加
sys.path.extend([r"D:\apps_nostock", r"D:\apps_nostock\common"])

# utils は DB/価格計算のみ
from utils import get_sql_server_connection, compute_start_price_usd
# eBay API は listings 側
from listings.ebay_api import delete_item_from_ebay, update_ebay_price
# ドライバは共通ヘルパから
from scrape_utils import build_driver

# ===================== 設定 =====================
TEST_MODE = True
TEST_URLS = ["NihoUpm2K54ijtstUBScd7","2JFzEwxUrqjTArr4mhoT7bQ","2JFzEwxUrqjTArr4mhoT7C"]

HEADLESS = True
TIMEOUT  = 12
Status = Literal["販売中", "売り切れ", "削除", "公開停止", "オークション", "判定不可"]

RATE = {
    "detail": (2.5, 5.0),
    "cooldown_every": 60,
    "cooldown_sleep": (45, 90),
    "retry_waits": [1.0, 2.0, 4.0],
}

# ===================== ユーティリティ =====================
def human_sleep(a: float, b: float):
    time.sleep(random.uniform(a, b))

# ===================== DB I/O =====================
def load_mercari_targets_from_db(limit: Optional[int] = None) -> List[Dict[str, str]]:
    """
    listings と vendor_item を (vendor_name, vendor_item_id) でJOIN。
    vendor_item.status がブランク（NULL or 空文字）のものだけ対象。
    返却: {url, sku(=vendor_item_id), account, ebay_item_id(=listing_id), vendor_name}
    """
    sql = """
        SELECT
            l.listing_id,
            l.account,
            l.vendor_item_id,
            l.vendor_name
        FROM [trx].[listings] AS l
        INNER JOIN [trx].[vendor_item] AS v
            ON v.vendor_name    = l.vendor_name
           AND v.vendor_item_id = l.vendor_item_id
        WHERE l.vendor_name = N'メルカリshops'
        ORDER BY l.start_time DESC
    """

    conn = get_sql_server_connection()
    try:
        out: List[Dict[str, str]] = []
        with conn.cursor() as cur:
            cur.execute(sql)
            for row in cur:
                ebay_item_id   = str(row[0]).strip()  # listings.listing_id
                account        = str(row[1]).strip()
                vendor_item_id = str(row[2]).strip()  # MercariのmXXXX
                vendor_name    = str(row[3]).strip()
                if vendor_name == "メルカリshops":
                    url = f"https://jp.mercari.com/shops/product/{vendor_item_id}"
                else:
                    url = f"https://jp.mercari.com/item/{vendor_item_id}"
                out.append({
                    "url": url,
                    "sku": vendor_item_id,
                    "account": account,
                    "ebay_item_id": ebay_item_id,
                    "vendor_name": vendor_name,
                })
                if limit and len(out) >= limit:
                    break
        return out
    finally:
        conn.close()

def delete_ebay_listing_record(conn, ebay_item_id: str, account: str, vendor_name: str) -> None:
    """ listings から eBay リスティングを削除（listing_idで一致） """
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM [trx].[listings]
             WHERE listing_id = ? AND account = ? AND vendor_name = ?
        """, (ebay_item_id, account, vendor_name))
    conn.commit()

def update_vendor_item_status(conn, vendor_name: str, sku: str, status: str) -> None:
    """ vendor_item の status を更新（sku=vendor_item_id） """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE [trx].[vendor_item]
               SET status = ?, last_checked_at = SYSDATETIME()
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (status, vendor_name, sku))
    conn.commit()

# ===================== Price Extractor =====================
PRICE_RE = re.compile(r"[¥￥]\s*([0-9,]+)")

def extract_price_jpy_from(main: BeautifulSoup) -> Optional[int]:
    """画面に出ている販売価格（¥12,345 など）を素朴に抽出。"""
    for sel in ["#main", '[data-testid="price"]', '[class*="price"]', "h2", "p", "span", "div"]:
        for el in main.select(sel):
            t = el.get_text(" ", strip=True)
            m = PRICE_RE.search(t)
            if m:
                return int(m.group(1).replace(",", ""))
    t = main.get_text(" ", strip=True)
    m = PRICE_RE.search(t)
    return int(m.group(1).replace(",", "")) if m else None

# ===================== Mercari 判定 =====================
def detect_status_from_mercari(driver: webdriver.Chrome) -> tuple[Status, Optional[int]]:
    try:
        WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
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

    return "判定不可", None

def detect_status_from_mercari_shops(driver: webdriver.Chrome) -> tuple[Status, Optional[int]]:
    # 読み込み待ち
    try:
        WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
    except Exception:
        pass
    time.sleep(0.6)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    main = soup.select_one("#main") or soup

    # 削除・非公開系
    whole = main.get_text(" ", strip=True)
    if any(t in whole for t in ["ページが見つかりませんでした", "該当する商品は削除されています。", "公開停止"]):
        return "削除", None

    # 価格
    price = extract_price_jpy_from(main)

    # 1) 先に売り切れ
    for el in main.select('button, a, [role="button"]'):
        txt = el.get_text(" ", strip=True)
        if txt.startswith("売り切れ"):
            return "売り切れ", None

    # 2) 次に購入ボタン（disabledでないことを確認）
    for el in main.select('button, a, [role="button"]'):
        txt = el.get_text(" ", strip=True)
        if txt == "購入手続きへ":
            classes = " ".join(el.get("class", [])).lower()
            if el.has_attr("disabled") or el.get("aria-disabled") == "true" or "disabled" in classes or "isdisabled" in classes:
                continue
            return "販売中", price

    return "判定不可", price


def get_status(driver: webdriver.Chrome, url: str) -> tuple[Status, Optional[int]]:
    driver.get(url)
    host_path = re.sub(r"^https?://", "", url)
    if "/shops/product/" in host_path:
        return detect_status_from_mercari_shops(driver)
    if "mercari.com" in host_path:
        return detect_status_from_mercari(driver)
    return "判定不可", None


# ===================== Price Sync I/O（vendor_item） =====================
def get_vendor_item_price(conn, vendor_name: str, sku: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT price
              FROM [trx].[vendor_item]
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (vendor_name, sku))
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

def update_vendor_item_price_and_status(conn, vendor_name: str, sku: str,
                                        price_jpy: Optional[int], status: str) -> None:
    """price_jpy が None のときは status だけ更新"""
    if price_jpy is None:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE [trx].[vendor_item]
                   SET status = ?, last_checked_at = SYSDATETIME()
                 WHERE vendor_name = ? AND vendor_item_id = ?
            """, (status, vendor_name, sku))
        conn.commit()
        return

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE [trx].[vendor_item]
               SET status = ?, price = ?, last_checked_at = SYSDATETIME()
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (status, price_jpy, vendor_name, sku))
    conn.commit()

def _is_transient_inventory_error(resp: dict | None) -> bool:
    if not resp or resp.get("success"):
        return False
    raw = resp.get("raw") or {}
    errors = ((raw.get("putOffer") or {}).get("errors") or []) or raw.get("errors") or []
    msgs = " ".join(str(e.get("message","")) for e in errors if isinstance(e, dict)).lower()
    codes = {int(e.get("errorId")) for e in errors if isinstance(e, dict) and str(e.get("errorId","")).isdigit()}
    return (25001 in codes) or ("internal error" in msgs)

# ===================== メイン =====================


def run(limit: Optional[int] = None, urls: Optional[list[str]] = None):
    log_ctx("[DRIVER] building...")
    driver = None
    conn = None
    total = deleted = updated = failed = 0

    try:
        driver = build_driver()  # ← ここで作る（失敗は except へ）
        conn = get_sql_server_connection()

        # 手動URL（テスト）か、DBから対象取得
        if urls:
            manual = []
            for u in urls:
                m = re.search(r"(m\d{8,})", u)
                if not m:
                    continue
                sku = m.group(1)
                manual.append({
                    "url": f"https://jp.mercari.com/item/{sku}",
                    "sku": sku,
                    "account": "",
                    "ebay_item_id": "",
                    "vendor_name": "メルカリ",
                })
            rows = manual
        else:
            rows = load_mercari_targets_from_db(limit=limit)

        if not rows:
            print("[WARN] 対象がありません（TEST/DB）")
            return
        else:
            print(f"[INFO] 処理対象 {len(rows)} 件を検出しました（vendor_name='メルカリ'）")

        for r in rows:
            url          = r["url"]
            sku          = r["sku"]                # MercariのmXXXX（= listings.vendor_item_id）
            account      = r["account"]
            ebay_item_id = r["ebay_item_id"]       # listings.listing_id
            vendor_name  = r["vendor_name"]

            status: Status = "判定不可"
            price_jpy: Optional[int] = None

            # ステータス取得（簡易リトライ）
            for i, wait in enumerate([0.0] + RATE["retry_waits"]):
                if i > 0:
                    time.sleep(wait)
                try:
                    status, price_jpy = get_status(driver, url)
                    if status != "判定不可":
                        break
                except Exception as e:
                    if i == len(RATE["retry_waits"]):
                        print(f"[ERR] get_status失敗: {url} ({e})")
                        failed += 1

            print(f"[STATUS] {url} -> {status} (price_jpy={price_jpy})")

            # ===== 販売中：価格差分チェック →（必要なら）eBay改定/削除 → DB反映 =====
            if status == "販売中" and price_jpy is not None:
                try:
                    old_price = get_vendor_item_price(conn, vendor_name, sku)

                    if (old_price is None) or (old_price != price_jpy):
                        new_price_usd = compute_start_price_usd(price_jpy)

                        if new_price_usd is None:
                            print(f"[PRICE] {sku}: {old_price} -> {price_jpy} JPY / 目標外レンジ ⇒ eBay出品を終了")
                            if ebay_item_id:
                                try:
                                    res = delete_item_from_ebay(account, ebay_item_id)
                                    ok = bool(res.get("success")) or res.get("note") in {"already_deleted", "already_ended"}
                                    if ok:
                                        delete_ebay_listing_record(conn, ebay_item_id, account, vendor_name)
                                        deleted += 1
                                    else:
                                        print(f"[WARN] eBay削除失敗 listingId={ebay_item_id} resp={res}")
                                except Exception as e:
                                    print(f"[ERR] eBay削除処理で例外 listingId={ebay_item_id}: {e}")
                            else:
                                print(f"[WARN] eBay削除不可（listing_idなし） sku={sku}")

                            update_vendor_item_price_and_status(conn, vendor_name, sku, price_jpy, status)
                            updated += 1
                        else:
                            did_update_ebay = False
                            resp = None
                            for wait in [0, 2, 6, 15]:
                                if wait: time.sleep(wait)
                                resp = update_ebay_price(account, ebay_item_id, new_price_usd, sku=sku, debug=True)
                                if resp and resp.get("success"):
                                    did_update_ebay = True
                                    break
                                if not _is_transient_inventory_error(resp):
                                    break

                            if did_update_ebay or not ebay_item_id:
                                update_vendor_item_price_and_status(conn, vendor_name, sku, price_jpy, status)
                                updated += 1
                                if did_update_ebay:
                                    print(f"[PRICE] {sku}: {old_price} -> {price_jpy} JPY / eBay {new_price_usd} USD を更新")
                                else:
                                    print(f"[PRICE] {sku}: {old_price} -> {price_jpy} JPY / eBay改定なし（listing_idなし）")
                            else:
                                update_vendor_item_price_and_status(conn, vendor_name, sku, None, status)
                                updated += 1
                                print(f"[WARN] eBay価格更新失敗 listingId={ebay_item_id} resp={resp}")
                                print(f"[PRICE] {sku}: {old_price} -> {price_jpy} JPY / eBay {new_price_usd} USD （未更新・DB価格は据え置き）")
                    else:
                        update_vendor_item_price_and_status(conn, vendor_name, sku, None, status)
                        updated += 1

                except Exception as e:
                    print(f"[WARN] 価格差分反映で例外 sku={sku}: {e}")

            else:
                # 販売中ではない：status のみ更新
                try:
                    update_vendor_item_price_and_status(conn, vendor_name, sku, None, status)
                    updated += 1
                except Exception as e:
                    print(f"[WARN] vendor_item更新失敗 sku={sku}: {e}")

            # ===== 終了系は eBay 出品も終了 & listings から削除 =====
            if status in {"削除", "オークション", "売り切れ", "公開停止"}:
                if ebay_item_id:
                    try:
                        res = delete_item_from_ebay(account, ebay_item_id)
                        ok = bool(res.get("success")) or res.get("note") in {"already_deleted", "already_ended"}
                        if ok:
                            delete_ebay_listing_record(conn, ebay_item_id, account, vendor_name)
                            deleted += 1
                        else:
                            print(f"[WARN] eBay削除失敗 listingId={ebay_item_id} resp={res}")
                    except Exception as e:
                        print(f"[ERR] eBay削除処理で例外 listingId={ebay_item_id}: {e}")
                else:
                    print(f"[WARN] eBay削除不可（listing_idなし） sku={sku}")

            total += 1
            human_sleep(*RATE["detail"])
            if total % RATE["cooldown_every"] == 0:
                human_sleep(*RATE["cooldown_sleep"])

        print(f"\n✅ 完了: 対象{total}件 / vendor_item更新{updated}件 / eBay削除{deleted}件 / 失敗{failed}件")

    except Exception:
        traceback.print_exc()
        raise
    finally:
        safe_quit(driver)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    urls: List[str] = []

    if TEST_MODE:
        # 裸ID → URL化（メルカリ / Shops 自動判定）
        for s in TEST_URLS:
            s = s.strip()
            if re.fullmatch(r"m\d{8,}", s):
                urls.append(f"https://jp.mercari.com/item/{s}")
            elif re.fullmatch(r"[A-Za-z0-9]{10,}", s):
                urls.append(f"https://jp.mercari.com/shops/product/{s}")
        print(f"[TEST] TEST_MODE=True / Built URLs ({len(urls)}): {urls}")
        run(urls=urls)       # ← URLを渡す
    else:
        run(urls=None)       # ← 本番はDB全件（関数内で取得）
