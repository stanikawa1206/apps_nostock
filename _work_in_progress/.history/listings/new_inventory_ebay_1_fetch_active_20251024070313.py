# -*- coding: utf-8 -*-
r"""
inventory_ebay_1_fetch_active.py (with price-sync & listings integration)
- 2つのMercari検索URLを順にクロール
- ページ送り：page_token=v1:{0,1,2,...}
- 各ページは「伸びなくなるまで」スクロールして、取れるだけ取得
- 初期処理： [trx].[vendor_item] を [trx].[listings].vendor_item_id と突き合わせ、status を NULL クリア
- 一覧から (vendor_item_id, title, price) を取得しながら UPSERT
- 価格変更検知時:
    * USD算出不可 → eBay出品終了＋[trx].[listings] 削除
    * 算出可能 → eBay価格更新（simulateモード可）
"""

# === Standard library ===
import sys,os
import re
import time
import random
import argparse
from datetime import datetime
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
import traceback
def log_ctx(msg, **kw): print(msg, " ".join(f"{k}={v}" for k,v in kw.items()))

if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# === Third-party ===
import pyodbc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === Local modules ===
# ルートと common をパスに追加（絶対importを可能に）
sys.path.extend([r"D:\apps_nostock", r"D:\apps_nostock\common"])

from utils import compute_start_price_usd, get_sql_server_connection
from listings.ebay_api import delete_item_from_ebay, update_ebay_price
from scrape_utils import scroll_until_stagnant_collect_items, build_driver,safe_quit

# stdout UTF-8化
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# === 設定 ===
VENDOR_NAME = "メルカリ"
NO_RESULT_TEXT = "出品された商品がありません"
simulate = False  # ← ★ 本番運用は False

# --- debug: exit toggles (temporary) ---
EXIT_AFTER_PRICE_UPDATE = False   # 価格更新が成功したら即終了
EXIT_AFTER_DELETE = False         # 出品削除が成功したら即終了

URLS = [
    {
        "name": "コインケース・小銭入れ",
        "preset": "ヴィトン小銭入れMS",
        "market": "shops",   # ← 追加: "shops" or "personal"
        "url": (
            "https://jp.mercari.com/search?brand_id=1326&category_id=243"
            "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
            "&item_condition_id=1%2C2%2C3"
            "&keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3%E3%80%80%E8%B2%A1%E5%B8%83"
            "&shipping_payer_id=2"
            "&sort=created_time&order=desc"
            "&status=on_sale"
            "&price_min=50000&price_max=115000"
            "&item_types=beyond"
        ),
    },
    #{
    #    "name": "コインケース・小銭入れ",
    #    "preset": "ヴィトン小銭入れM",
    #    "url": (
    #        "https://jp.mercari.com/search?brand_id=1326&category_id=243"
    #        "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
    #        "&e6cec404-5b34-46aa-8316-cda6695a85f3=a2364058-0db2-4a68-bc92-afcc2f79787e"
    #        "&item_condition_id=1%2C2%2C3"
    #        "&keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3%E3%80%80%E8%B2%A1%E5%B8%83"
    #        "&shipping_payer_id=2&sort=created_time&status=on_sale"
    #        "&price_min=50000&price_max=115000"
    #    ),
    #},    {
    #    "name": "長財布",
    #    "preset": "ヴィトン長財布M",
    #    "url": (
    #        "https://jp.mercari.com/search?brand_id=1326&category_id=241"
    #        "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
    #        "&e6cec404-5b34-46aa-8316-cda6695a85f3=a2364058-0db2-4a68-bc92-afcc2f79787e"
    #        "&item_condition_id=1%2C2%2C3"
    #        "&keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3%E3%80%80%E8%B2%A1%E5%B8%83"
    #        "&shipping_payer_id=2&sort=created_time&status=on_sale"
    #        "&price_min=50000&price_max=115000"
    #    ),
    #},
    {
        "name": "折り財布",
        "preset": "ヴィトン折り財布M",
        "url": (
            "https://jp.mercari.com/search?brand_id=1326&category_id=242"
            "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
            "&e6cec404-5b34-46aa-8316-cda6695a85f3=a2364058-0db2-4a68-bc92-afcc2f79787e"
            "&item_condition_id=1%2C2%2C3"
            "&keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3%E3%80%80%E8%B2%A1%E5%B8%83"
            "&shipping_payer_id=2&sort=created_time&status=on_sale"
            "&price_min=50000&price_max=115000"
        ),
    },
]

# ===================
# Utility
# ===================
def reset_vendor_item_status_for_active_skus(conn):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE vi
               SET vi.[status] = NULL
            FROM [trx].[vendor_item] AS vi
            INNER JOIN [trx].[listings] AS l
                ON vi.[vendor_item_id] = l.[vendor_item_id]
        """)
        conn.commit()
    print("[INIT] status cleared on vendor_item joined with listings")


def add_or_replace_query(url: str, **params) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = str(v)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def page_url(base_url: str, idx_zero_based: int) -> str:
    return base_url if idx_zero_based == 0 else add_or_replace_query(base_url, page_token=f"v1:{idx_zero_based}")

def has_no_results_banner(driver) -> bool:
    try:
        txt = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        return NO_RESULT_TEXT in txt
    except Exception:
        return False

# ===================
# listings / vendor_item
# ===================
def get_listing_core_by_sku(conn, vendor_item_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT listing_id, account, vendor_name
              FROM [trx].[listings]
             WHERE vendor_item_id = ?
        """, (vendor_item_id,))
        row = cur.fetchone()
        if row:
            return tuple(str(r).strip() if r is not None else None for r in row)
    return (None, None, None)

def delete_listing_by_itemid(conn, ebay_item_id: str, account: str, vendor_name: str):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM [trx].[listings]
             WHERE listing_id = ? AND account = ? AND vendor_name = ?
        """, (ebay_item_id, account, vendor_name))
    conn.commit()

def get_vendor_item_price(conn, vendor_name: str, vendor_item_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT price FROM [trx].[vendor_item]
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (vendor_name, vendor_item_id))
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

# ===================
# eBay価格変更 or 削除
# ===================
def _is_transient_inventory_error(resp):
    if not resp or resp.get("success"):
        return False
    raw = resp.get("raw") or {}
    errors = ((raw.get("putOffer") or {}).get("errors") or []) or raw.get("errors") or []
    msgs = " ".join(str(e.get("message","")) for e in errors if isinstance(e, dict)).lower()
    codes = {int(e.get("errorId")) for e in errors if isinstance(e, dict) and str(e.get("errorId","")).isdigit()}
    return (25001 in codes) or ("internal error" in msgs)

def handle_price_change_side_effects(conn, sku, vendor_name, old_price, new_price_jpy, simulate: bool):
    ebay_item_id, account, listing_vendor = get_listing_core_by_sku(conn, sku)

    # eBay出品がないSKUは副作用なし
    if not ebay_item_id:
        # print(f"[SIDE-EFFECT] vendor_item_id={sku} listingsなし → eBay連携なし")
        return

    usd = compute_start_price_usd(new_price_jpy)

    if usd is None:
        # 目標外レンジ → 出品終了フロー（DB更新は後のMERGEに任せる）
        print(f"[PRICE] {sku}: {old_price} -> {new_price_jpy} JPY / 目標外レンジ (usd=None)")
        if simulate:
            print(f"[SIMULATE DELETE] vendor_item_id={sku} item_id={ebay_item_id}  (eBayは未実行)")
        else:
            res = delete_item_from_ebay(account, ebay_item_id)
            ok = bool(res.get('success')) or res.get('note') in {'already_deleted','already_ended'}
            if ok:
                delete_listing_by_itemid(conn, ebay_item_id, account, listing_vendor or vendor_name)
                # ▼ 検証用：実削除が完了したら即終了
                if EXIT_AFTER_DELETE:
                    print("[TEST EXIT] real delete completed → プログラム終了")
                    sys.exit(0)
            else:
                print(f"[WARN] eBay削除失敗 itemId={ebay_item_id} resp={res}")
        return

    # usd 算出可 → 価格更新フロー（DB更新は後のMERGEに任せる）
    print(f"【価格変更】 {sku}: {old_price} -> {new_price_jpy} JPY / USD {usd}")

    if simulate:
        print(f"[SIMULATE UPDATE] {sku} item_id={ebay_item_id} USD={usd}  (eBayは未実行)")
        return

    did_update_ebay = False
    resp = None
    for wait in [0, 2, 6, 15]:
        if wait:
            time.sleep(wait)
        resp = update_ebay_price(account, ebay_item_id, usd, sku=sku, debug=True)
        if resp and resp.get("success"):
            did_update_ebay = True
            break
        if not _is_transient_inventory_error(resp):
            break

    if did_update_ebay:
        pass
    else:
        print(f"[警告] eBay価格更新失敗 resp={resp}（DB価格は後のMERGEで既存値のまま）")

    # ▼ 成否に関わらず強制終了
    if EXIT_AFTER_PRICE_UPDATE:
        print("[TEST EXIT] price update finished → プログラム終了")
        sys.exit(0)


def upsert_vendor_items(conn, rows, run_ts):
    """
    rows: dictの配列
      {vendor_name, vendor_item_id, status, preset, title_jp, vendor_page, price}
    """
    if not rows:
        return 0

    sql = """
MERGE [trx].[vendor_item] AS T
USING (SELECT ? AS vendor_name, ? AS vendor_item_id) AS S
ON (T.[vendor_name] = S.vendor_name AND T.[vendor_item_id] = S.vendor_item_id)
WHEN MATCHED THEN
  UPDATE SET
    T.[status]         = ?,
    T.[preset]         = ?,
    T.[title_jp]       = ?,
    T.[vendor_page]    = ?,
    T.[last_checked_at]= ?,
    T.[prev_price] = CASE
                       WHEN (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL) OR (T.[price] IS NOT NULL AND ? IS NULL))
                         THEN T.[price]
                       ELSE T.[prev_price]
                     END,
    T.[price]      = COALESCE(?, T.[price])
WHEN NOT MATCHED THEN
  INSERT (
      [vendor_name], [vendor_item_id], [status], [preset], [title_jp],
      [vendor_page], [created_at], [last_checked_at],
      [price], [prev_price]
  )
  VALUES (S.vendor_name, S.vendor_item_id, ?, ?, ?, ?, ?, ?, ?, NULL);
"""

    with conn.cursor() as cur:
        for r in rows:
            params = (
                # USING
                r["vendor_name"], r["vendor_item_id"],
                # UPDATE
                r["status"], r["preset"], r["title_jp"], r["vendor_page"], run_ts,
                r["price"], r["price"], r["price"],  # NULL安全比較用に3回
                r["price"],                           # 新価格
                # INSERT
                r["status"], r["preset"], r["title_jp"], r["vendor_page"], run_ts, run_ts, r["price"]
            )
            cur.execute(sql, params)
        conn.commit()


# ===================
# Main
# ===================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pause", type=float, default=0.6)
    args = ap.parse_args()

    # 実行タイムスタンプ（DBに統一反映）
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # リソース（必ず finally でクリーンアップ）
    conn = None
    driver = None

    try:
        # === DB接続 ===
        conn = get_sql_server_connection()

        # === 初期化（致命的なため失敗時は即終了） ===
        log_ctx("[INIT] clearing status...")
        try:
            reset_vendor_item_status_for_active_skus(conn)
        except Exception:
            traceback.print_exc()
            raise  # 初期化失敗は続行しない

        # === WebDriver 起動（致命的エラー扱い） ===
        log_ctx("[DRIVER] building...")
        try:
            driver = build_driver()
        except Exception:
            traceback.print_exc()
            raise  # 起動不能は続行しない
        total_written = 0


        # === URLごとのメイン処理 ===
        for cfg in URLS:
            try:
                base_url = cfg["url"]
                preset   = cfg["preset"]
                print(f"\n◎対象URL {cfg['name']} / preset={preset}")

                page_idx = 0
                while True:
                    # 1) ページ遷移
                    url = page_url(base_url, page_idx)
                    log_ctx("[PAGE NAV] GET", page=page_idx+1, url=url, name=cfg["name"], preset=preset)
                    try:
                        driver.get(url)
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                    except Exception:
                        traceback.print_exc()
                        log_ctx("[PAGE NAV] failed", page=page_idx+1, url=url)
                        page_idx += 1  # このページはスキップして次へ
                        continue

                    # 2) 検索結果ゼロならURLループ終了
                    if has_no_results_banner(driver):
                        break

                    # 3) スクロール&収集
                    try:
                        items = scroll_until_stagnant_collect_items(driver, args.pause)
                    except Exception:
                        traceback.print_exc()
                        log_ctx("[SCRAPE] failed", page=page_idx+1)
                        page_idx += 1
                        continue

                    print(f"[PAGE {page_idx+1}] count={len(items)}")

                    # 4) 価格変更の副作用（SKU単位で落ちても続行）
                    cnt_skip = cnt_changed = cnt_unchanged = 0
                    for iid, title, price in items:
                        if price is None:
                            cnt_skip += 1
                            print(f"[SKIP] price is None for item_id={iid} title={title}")
                            continue

                        old_price = get_vendor_item_price(conn, VENDOR_NAME, iid)

                        if old_price is not None and old_price != price:
                            cnt_changed += 1
                            try:
                                handle_price_change_side_effects(
                                    conn, iid, VENDOR_NAME, old_price, price, simulate=simulate
                                )
                            except Exception:
                                traceback.print_exc()
                                log_ctx("[SIDE-EFFECT ERROR]", iid=iid, old=old_price, new=price)
                                continue
                        else:
                            cnt_unchanged += 1

                    # 5) ページ分を一括 MERGE
                    rows = [{
                        "vendor_name": VENDOR_NAME,
                        "vendor_item_id": iid,
                        "status": "販売中",
                        "preset": preset,
                        "title_jp": title,
                        "vendor_page": page_idx,
                        "price": price,
                    } for iid, title, price in items]

                    log_ctx("[UPSERT] begin", page=page_idx+1, rows=len(rows))
                    try:
                        upsert_vendor_items(conn, rows, run_ts)
                    except Exception:
                        traceback.print_exc()
                        log_ctx("[UPSERT] failed", page=page_idx+1)
                        break  # このURLの処理は中断して次のURLへ

                    total_written += len(rows)
                    print(f"[PAGE {page_idx+1}] upserted={len(rows)}")

                    # 6) ページサマリ
                    print(f"[PAGE {page_idx+1} RESULT] price skip={cnt_skip}, "
                          f"price changed={cnt_changed}, price unchanged={cnt_unchanged}, "
                          f"total={len(items)}")

                    # 7) 次ページへ（0件なら終了）
                    if len(items) == 0:
                        break

                    page_idx += 1
                    time.sleep(args.pause + random.uniform(0.2, 0.5))

            except Exception:
                traceback.print_exc()
                log_ctx("[TARGET] aborted", name=cfg.get("name"), preset=cfg.get("preset"))
                # 他のURLへは続行
                continue

        print(f"\n[SUMMARY] total_rows={total_written}")

    finally:
        # === 後片付け（存在チェックして安全に） ===
        try:
            if driver is not None:
                safe_quit(driver) 
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
