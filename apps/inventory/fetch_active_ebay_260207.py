# -*- coding: utf-8 -*-
r"""
inventory_ebay_1_fetch_active.py (with price-sync & listings integration; presets+shops ready)
- mst.presets から対象プリセットを読み込み（固定URLSは廃止）
- vendor_name に応じて、メルカリ通常/メルカリshops を自動切替
- ページ送り：page_token=v1:{0,1,2,...}
- 各ページは「伸びなくなるまで」スクロールして、取れるだけ取得
- 初期処理： [trx].[vendor_item] を [trx].[listings] と突き合わせ、status を NULL クリア
- 一覧から (vendor_item_id, title, price) を取得しながら UPSERT
- 価格変更検知時:
    * USD算出不可 → eBay出品終了＋[trx].[listings] 削除
    * 算出可能 → eBay価格更新（simulateモード可）
"""

# === Standard library ===
import sys
import os
import time
import random
import argparse
from datetime import datetime
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
import traceback
from typing import Any, Dict, List, Tuple, Optional

def log_ctx(msg, **kw):
    suffix = " ".join(f"{k}={v}" for k, v in kw.items())
    print(msg, suffix, flush=True) if suffix else print(msg, flush=True)

# Windows の標準出力を UTF-8 に
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
# ルートと common をパスに追加（絶対 import を可能に）
sys.path.extend([r"D:\apps_nostock", r"D:\apps_nostock\common"])

from apps.common.utils import compute_start_price_usd, get_sql_server_connection
from apps.adapters.ebay_api import delete_item_from_ebay, update_ebay_price

# 検索URLビルダー（brand/category/keyword/price/status から URL を生成）
from apps.adapters.mercari_search import make_search_url, fetch_active_presets

# スクロール収集：通常／shops を両方 import
from apps.adapters.mercari_scraper import (
    scroll_until_stagnant_collect_items,    # 通常メルカリ（非PR）
    scroll_until_stagnant_collect_shops,    # メルカリshops
    build_driver,
    safe_quit,
)

# === 設定 ===
NO_RESULT_TEXT = "出品された商品がありません"
simulate = False  # ← ★ 本番運用は False

# --- debug: exit toggles (temporary) ---
EXIT_AFTER_PRICE_UPDATE = False   # 価格更新が成功したら即終了
EXIT_AFTER_DELETE = False         # 出品削除が成功したら即終了


# ===================
# Utility
# ===================
def reset_vendor_item_status_for_active_skus(conn):
    """listings に存在する SKU の vendor_item.status を NULL クリア（vendor_nameも一致させる）"""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE vi
               SET vi.[status] = NULL
            FROM [trx].[vendor_item] AS vi
            INNER JOIN [trx].[listings] AS l
                ON vi.[vendor_name] = l.[vendor_name]
               AND vi.[vendor_item_id] = l.[vendor_item_id]
        """)
        conn.commit()
    print("[INIT] status cleared on vendor_item joined with listings", flush=True)


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
def get_listing_core_by_sku(
    conn,
    vendor_item_id: str,
    vendor_name: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    with conn.cursor() as cur:
        if vendor_name:
            cur.execute("""
                SELECT listing_id, account, vendor_name
                  FROM [trx].[listings]
                 WHERE vendor_item_id = ?
                   AND vendor_name = ?
            """, (vendor_item_id, vendor_name))
        else:
            cur.execute("""
                SELECT listing_id, account, vendor_name
                  FROM [trx].[listings]
                 WHERE vendor_item_id = ?
            """, (vendor_item_id,))
        row = cur.fetchone()
        if row:
            return tuple(str(r).strip() if r is not None else None for r in row)  # type: ignore
    return (None, None, None)

def delete_listing_by_itemid(conn, ebay_item_id: str, account: str, vendor_name: str):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM [trx].[listings]
             WHERE listing_id = ? AND account = ? AND vendor_name = ?
        """, (ebay_item_id, account, vendor_name))
    conn.commit()

def get_vendor_item_prices_batch(conn, vendor_name: str, vendor_item_ids: List[str]) -> Dict[str, Optional[int]]:
    """
    vendor_item_ids の価格をまとめて取得して dict で返す:
    { vendor_item_id: price or None }
    """
    if not vendor_item_ids:
        return {}

    placeholders = ",".join("?" for _ in vendor_item_ids)
    sql = f"""
        SELECT vendor_item_id, price
        FROM [trx].[vendor_item]
        WHERE vendor_name = ? AND vendor_item_id IN ({placeholders})
    """

    params = [vendor_name] + vendor_item_ids
    out: Dict[str, Optional[int]] = {}

    with conn.cursor() as cur:
        cur.execute(sql, params)
        for vid, price in cur.fetchall():
            out[str(vid)] = int(price) if price is not None else None

    # 存在しなかった ID は None 扱い
    for v in vendor_item_ids:
        out.setdefault(v, None)

    return out


# ===================
# presets targets (mst.v_presets)
# ===================
_PRESET_TARGET_CACHE: Dict[str, Tuple[str, float, float]] = {}

def get_preset_targets(conn, preset: str) -> Optional[Tuple[str, float, float]]:
    """
    mst.v_presets から preset の (mode, low_usd_target, high_usd_target) を取得
    """
    if not preset:
        return None
    if preset in _PRESET_TARGET_CACHE:
        return _PRESET_TARGET_CACHE[preset]

    sql = """
        SELECT mode, low_usd_target, high_usd_target
          FROM mst.v_presets WITH (NOLOCK)
         WHERE preset = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql, (preset,))
        row = cur.fetchone()

    if not row:
        return None

    mode = str(row[0])
    low = float(row[1])
    high = float(row[2])

    _PRESET_TARGET_CACHE[preset] = (mode, low, high)
    return mode, low, high


# ===================
# eBay価格変更 or 削除
# ===================
def _is_transient_inventory_error(resp: Dict[str, Any]) -> bool:
    if not resp or resp.get("success"):
        return False
    raw = resp.get("raw") or {}
    errors = ((raw.get("putOffer") or {}).get("errors") or []) or raw.get("errors") or []
    msgs = " ".join(str(e.get("message","")) for e in errors if isinstance(e, dict)).lower()
    codes = {int(e.get("errorId")) for e in errors if isinstance(e, dict) and str(e.get("errorId","")).isdigit()}
    return (25001 in codes) or ("internal error" in msgs)

def handle_price_change_side_effects(conn, sku, vendor_name, old_price, new_price_jpy, preset, simulate=False):
    # vendor_name も指定して listings を引く（衝突防止）
    ebay_item_id, account, listing_vendor = get_listing_core_by_sku(conn, sku, vendor_name=vendor_name)

    # eBay出品がないSKUは副作用なし
    if not ebay_item_id:
        return

    targets = get_preset_targets(conn, preset)
    if not targets:
        print(f"[WARN] preset not found in mst.v_presets: preset={preset} sku={sku}", flush=True)
        return  # 安全側（消さない/更新しない）
    mode, low_usd_target, high_usd_target = targets

    # ★ここが本命：GA固定をやめて preset の値で計算
    usd = compute_start_price_usd(new_price_jpy, mode, low_usd_target, high_usd_target)

    if usd is None:
        # 目標外レンジ → 出品終了フロー（DB更新は後のMERGEに任せる）
        print(f"[PRICE] {sku}: {old_price} -> {new_price_jpy} JPY / 目標外レンジ (usd=None) mode={mode} {low_usd_target}-{high_usd_target}", flush=True)
        if simulate:
            print(f"[SIMULATE DELETE] vendor_item_id={sku} item_id={ebay_item_id}  (eBayは未実行)", flush=True)
        else:
            res = delete_item_from_ebay(account, ebay_item_id)
            ok = bool(res.get("success")) or res.get("note") in {"already_deleted", "already_ended"}
            if ok:
                delete_listing_by_itemid(conn, ebay_item_id, account, listing_vendor or vendor_name)
                if EXIT_AFTER_DELETE:
                    print("[TEST EXIT] real delete completed → プログラム終了", flush=True)
                    sys.exit(0)
            else:
                print(f"[WARN] eBay削除失敗 itemId={ebay_item_id} resp={res}", flush=True)
        return

    # usd 算出可 → 価格更新フロー（DB更新は後のMERGEに任せる）
    print(f"【価格変更】 {sku}: {old_price} -> {new_price_jpy} JPY / USD {usd}  mode={mode} {low_usd_target}-{high_usd_target}", flush=True)

    if simulate:
        print(f"[SIMULATE UPDATE] {sku} item_id={ebay_item_id} USD={usd}  (eBayは未実行)", flush=True)
        return

    did_update_ebay = False
    resp: Optional[Dict[str, Any]] = None
    for wait in [0, 2, 6, 15]:
        if wait:
            time.sleep(wait)
        resp = update_ebay_price(account, ebay_item_id, usd, sku=sku, debug=True)
        if resp and resp.get("success"):
            did_update_ebay = True
            break
        if not _is_transient_inventory_error(resp or {}):
            break

    if not did_update_ebay:
        print(f"[警告] eBay価格更新失敗 resp={resp}（DB価格は後のMERGEで既存値のまま）", flush=True)

    if EXIT_AFTER_PRICE_UPDATE:
        print("[TEST EXIT] price update finished → プログラム終了", flush=True)
        sys.exit(0)


def upsert_vendor_items(conn, rows: List[Dict[str, Any]], run_ts: str) -> int:
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
    T.[price]      = COALESCE(?, T.[price]),
    T.[出品状況] = CASE
                     WHEN ISNULL(T.[出品状況], N'') = N'古い更新'
                      AND (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL) OR (T.[price] IS NOT NULL AND ? IS NULL))
                       THEN NULL
                     ELSE T.[出品状況]
                   END,
    T.[出品状況詳細] = CASE
                         WHEN ISNULL(T.[出品状況], N'') = N'古い更新'
                          AND (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL) OR (T.[price] IS NOT NULL AND ? IS NULL))
                           THEN NULL
                         ELSE T.[出品状況詳細]
                       END,
    T.[last_ng_at] = CASE
                       WHEN ISNULL(T.[出品状況], N'') = N'古い更新'
                        AND (T.[price] <> ? OR (T.[price] IS NULL AND ? IS NOT NULL) OR (T.[price] IS NOT NULL AND ? IS NULL))
                         THEN NULL
                       ELSE T.[last_ng_at]
                     END
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

                # UPDATE（固定）
                r["status"], r["preset"], r["title_jp"], r["vendor_page"], run_ts,

                # prev_price 用（新価格で比較を3回）
                r["price"], r["price"], r["price"],
                # price 更新（新価格）
                r["price"],

                # 出品状況クリア用（新価格で比較を3回）
                r["price"], r["price"], r["price"],
                # 出品状況詳細クリア用（新価格で比較を3回）
                r["price"], r["price"], r["price"],
                # last_ng_at クリア用（新価格で比較を3回）
                r["price"], r["price"], r["price"],

                # INSERT
                r["status"], r["preset"], r["title_jp"], r["vendor_page"], run_ts, run_ts, r["price"]
            )
            cur.execute(sql, params)
        conn.commit()
    return len(rows)

# ===================
# Main
# ===================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pause", type=float, default=0.6)
    args = ap.parse_args()

    # 実行タイムスタンプ（DBに統一反映）
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # リソース
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
            raise

        # === WebDriver 起動（致命的エラー扱い） ===
        log_ctx("[DRIVER] building...")
        try:
            driver = build_driver()
        except Exception:
            traceback.print_exc()
            raise

        total_written = 0

        # === プリセットを DB から取得 ===
        presets = fetch_active_presets(conn)
        if not presets:
            print("[INFO] 有効なプリセットがありません（mst.presets を確認）", flush=True)
            return

        # === プリセットごとのメイン処理 ===
        for p in presets:
            try:
                preset      = p["preset"]
                vendor_name = p["vendor_name"]  # 'メルカリ' or 'メルカリshops'
                mode        = p["mode"]         # GA / DDP など

                base_url = make_search_url(
                    vendor_name=vendor_name,
                    brand_id=p["brand_id"],
                    category_id=p["category_id"],
                    status="on_sale",
                    mode=mode,
                    low_usd_target=p["low_usd_target"],
                    high_usd_target=p["high_usd_target"],
                )

                print(f"\n◎preset={preset} vendor={vendor_name}\n🔍 {base_url}", flush=True)

                page_idx = 0
                while True:
                    page_start = time.time()

                    # 1) ページ遷移
                    url = page_url(base_url, page_idx)
                    log_ctx("[PAGE NAV] GET", page=page_idx+1, url=url, preset=preset, vendor=vendor_name)
                    try:
                        driver.get(url)
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    except Exception:
                        traceback.print_exc()
                        log_ctx("[PAGE NAV] failed", page=page_idx+1, url=url)
                        page_idx += 1  # このページはスキップして次へ
                        continue

                    # 2) 検索結果ゼロならURLループ終了
                    if has_no_results_banner(driver):
                        break

                    # 3) スクロール&収集（vendor_name で collector を切替）
                    try:
                        if vendor_name == "メルカリshops":
                            items = scroll_until_stagnant_collect_shops(driver, args.pause)  # [(id,title,price),...]
                        else:
                            items = scroll_until_stagnant_collect_items(driver, args.pause)  # [(id,title,price),...]
                    except Exception:
                        traceback.print_exc()
                        log_ctx("[SCRAPE] failed", page=page_idx+1)
                        page_idx += 1
                        continue

                    print(f"[PAGE {page_idx+1}] count={len(items)}", flush=True)

                    # ★ ここでまとめて SELECT
                    item_ids = [iid for iid, _, _ in items]
                    old_price_map = get_vendor_item_prices_batch(conn, vendor_name, item_ids)

                    # 4) 価格変更の副作用（SKU単位で落ちても続行）
                    cnt_skip = cnt_changed = cnt_unchanged = 0
                    for iid, title, price in items:
                        if price is None:
                            cnt_skip += 1
                            print(f"[SKIP] price is None for item_id={iid} title={title}", flush=True)
                            continue

                        old_price = old_price_map.get(iid)

                        if old_price is not None and old_price != price:
                            cnt_changed += 1
                            try:
                                # ★ここがバグ修正：rec["preset"] は未定義 → preset を渡す    
                                handle_price_change_side_effects(
                                    conn, iid, vendor_name, old_price, price, preset, simulate=simulate
                                )
                            except Exception:
                                traceback.print_exc()
                                log_ctx("[SIDE-EFFECT ERROR]", iid=iid, old=old_price, new=price)
                                continue
                        else:
                            cnt_unchanged += 1

                    # 5) ページ分を一括 MERGE
                    rows = [{
                        "vendor_name": vendor_name,
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
                        break  # このプリセットの処理は中断して次へ

                    total_written += len(rows)
                    print(f"[PAGE {page_idx+1}] upserted={len(rows)}", flush=True)

                    # 6) ページサマリ
                    print(f"[PAGE {page_idx+1} RESULT] price skip={cnt_skip}, "
                          f"price changed={cnt_changed}, price unchanged={cnt_unchanged}, "
                          f"total={len(items)}", flush=True)

                    # 7) 次ページへ（0件なら終了）
                    if len(items) == 0:
                        break

                    elapsed = time.time() - page_start
                    TARGET = 35.0  # ← 好みで 30〜40 に調整OK

                    if elapsed < TARGET:
                        remaining = TARGET - elapsed
                        time.sleep(remaining + random.uniform(0.0, 3.0))

                    page_idx += 1
                    time.sleep(args.pause + random.uniform(0.2, 0.5))

            except Exception:
                traceback.print_exc()
                log_ctx("[TARGET] aborted", preset=p.get("preset"), vendor=p.get("vendor_name"))
                continue

        print(f"\n[SUMMARY] total_rows={total_written}", flush=True)

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
