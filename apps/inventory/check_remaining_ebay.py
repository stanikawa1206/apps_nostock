# filename: stock_checker.py
# -*- coding: utf-8 -*-

from __future__ import annotations
import json, re, time, random, argparse, sys, traceback, os
from typing import Literal, Optional, List, Dict

from selenium import webdriver  # 型注釈用

import sys
from pathlib import Path

# ===== プロジェクトルートを sys.path に追加（最初にやる）=====
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ===== その後で apps を import =====
from apps.adapters.mercari_item_status import (
    Status,
    detect_status_from_mercari,
    detect_status_from_mercari_shops,
)

# ===== ここで send_mail を import（重要） =====
from apps.common.utils import send_mail

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
from apps.common.utils import get_sql_server_connection, compute_start_price_usd
from apps.adapters.ebay_api import delete_item_from_ebay, update_ebay_price
from apps.adapters.mercari_scraper import build_driver

# ===================== 設定 =====================
TEST_MODE =False
TEST_URLS = ["m29108294683"]

HEADLESS = True

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
def is_account_excluded_for_sku(conn, vendor_item_id: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT a.is_excluded
            FROM trx.listings l
            JOIN mst_ebay_accounts a
              ON l.account = a.account
            WHERE l.vendor_item_id = ?
        """, (vendor_item_id,))
        rows = cur.fetchall()
        return any(r[0] for r in rows)
    finally:
        cur.close()


def load_mercari_targets_from_db(limit: Optional[int] = None) -> List[Dict[str, str]]:
    """
    listings と vendor_item を (vendor_name, vendor_item_id) でJOIN。
    vendor_item.status がブランク（NULL or 空文字）のものだけ対象。
    対象: メルカリshops / メルカリ 両方
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
        WHERE l.vendor_name IN (N'メルカリshops', N'メルカリ')
          AND (v.status IS NULL OR LTRIM(RTRIM(v.status)) = N'')
        ORDER BY l.start_time DESC
    """

    conn = get_sql_server_connection()
    try:
        out: List[Dict[str, str]] = []
        with conn.cursor() as cur:
            cur.execute(sql)
            for row in cur:
                ebay_item_id   = str(row[0]).strip()
                account        = str(row[1]).strip()
                vendor_item_id = str(row[2]).strip()
                vendor_name    = str(row[3]).strip()

                # URL生成を vendor_name に応じて分岐
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
def run(urls: Optional[List[str]] = None):
    log_ctx("[DRIVER] building...")
    driver = None
    conn = None
    total = deleted = updated = failed = 0
    unresolved_after_retry = 0

    # ★ 1周目で「判定不可」だったものをここに溜める
    retry_rows: List[Dict[str, str]] = []

    try:
        driver = build_driver()
        conn = get_sql_server_connection()

        # 手動URL（TEST_MODEなど） or DB全件
        if urls:
            manual = []
            for s in urls:
                s = s.strip()
                # URL化 + vendor_name 判定
                if s.startswith("http"):
                    # もしすでにURLで渡された場合にも対応
                    url = s
                    if "/item/" in s:
                        m = re.search(r"/item/(m\d{8,})", s)
                        if not m: 
                            continue
                        sku, vendor_name = m.group(1), "メルカリ"
                    elif "/shops/product/" in s:
                        m = re.search(r"/shops/product/([A-Za-z0-9]{10,})", s)
                        if not m: 
                            continue
                        sku, vendor_name = m.group(1), "メルカリshops"
                    else:
                        continue
                else:
                    # 裸IDの場合
                    if re.fullmatch(r"m\d{8,}", s):
                        sku, vendor_name, url = s, "メルカリ", f"https://jp.mercari.com/item/{s}"
                    elif re.fullmatch(r"[A-Za-z0-9]{10,}", s):
                        sku, vendor_name, url = s, "メルカリshops", f"https://jp.mercari.com/shops/product/{s}"
                    else:
                        continue

                manual.append({
                    "url": url,
                    "sku": sku,
                    "account": "",
                    "ebay_item_id": "",
                    "vendor_name": vendor_name,
                })
            rows = manual
        else:
            rows = load_mercari_targets_from_db(limit=None)

        if not rows:
            print("[WARN] 対象がありません（TEST/DB）")
            return
        else:
            vendors = ",".join(sorted({r["vendor_name"] for r in rows}))
            print(f"[INFO] 処理対象 {len(rows)} 件を検出: vendors={vendors}")

        # ===== 1周目：通常処理 =====
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

            # ★ 本番モード(TEST_MODE=False)で 判定不可 のものは後でまとめて再チェック
            if (not TEST_MODE) and status == "判定不可":
                retry_rows.append(r)

            # ===== 販売中：価格差分チェック →（必要なら）eBay改定/削除 → DB反映 =====
            if status == "販売中" and price_jpy is not None:
                try:
                    old_price = get_vendor_item_price(conn, vendor_name, sku)

                    if (old_price is None) or (old_price != price_jpy):
                        new_price_usd = compute_start_price_usd(price_jpy, "GA", 450, 1000)

                        if new_price_usd is None:
                            print(f"[PRICE] {sku}: {old_price} -> {price_jpy} JPY / 目標外レンジ ⇒ eBay出品を終了")
                            if ebay_item_id:
                                try:
                                    if not is_account_excluded_for_sku(conn, sku):
                                        res = delete_item_from_ebay(account, ebay_item_id)
                                        ok = bool(res.get("success")) or res.get("note") in {"already_deleted", "already_ended"}
                                        if ok:
                                            delete_ebay_listing_record(conn, ebay_item_id, account, vendor_name)
                                            deleted += 1
                                        else:
                                            print(f"[WARN] eBay削除失敗 listingId={ebay_item_id} resp={res}")
                                    else:
                                        print(f"[SKIP DELETE] excluded account sku={sku}")
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
                                if wait: 
                                    time.sleep(wait)
                                if not is_account_excluded_for_sku(conn, sku):
                                    resp = update_ebay_price(account, ebay_item_id, new_price_usd, sku=sku, debug=True)
                                else:
                                    print(f"[SKIP UPDATE] excluded account sku={sku}")
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

        # ===== 2周目：本番時のみ、判定不可をまとめて再チェック =====
        if (not TEST_MODE) and retry_rows:
            log_ctx(f"[RETRY] 判定不可だった {len(retry_rows)} 件を再チェックします…")

            for r in retry_rows:
                url          = r["url"]
                sku          = r["sku"]
                account      = r["account"]
                ebay_item_id = r["ebay_item_id"]
                vendor_name  = r["vendor_name"]

                status: Status = "判定不可"
                price_jpy: Optional[int] = None

                # 2周目用リトライ（少し待ちを長めにしてもOK）
                for i, wait in enumerate([0.0, 3.0, 8.0]):
                    if i > 0:
                        time.sleep(wait)
                    try:
                        status, price_jpy = get_status(driver, url)
                        if status != "判定不可":
                            break
                    except Exception as e:
                        if i == 2:
                            print(f"[ERR][RETRY] get_status失敗: {url} ({e})")
                            failed += 1

                print(f"[RETRY-STATUS] {url} -> {status} (price_jpy={price_jpy})")

                if status == "判定不可":
                    # 2周目でも判定不可 → ひとまず status だけ残して終了
                    try:
                        update_vendor_item_price_and_status(conn, vendor_name, sku, None, status)
                        updated += 1
                    except Exception as e:
                        print(f"[WARN] (RETRY) vendor_item更新失敗 sku={sku}: {e}")
                        unresolved_after_retry += 1
                    continue

                # ===== ここから先は 1周目と同じロジックで処理 =====
                if status == "販売中" and price_jpy is not None:
                    try:
                        old_price = get_vendor_item_price(conn, vendor_name, sku)

                        if (old_price is None) or (old_price != price_jpy):
                            new_price_usd = compute_start_price_usd(price_jpy, "GA", 450, 1000)

                            if new_price_usd is None:
                                print(f"[PRICE-RETRY] {sku}: {old_price} -> {price_jpy} JPY / 目標外レンジ ⇒ eBay出品を終了")
                                if ebay_item_id:
                                    try:
                                        if not is_account_excluded_for_sku(conn, sku):
                                            res = delete_item_from_ebay(account, ebay_item_id)
                                            ok = bool(res.get("success")) or res.get("note") in {"already_deleted", "already_ended"}
                                            if ok:
                                                delete_ebay_listing_record(conn, ebay_item_id, account, vendor_name)
                                                deleted += 1
                                            else:
                                                print(f"[WARN] (RETRY) eBay削除失敗 listingId={ebay_item_id} resp={res}")
                                        else:
                                                print(f"[SKIP DELETE] excluded account sku={sku}")
                                    except Exception as e:
                                        print(f"[ERR] (RETRY) eBay削除処理で例外 listingId={ebay_item_id}: {e}")
                                else:
                                    print(f"[WARN] (RETRY) eBay削除不可（listing_idなし） sku={sku}")

                                update_vendor_item_price_and_status(conn, vendor_name, sku, price_jpy, status)
                                updated += 1
                            else:
                                did_update_ebay = False
                                resp = None
                                for wait in [0, 2, 6, 15]:
                                    if wait:
                                        time.sleep(wait)
                                    if not is_account_excluded_for_sku(conn, sku):
                                        resp = update_ebay_price(account, ebay_item_id, new_price_usd, sku=sku, debug=True)
                                    else:
                                        print(f"[SKIP UPDATE] excluded account sku={sku}")
                                    if resp and resp.get("success"):
                                        did_update_ebay = True
                                        break
                                    if not _is_transient_inventory_error(resp):
                                        break

                                if did_update_ebay or not ebay_item_id:
                                    update_vendor_item_price_and_status(conn, vendor_name, sku, price_jpy, status)
                                    updated += 1
                                    if did_update_ebay:
                                        print(f"[PRICE-RETRY] {sku}: {old_price} -> {price_jpy} JPY / eBay {new_price_usd} USD を更新")
                                    else:
                                        print(f"[PRICE-RETRY] {sku}: {old_price} -> {price_jpy} JPY / eBay改定なし（listing_idなし）")
                                else:
                                    update_vendor_item_price_and_status(conn, vendor_name, sku, None, status)
                                    updated += 1
                                    print(f"[WARN] (RETRY) eBay価格更新失敗 listingId={ebay_item_id} resp={resp}")
                                    print(f"[PRICE-RETRY] {sku}: {old_price} -> {price_jpy} JPY / eBay {new_price_usd} USD （未更新・DB価格は据え置き）")
                        else:
                            update_vendor_item_price_and_status(conn, vendor_name, sku, None, status)
                            updated += 1

                    except Exception as e:
                        print(f"[WARN] (RETRY) 価格差分反映で例外 sku={sku}: {e}")

                else:
                    # 販売中ではない：status のみ更新
                    try:
                        update_vendor_item_price_and_status(conn, vendor_name, sku, None, status)
                        updated += 1
                    except Exception as e:
                        print(f"[WARN] (RETRY) vendor_item更新失敗 sku={sku}: {e}")

                if status in {"削除", "オークション", "売り切れ", "公開停止"}:
                    if ebay_item_id:
                        try:
                            if not is_account_excluded_for_sku(conn, sku):
                                res = delete_item_from_ebay(account, ebay_item_id)
                                ok = bool(res.get("success")) or res.get("note") in {"already_deleted", "already_ended"}
                                if ok:
                                    delete_ebay_listing_record(conn, ebay_item_id, account, vendor_name)
                                    deleted += 1
                                else:
                                    print(f"[WARN] eBay削除失敗 listingId={ebay_item_id} resp={res}")
                            else:
                                print(f"[SKIP DELETE] excluded account sku={sku}")
                        except Exception as e:
                            print(f"[ERR] eBay削除処理で例外 listingId={ebay_item_id}: {e}")
                    else:
                        print(f"[WARN] (RETRY) eBay削除不可（listing_idなし） sku={sku}")

        print(f"\n✅ 完了: 対象{total}件 / vendor_item更新{updated}件 / eBay削除{deleted}件 / 失敗{failed}件")
        # ★ 2回目リトライ後も判定不可のまま残っている件数を出力（親がパース用）
        if not TEST_MODE:
            print(f"UNRESOLVED={unresolved_after_retry}")


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

    # ====== ★ TEST_MODE の危険警告メール送信（処理は続行） ======
    if TEST_MODE:
        print(f"[TEST] TEST_MODE=True / TEST_URLS={TEST_URLS}")

        # ===== ⚠️ TEST_MODE 警告メール（処理は続行） =====
        try:
            subject = "【⚠️警告】stock_checker.py が TEST_MODE=True で起動されました！"
            body = (
                "⚠️⚠️⚠️【重大警告】⚠️⚠️⚠️\n\n"
                "stock_checker.py が TEST_MODE=True のまま起動しました。\n"
                "本番環境で実行している場合、データが更新されず\n"
                "正しい同期・在庫判定が行われない可能性があります。\n\n"
                "➡️ TEST_MODE=False に戻して本番運用を行ってください。\n"
                "（この警告メールは safety check のため自動送信されました）"
            )
            send_mail(subject=subject, body=body)
        except Exception as e:
            print(f"[WARN] TEST_MODE 警告メール送信に失敗: {e}")

    if TEST_MODE:
        # TEST_URLS は裸IDのまま run に渡す
        print(f"[TEST] TEST_MODE=True / TEST_URLS={TEST_URLS}")
        run(urls=TEST_URLS)
    else:
        run(urls=None)   # DB全件
