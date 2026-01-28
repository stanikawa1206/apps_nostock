# -*- coding: utf-8 -*-
"""
scrape_preset_item.py

指定した 1 つの preset について：

- mst.presets（fetch_active_presets）から行を取得
- make_search_url() で status="on_sale" の検索URLを生成
- page_token=v1:0,1,2,... で 0ページ目から順に巡回
- 各ページの検索結果から item_id を全取得
- 各 item の詳細ページをスクレイプして
    - mst.seller
    - trx.vendor_item
  を upsert するバッチ

※ この版ではさらに：
    - last_updated_str（古い更新判定）
    - セラー評価（セラー条件未達）
    - 価格（計算価格が範囲外）
  の 3 つで NG 判定し、
    - 出品状況・出品状況詳細・last_ng_at を更新
    - 「古い更新」になったタイミングで eBay 出品取消
    - 「計算価格が範囲外」になった商品も eBay 出品取消
"""

import sys
import os
import traceback
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse
from typing import Dict, Any, List, Tuple, Optional, Set
import time
import re  # ← 追加（古い更新の判定用）

sys.path.append(r"D:\apps_nostock")

from apps.adapters.mercari_scraper import (
    scroll_until_stagnant_collect_items,
    scroll_until_stagnant_collect_shops,
)

# ------------------------------
# sys.path bootstrap
# ------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]  # D:/apps_nostock
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Windows の標準出力を UTF-8 に
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ------------------------------
# third-party
# ------------------------------
import pyodbc  # type: ignore
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore

# ------------------------------
# local modules
# ------------------------------
from apps.common.utils import (
    get_sql_server_connection,
    send_mail,
    compute_start_price_usd,  # ← 追加（計算価格が範囲外判定用）
)

# new_publish_ebay から Selenium/DB 関連を再利用
from apps.publish.publish_ebay_251225 import (
    build_driver,
    _close_any_modal,
    parse_detail_shops,
    parse_detail_personal,
    upsert_vendor_item,
    upsert_seller,
    _maybe_commit,
    mark_listing_status_head_detail,  # ← 追加（出品状況更新）
    get_seller_gate,                  # ← 追加（セラーGate判定）
)

from apps.adapters.mercari_search import (
    make_search_url,
    fetch_active_presets,
)

from apps.adapters.mercari_item_status import (
    handle_listing_delete,  # ← 追加（eBay出品取消）
)

# ==============================
# 設定
# ==============================

# ★ このファイルを使うときに preset 名をここに書き換える
#   例: TARGET_PRESETS = ["ケイトスペード折り財布M"]
TARGET_PRESETS = ['ヴィトン小銭入れMS','ヴィトン折り財布M','ヴィトン折り財布MS','ヴィトン長財布M']

# 何件書き込んだら commit するか
BATCH_COMMIT: int = 100

# スクロール時の待ち時間（秒） inventory_ebay_1_fetch_active.py と同じくらい
SCROLL_PAUSE: float = 0.6

# 検索結果が 0 件のときに使われるメッセージ（保険用）
NO_RESULT_TEXT = "出品された商品がありません"


def log_ctx(msg: str, **kw):
    """簡易ログ"""
    if kw:
        kv = " ".join(f"{k}={v}" for k, v in kw.items())
        print(msg, kv)
    else:
        print(msg)


# ==============================
# URL ヘルパ
# ==============================

def add_or_replace_query(url: str, **params) -> str:
    """
    inventory_ebay_1_fetch_active.py と同じイメージの
    クエリパラメータ差し替えヘルパ。
    """
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = str(v)
    return urlunparse((u.scheme, u.netloc, u.path, u.params,
                       urlencode(q, doseq=True), u.fragment))


def page_url(base_url: str, idx_zero_based: int) -> str:
    """
    page_token=v1:{0,1,2,...} を付与する。
    0ページ目は base_url そのまま。
    """
    if idx_zero_based == 0:
        return base_url
    return add_or_replace_query(base_url, page_token=f"v1:{idx_zero_based}")


def has_no_results_banner(driver) -> bool:
    """
    ページ全体のテキストから「出品された商品がありません」を検出。
    スクレイプミスなどで a タグが取れない場合の保険。
    """
    try:
        txt = driver.execute_script(
            "return document.body ? document.body.innerText : ''"
        ) or ""
        return NO_RESULT_TEXT in txt
    except Exception:
        return False


def build_item_url(vendor_name: str, vendor_item_id: str) -> str:
    """vendor_name に応じて詳細ページの URL を作成。"""
    vendor_name = (vendor_name or "").strip()
    if vendor_name == "メルカリshops":
        return f"https://jp.mercari.com/shops/product/{vendor_item_id}"
    else:
        return f"https://jp.mercari.com/item/{vendor_item_id}"


# ==============================
# preset 情報取得
# ==============================

def load_preset_row(conn: pyodbc.Connection, preset_name: str) -> Dict[str, Any]:
    """
    fetch_active_presets() の結果から、指定 preset の行だけを返す。
    """
    presets = fetch_active_presets(conn)
    for p in presets:
        if (p.get("preset") or "").strip() == preset_name.strip():
            return p
    raise RuntimeError(f"mst.presets に preset={preset_name!r} が見つかりません")


# ==============================
# ヘルパ：旧出品状況取得
# ==============================

def fetch_old_status_head(conn: pyodbc.Connection,
                          vendor_name: str,
                          vendor_item_id: str) -> Optional[str]:
    """
    処理前の trx.vendor_item.出品状況 を取得。
    古い更新 → 新規に「古い更新」になったかどうか判定するために使う。
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT [出品状況]
              FROM [trx].[vendor_item] WITH (NOLOCK)
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (vendor_name, vendor_item_id))
        row = cur.fetchone()
    if not row:
        return None
    return (row[0] or "").strip() or None


# ==============================
# メインロジック（1 preset 分）
# ==============================

def scrape_one_preset(conn: pyodbc.Connection, preset_name: str) -> Tuple[int, int, int]:
    """
    指定 preset について、on_sale の全ページを巡回し、
    各商品の詳細ページをスクレイプして DB を更新する。

    さらに：
      - last_updated_str で「古い更新」判定
      - mst.seller の rating_count でセラーGate判定
      - compute_start_price_usd による「計算価格が範囲外」判定
    を行い、
      - 出品状況 / 出品状況詳細 / last_ng_at を更新
      - 「古い更新」になったタイミングで eBay出品取消
      - 「計算価格が範囲外」の商品も eBay出品取消
    戻り値: (page_count, success_count, fail_count)
    """
    p = load_preset_row(conn, preset_name)
    vendor_name = (p.get("vendor_name") or "").strip()

    # 検索URL（0ページ目）
    base_url = make_search_url(
        vendor_name=vendor_name,
        brand_id=p.get("brand_id"),
        category_id=p.get("category_id"),
        status="on_sale",
        mode=p.get("mode"),
        low_usd_target=p.get("low_usd_target"),
        high_usd_target=p.get("high_usd_target"),
    )

    print(f"[PRESET] {preset_name} vendor={vendor_name}")
    print(f"[SEARCH URL] {base_url}")

    driver = build_driver()
    page_idx = 0
    page_count = 0
    success = 0
    fail = 0
    writes_since_commit = 0
    seen_items: Set[str] = set()

    try:
        while True:
            url = page_url(base_url, page_idx)
            log_ctx("[PAGE NAV] GET", preset=preset_name, page=page_idx, url=url)

            try:
                driver.get(url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except Exception:
                traceback.print_exc()
                log_ctx("[PAGE NAV] failed", page=page_idx)
                # ここで終わっても良いが、一応次ページへ進めてみる
                page_idx += 1
                continue

            _close_any_modal(driver)

            # 「商品がありません」バナー
            if has_no_results_banner(driver):
                print(f"[INFO] page={page_idx} 検索結果なし → 終了")
                break

            # ★ スクロール＆取得
            if vendor_name == "メルカリshops":
                items = scroll_until_stagnant_collect_shops(driver, SCROLL_PAUSE)
            else:
                items = scroll_until_stagnant_collect_items(driver, SCROLL_PAUSE)

            # items は [(item_id, title, price), ...] 形式なので ID に直す
            item_ids = [iid for (iid, title, price) in items]

            print(f"[PAGE {page_idx}] item_count={len(item_ids)}")

            if not item_ids:
                # a タグも 0 件 → 終点とみなして終了
                print(f"[INFO] page={page_idx} item 0 件 → 終了")
                break

            page_count += 1

            # 各商品を詳細スクレイプ
            for idx, iid in enumerate(item_ids, start=1):
                if iid in seen_items:
                    continue
                seen_items.add(iid)

                sku = iid.strip()
                detail_url = build_item_url(vendor_name, sku)

                print(f"  - [{idx}/{len(item_ids)}] SKU={sku} {detail_url}")

                # 処理前の出品状況（古い更新 → 古い更新になったタイミング検出用）
                old_status_head = fetch_old_status_head(conn, vendor_name, sku)

                try:
                    if vendor_name == "メルカリshops":
                        rec = parse_detail_shops(driver, detail_url, preset_name, vendor_name)
                    else:
                        rec = parse_detail_personal(driver, detail_url, preset_name, vendor_name)
                except Exception as e:
                    print(f"[ERROR] 詳細解析失敗 SKU={sku}: {e}")
                    traceback.print_exc()
                    fail += 1
                    continue

                # ★ このページで拾ったことを明示する
                rec["vendor_page"] = page_idx

                # mst.seller / trx.vendor_item を upsert
                try:
                    seller_id = (rec.get("seller_id") or "").strip()
                    seller_name = rec.get("seller_name") or ""
                    rating_count = int(rec.get("rating_count") or 0)

                    if seller_id:
                        upsert_seller(conn, vendor_name, seller_id, seller_name, rating_count)
                        writes_since_commit += 1

                    upsert_vendor_item(conn, rec)
                    writes_since_commit += 1

                    writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)
                    success += 1

                except Exception as e:
                    print(f"[ERROR] DB upsert 失敗 SKU={sku}: {e}")
                    traceback.print_exc()
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    fail += 1
                    continue

                # ============================
                # ここから NG 判定 ＆ ステータス更新
                # publish_ebay のロジックを簡略移植
                #   1) 古い更新 → 出品状況=古い更新 + 必要なら eBay取消
                #   2) セラーGate NG → 出品状況=セラー条件未達（取消なし）
                #   3) 価格レンジ外 → 出品状況=計算価格が範囲外 + eBay取消
                # ============================

                last_updated_str = rec.get("last_updated_str") or ""


                # 1) 古い更新 判定
                is_old_now = bool(
                    re.search(r"(半年以上前|\d+\s*[ヶか]月前|数\s*[ヶか]月前)", last_updated_str)
                )

                if is_old_now:
                    # 出品状況を「古い更新」に更新（これは常にやる）
                    mark_listing_status_head_detail(
                        conn,
                        vendor_name,
                        sku,
                        "古い更新",
                        last_updated_str,
                    )
                    writes_since_commit += 1
                    writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

                    # --- ここから「古い更新化」判定を細かく分ける ---

                    if old_status_head is None:
                        # 元々 trx.vendor_item にレコードが無かったケース
                        # → 初回スクレイプでいきなり last_updated_str が古かっただけ。
                        # eBay 出品取消は行わない。
                        print(f"    -> 初回スクレイプで古い商品 (SKU={sku})。出品状況のみ古い更新に設定。キャンセルなし。")
                        continue

                    # もともと「古い更新」以外だったものが、今回初めて「古い更新」になった
                    if old_status_head != "古い更新":
                        print(f"    -> 古い更新化に伴い eBay 出品取消 SKU={sku}")
                        handle_listing_delete(conn, sku)

                    # もともと古い更新だった場合は、特に追加処理なし
                    # （キャンセルも二重で飛ばさない）
                    continue


                # 2) セラー Gate 判定（評価が少ないセラー）
                if seller_id:
                    try:
                        is_ok, rating = get_seller_gate(conn, vendor_name, seller_id)
                    except Exception as e:
                        print(f"[WARN] get_seller_gate 失敗 SKU={sku} seller_id={seller_id}: {e}")
                        is_ok, rating = True, 0

                    if not is_ok:
                        mark_listing_status_head_detail(
                            conn,
                            vendor_name,
                            sku,
                            "セラー条件未達",
                            f"rating={rating}",
                        )
                        writes_since_commit += 1
                        writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

                        # セラー評価が下がることは基本ないので、ここでは出品取消は行わない
                        continue

                # 3) 計算価格が範囲外 判定
                try:
                    start_price_usd = compute_start_price_usd(
                        rec.get("price") or 0,
                        p.get("mode"),
                        p.get("low_usd_target"),
                        p.get("high_usd_target"),
                    )
                except Exception as e:
                    print(f"[WARN] compute_start_price_usd 失敗 SKU={sku}: {e}")
                    start_price_usd = None

                if not start_price_usd:
                    # 価格レンジ外 → 出品状況を更新し、eBay 出品も取消
                    low_usd = p.get("low_usd_target")
                    high_usd = p.get("high_usd_target")
                    detail_msg = f"{low_usd}–{high_usd}USD"

                    mark_listing_status_head_detail(
                        conn,
                        vendor_name,
                        sku,
                        "計算価格が範囲外",
                        detail_msg,
                    )
                    writes_since_commit += 1
                    writes_since_commit = _maybe_commit(conn, writes_since_commit, BATCH_COMMIT)

                    print(f"    -> 計算価格が範囲外のため eBay 出品取消 SKU={sku}")
                    handle_listing_delete(conn, sku)

                    continue

                # ここまで来たものは、特に NG なし（販売中でセラー評価もOK・価格もレンジ内）
                # 出品状況は何も触らず次へ進む

            page_idx += 1

        # 最後に残り分をコミット
        if writes_since_commit > 0:
            conn.commit()

        return page_count, success, fail

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def aggregate_and_save_preset_stats(conn, preset: str, started_at: datetime) -> None:
    """
    started_at 以降に last_checked_at が更新された vendor_item を対象に、
    preset × vendor_page × last_updated_str ごとの件数を集計して
    trx.preset_page_stats に書き出す。

    ★ 該当 preset のレコードを（過去日も含めて）全部 DELETE してから INSERT する。
    """
    collected_date: date = started_at.date()

    with conn.cursor() as cur:
        # ① preset 単位で全削除（過去分も含めて消す）
        cur.execute("""
            DELETE FROM [trx].[preset_page_stats]
             WHERE preset = ?
        """, (preset,))
    conn.commit()

    # ② 今回分を再集計
    sql = """
        SELECT
            preset,
            vendor_page,
            ISNULL(last_updated_str, N'') AS last_updated_str,
            COUNT(*) AS item_count
        FROM [trx].[vendor_item] WITH (NOLOCK)
        WHERE preset = ?
          AND last_checked_at >= ?
        GROUP BY
            preset,
            vendor_page,
            ISNULL(last_updated_str, N'');
    """

    with conn.cursor() as cur:
        cur.execute(sql, (preset, started_at))
        rows = cur.fetchall()

        for preset_val, vendor_page, last_updated_str, item_count in rows:
            cur.execute("""
                INSERT INTO [trx].[preset_page_stats] (
                    preset,
                    vendor_page,
                    last_updated_str,
                    item_count,
                    collected_date
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                preset_val,
                vendor_page,
                last_updated_str,
                item_count,
                collected_date
            ))

    conn.commit()



# ==============================
# エントリポイント
# ==============================

def main():
    # 複数 preset のチェック
    if not TARGET_PRESETS:
        raise RuntimeError("TARGET_PRESETS に 1件以上の preset 名を設定してください。")

    conn = get_sql_server_connection()
    conn.autocommit = False

    try:
        for preset_name in TARGET_PRESETS:
            start_time = datetime.now()
            print(f"[START] preset={preset_name} at {start_time}")

            # 1) 指定 preset を scrape
            page_count, success, fail = scrape_one_preset(conn, preset_name)

            # 2) 今回の scrape で更新された行だけを集計して stats table に保存
            aggregate_and_save_preset_stats(conn, preset_name, start_time)

            end_time = datetime.now()
            elapsed = end_time - start_time

            subject = f"✅ scrape_preset_item 完了 preset={preset_name}"
            body = (
                f"preset: {preset_name}\n"
                f"開始: {start_time}\n"
                f"終了: {end_time}\n"
                f"処理時間: {elapsed}\n"
                f"処理ページ数: {page_count}\n"
                f"成功件数: {success}\n"
                f"失敗件数: {fail}\n"
            )

            print(body)

            try:
                send_mail(subject, body)
            except Exception as e:
                print(f"[WARN] 完了メール送信失敗: {e}")

        # ループが全部終わってから autocommit を元に戻す
        conn.autocommit = True
        print("=== 全 preset の処理 完了 ===")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
