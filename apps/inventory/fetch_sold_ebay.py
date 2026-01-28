# -*- coding: utf-8 -*-
import sys
import time
from datetime import datetime
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse

import pyodbc
import urllib3
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

# ===== パス設定 & インポート =====
# ルートと common をパスに追加
sys.path.extend([r"D:\apps_nostock"])  # apps配下を認識させる

from apps.common.utils import get_sql_server_connection
from apps.adapters.mercari_scraper import (
    scroll_until_stagnant_collect_items,
    build_driver,
    scroll_until_stagnant_collect_shops,
)
from apps.adapters.mercari_search import make_search_url, fetch_active_presets
from apps.adapters.mercari_item_status import handle_listing_delete


# stdout UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# ===== 設定 =====
MAX_PAGES = 3            # 売り切れは3ページまでで十分
SIMULATE_DELETE = False   # True: eBay APIは叩かずprintのみ
PAUSE = 0.6              # スクロール間隔

URLS = [
    {
        "name": "コインケース・小銭入れ（売切）",
        "preset": "ヴィトン小銭入れM",
        "url": (
            "https://jp.mercari.com/search?keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3%E3%80%80%E8%B2%A1%E5%B8%83"
            "&brand_id=1326&category_id=243"
            "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
            "&e6cec404-5b34-46aa-8316-cda6695a85f3=a2364058-0db2-4a68-bc92-afcc2f79787e"
            "&item_condition_id=1%2C2%2C3"
            "&shipping_payer_id=2&sort=created_time"
            "&status=sold_out%7Ctrading"
            "&price_min=50000&price_max=115000"
        ),
    },    {
        "name": "長財布（売切）",
        "preset": "ヴィトン長財布M",
        "url": (
            "https://jp.mercari.com/search?keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3%E3%80%80%E8%B2%A1%E5%B8%83"
            "&brand_id=1326&category_id=241"
            "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
            "&e6cec404-5b34-46aa-8316-cda6695a85f3=a2364058-0db2-4a68-bc92-afcc2f79787e"
            "&item_condition_id=1%2C2%2C3"
            "&shipping_payer_id=2&sort=created_time"
            "&status=sold_out%7Ctrading"
            "&price_min=50000&price_max=115000"
        ),
    },
    {
        "name": "折り財布（売切）",
        "preset": "ヴィトン折り財布M",
        "url": (
            "https://jp.mercari.com/search?keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3%E3%80%80%E8%B2%A1%E5%B8%83"
            "&brand_id=1326&category_id=242"
            "&d664efe3-ae5a-4824-b729-e789bf93aba9=B38F1DC9286E0B80812D9B19DB14298C1FF1116CA8332D9EE9061026635C9088"
            "&e6cec404-5b34-46aa-8316-cda6695a85f3=a2364058-0db2-4a68-bc92-afcc2f79787e"
            "&item_condition_id=1%2C2%2C3"
            "&shipping_payer_id=2&sort=created_time"
            "&status=sold_out%7Ctrading"
            "&price_min=50000&price_max=115000"
        ),
    },
]



# ===== URLヘルパ =====
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
    # 1ページ目＝そのまま、それ以降は page_token=v1:{n}
    return base_url if idx_zero_based == 0 else add_or_replace_query(base_url, page_token=f"v1:{idx_zero_based}")


def has_no_results_banner(driver) -> bool:
    try:
        txt = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
        return "出品された商品がありません" in txt
    except Exception:
        return False


# ===== DB I/O =====
def upsert_vendor_item(conn: pyodbc.Connection, vendor_name: str, item_id: str, title: str, page_num: int, preset: str, now_str: str):
    with conn.cursor() as cur:
        # 存在確認
        cur.execute("""
            SELECT COUNT(*)
              FROM [trx].[vendor_item]
             WHERE vendor_name = ? AND vendor_item_id = ?
        """, (vendor_name, item_id))
        exists = cur.fetchone()[0] > 0

        if not exists:
            cur.execute("""
                INSERT INTO [trx].[vendor_item]
                    (vendor_name, vendor_item_id, title_jp, created_at, last_checked_at,
                     vendor_page, status, preset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (vendor_name, item_id, title, now_str, now_str, page_num, "売り切れ", preset))
        else:
            cur.execute("""
                UPDATE [trx].[vendor_item]
                   SET last_checked_at = ?,
                       vendor_page     = ?,
                       status          = ?,
                       preset          = ?
                 WHERE vendor_name = ? AND vendor_item_id = ?
            """, (now_str, page_num, "売り切れ", preset, vendor_name, item_id))
    conn.commit()

# ===== メイン =====
def main():
    conn = None
    driver = None
    try:
        conn = get_sql_server_connection()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        driver = build_driver()

        # ▼ mst.presets から有効プリセットを取得
        presets = fetch_active_presets(conn)
        print(f"[PRESETS] {len(presets)} rows")

        for p in presets:
            preset_name = p["preset"]
            vendor_name = p["vendor_name"]      # 'メルカリ' or 'メルカリshops'
            brand_id    = p["brand_id"]
            category_id = p["category_id"]
            mode        = p.get("mode", "DDP")  # ★ GA / DDP（なければ DDP にしておく）

            # 検索URLを生成（売切れ・取引中を対象）
            base_url = make_search_url(
                vendor_name=vendor_name,
                brand_id=brand_id,
                category_id=category_id,
                status="sold_out|trading",
                mode=mode,                             # ★ 追加
                low_usd_target=p["low_usd_target"],
                high_usd_target=p["high_usd_target"],
            )

            print(f"\n--- 売り切れ取得開始 preset='{preset_name}' vendor='{vendor_name}' mode='{mode}' ---")
            print(f"🔍 {base_url}")

            page_idx = 0
            seen_ids = set()

            while True:
                if page_idx >= MAX_PAGES:
                    print(f"[STOP] reached MAX_PAGES={MAX_PAGES}")
                    break

                target_url = page_url(base_url, page_idx)
                print(f"[PAGE {page_idx+1}] GET {target_url}")

                try:
                    driver.get(target_url)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                except (urllib3.exceptions.ReadTimeoutError, TimeoutError, Exception) as e:
                    # ★ メルカリ or ブラウザが重くてタイムアウトしたときはこちらに来る
                    print(f"[WARN] driver.get() / ページ読込でエラー preset='{preset_name}' "
                        f"vendor='{vendor_name}' page={page_idx+1}: {e}")
                    print(f"[WARN] このプリセットは途中までで打ち切って、次のプリセットに進みます。")
                    break  # ← while True を抜けて、次の preset へ


                if has_no_results_banner(driver):
                    print(f"[PAGE {page_idx+1}] no-results banner -> stop")
                    break

                # --- vendor_name に応じて外部関数を切替 ---
                try:
                    if vendor_name == "メルカリshops":
                        items = scroll_until_stagnant_collect_shops(driver, PAUSE)
                    else:
                        items = scroll_until_stagnant_collect_items(driver, PAUSE)
                except Exception:
                    import traceback
                    traceback.print_exc()
                    print(f"[SCRAPE FAILED] page={page_idx+1}")
                    page_idx += 1
                    continue
                # ----------------------------------------------

                print(f"[PAGE {page_idx+1}] count={len(items)}")

                new_items = []
                for iid, title, price in items:
                    iid = (iid or "").strip()
                    if not iid or iid in seen_ids:
                        continue
                    seen_ids.add(iid)
                    new_items.append((iid, (title or "").strip()))

                if not new_items:
                    print(f"[PAGE {page_idx+1}] all seen -> stop")
                    break

                for iid, title in new_items:
                    upsert_vendor_item(conn, vendor_name, iid, title, page_idx + 1, preset_name, now_str)
                    handle_listing_delete(conn, iid, simulate=SIMULATE_DELETE)

                page_idx += 1
                time.sleep(1)

        print("\n✅ 売り切れチェック完了")

    except Exception:
        import traceback
        traceback.print_exc()
        raise
    finally:
        try:
            if driver is not None:
                driver.quit()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
