# -*- coding: utf-8 -*-
"""
_find_seller_info / _extract_shops_seller の単体テスト用スクリプト
→ scrape結果で mst.seller を更新する
"""
import sys
from pathlib import Path
# =========================
# sys.path bootstrap: file-direct run safe
# =========================
# このファイル: D:\apps_nostock\apps\publish\publish_ebay.py
# プロジェクトルート: D:\apps_nostock  ← parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# =========================
# Local (project)
# =========================
from apps.common.utils import (
    get_sql_server_connection,
)


import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ★ ここで既存関数を import
from publish_ebay import (
    _find_seller_info,
    _extract_shops_seller,
)


# -------------------------
# Selenium 起動
# -------------------------
def build_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--lang=ja-JP")
    opts.page_load_strategy = "eager"
    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_window_size(1400, 1000)
    return driver


# -------------------------
# mst.seller upsert（常時更新）
# -------------------------
SQL_UPSERT_MST_SELLER = """
MERGE INTO mst.seller AS tgt
USING (VALUES (?, ?, ?, ?)) AS src (vendor_name, seller_id, seller_name, rating_count)
ON (tgt.vendor_name = src.vendor_name AND tgt.seller_id = src.seller_id)
WHEN MATCHED THEN
    UPDATE SET
        seller_name     = src.seller_name,
        rating_count    = src.rating_count,
        last_checked_at = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT (vendor_name, seller_id, seller_name, rating_count, is_ng, last_checked_at)
    VALUES (src.vendor_name, src.seller_id, src.seller_name, src.rating_count, 0, SYSDATETIME());
"""


def upsert_mst_seller(conn, vendor_name: str, seller_id: str, seller_name: str, rating_count: int) -> None:
    seller_id = (seller_id or "").strip()
    seller_name = (seller_name or "").strip()

    # 方針：取れた値で更新。取れてないならここでは更新しない（テストとして分かりやすく）
    if not seller_id:
        raise ValueError("seller_id is blank")
    if rating_count is None:
        raise ValueError("rating_count is None")

    with conn.cursor() as cur:
        cur.execute(SQL_UPSERT_MST_SELLER, (vendor_name, seller_id, seller_name, int(rating_count)))


# -------------------------
# テスト対象URL
# -------------------------
PERSONAL_URLS = [
    "https://jp.mercari.com/item/m96084475213",
    "https://jp.mercari.com/item/m54498779999"
]

SHOPS_URLS = [

]


# -------------------------
# main
# -------------------------
def main():
    driver = build_driver()
    conn = get_sql_server_connection()

    try:
        print("===== 通常メルカリ（_find_seller_info）=====")
        for url in PERSONAL_URLS:
            print(f"\n[URL] {url}")
            driver.get(url)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            seller_id, seller_name, rating_count = _find_seller_info(driver, url)

            print(f"seller_id    : {seller_id}")
            print(f"seller_name  : {seller_name}")
            print(f"rating_count : {rating_count}")

            # ★ mst.seller 更新
            upsert_mst_seller(conn, "メルカリ", seller_id, seller_name, rating_count)
            conn.commit()
            print("→ mst.seller upsert OK")


        print("\n===== メルカリShops（_extract_shops_seller）=====")
        for url in SHOPS_URLS:
            print(f"\n[URL] {url}")
            driver.get(url)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            seller_id, seller_name, rating_count = _extract_shops_seller(driver)

            print(f"seller_id    : {seller_id}")
            print(f"seller_name  : {seller_name}")
            print(f"rating_count : {rating_count}")

            # ★ mst.seller 更新
            upsert_mst_seller(conn, "メルカリshops", seller_id, seller_name, rating_count)
            conn.commit()
            print("→ mst.seller upsert OK")

    finally:
        try:
            conn.close()
        except Exception:
            pass
        driver.quit()


if __name__ == "__main__":
    main()
