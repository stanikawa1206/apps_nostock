# -*- coding: utf-8 -*-
"""
Mercari 画像URL取得の単体テスト
- 通常メルカリ:  item_id -> https://jp.mercari.com/item/<item_id>
- メルカリShops: product_id -> https://mercari-shops.com/products/<product_id>

目的:
  画像を取得しているロジック（2つ）だけを安全にテストし、
  取得したURLを表示する。

注意:
  既存 publish_ebay.py の関数を直接 import しない（依存が重い場合がある）ので、
  画像取得ロジックはここに最小で複製しています。
"""

import sys
from pathlib import Path

# =========================
# sys.path bootstrap: file-direct run safe
# =========================
# このファイル: D:\apps_nostock\apps\publish\test_collect_images.py
# プロジェクトルート: D:\apps_nostock  ← parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# =========================
# Local (project)
# =========================
# ※今回はDBアクセス不要なので utils は使わないが、あなたの雰囲気に合わせて残しておく
from apps.common.utils import (
    get_sql_server_connection,  # noqa: F401
)

from typing import List, Optional
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ★ ここで既存関数を import
from publish_ebay import (
    collect_images_shops,
    collect_images_personal,
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
# URL builder（ID -> URL）
# -------------------------
def build_personal_url(item_id: str) -> str:
    item_id = (item_id or "").strip()
    if not item_id or not item_id.startswith("m"):
        raise ValueError(f"personal item_id must start with 'm' (e.g. m123...), got: {item_id!r}")
    return f"https://jp.mercari.com/item/{item_id}"


def build_shops_url(product_id: str) -> str:
    product_id = (product_id or "").strip()
    if not product_id:
        raise ValueError("shops product_id is blank")
    return f"https://mercari-shops.com/products/{product_id}"



# -------------------------
# テスト対象（IDだけ入れる）
# -------------------------
PERSONAL_ITEM_IDS = [
    "m67795518375",
]

SHOPS_PRODUCT_IDS = [
]


# -------------------------
# main
# -------------------------
def main():
    driver = build_driver()
    try:
        print("===== 通常メルカリ（画像URL取得）=====")
        for item_id in PERSONAL_ITEM_IDS:
            url = build_personal_url(item_id)
            print(f"\n[item_id] {item_id}")
            print(f"[URL]     {url}")

            driver.get(url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # eager なので少し待つ（最小）
            time.sleep(0.5)

            imgs = collect_images_personal(driver, limit=10)
            for i, u in enumerate(imgs, start=1):
                print(f"  personal img{i:02d}: {u}")

        print("\n===== メルカリShops（画像URL取得）=====")
        for product_id in SHOPS_PRODUCT_IDS:
            url = build_shops_url(product_id)
            print(f"\n[product_id] {product_id}")
            print(f"[URL]        {url}")

            driver.get(url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # slick が出るまで待つ（ない場合もあるので軽く）
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".slick-slide img[src]"))
                )
            except Exception:
                pass

            imgs = collect_images_shops(driver, limit=10)
            for i, u in enumerate(imgs, start=1):
                print(f"  shops img{i:02d}: {u}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
