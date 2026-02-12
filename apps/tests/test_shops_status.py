# filename: test_shops_status.py
# -*- coding: utf-8 -*-

import sys
from pathlib import Path

# ===== プロジェクトルート追加 =====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.adapters.mercari_item_status import detect_status_from_mercari_shops
from apps.adapters.mercari_scraper import build_driver


# ===== テスト対象URL（ここを変えるだけ）=====
TEST_URL = "https://jp.mercari.com/shops/product/2JLbbSyQsBwSZNvGAREW4w1"


def main():
    driver = build_driver()

    try:
        print(f"[OPEN] {TEST_URL}")
        driver.get(TEST_URL)

        status, price = detect_status_from_mercari_shops(driver)

        print("=" * 40)
        print("STATUS:", status)
        print("PRICE :", price)
        print("=" * 40)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
