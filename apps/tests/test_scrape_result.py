# test_parse_mercari.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ★ publish_ebay.py から関数を import
from apps.publish.publish_ebay_260227 import (
    parse_detail_personal,
    parse_detail_shops,
)

def main():

    # ----------------------------
    # Chrome起動
    # ----------------------------
    options = Options()
    options.add_argument("--start-maximized")
    # options.add_argument("--headless")  # 必要なら有効化

    driver = webdriver.Chrome(options=options)

    try:
        # =====================================
        # ① 通常メルカリ
        # =====================================
        personal_url = "https://jp.mercari.com/item/m43867694777"

        print("\n==============================")
        print("通常メルカリ scrape 開始")
        print("==============================")

        personal_result = parse_detail_personal(
            driver,
            personal_url,
            preset="test",
            vendor_name="mercari"
        )

        print("\n--- scrape結果 (personal) ---")
        for k, v in personal_result.items():
            print(f"{k}: {v}")


        # =====================================
        # ② メルカリShops
        # =====================================
        shops_url = "https://jp.mercari.com/shops/product/awFqct4S6S7sW54E7j4JDM"

        print("\n==============================")
        print("Shops scrape 開始")
        print("==============================")

        shops_result = parse_detail_shops(
            driver,
            shops_url,
            preset="test",
            vendor_name="mercari_shops"
        )

        print("\n--- scrape結果 (shops) ---")
        for k, v in shops_result.items():
            print(f"{k}: {v}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
