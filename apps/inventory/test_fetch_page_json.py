import json
import time
import base64
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from playwright.sync_api import sync_playwright

TARGET_URL = "https://jp.mercari.com/search?keyword=ヴィトン"
N = 200

# -----------------------------
# driver
# -----------------------------
def build_driver():
    options = Options()
    options.add_argument("--start-maximized")

    # Networkログ取得
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(options=options)
    return driver


# -----------------------------
# Network intercept
# -----------------------------
def fetch_page_json(page, url):

    with page.expect_response(lambda r: "entities:search" in r.url) as resp_info:
        page.goto(url)

    response = resp_info.value
    return response.json()

# -----------------------------
# JSON → rows
# -----------------------------
def extract_items_from_json(json_data):

    rows = []

    # メルカリ検索JSON
    items = json_data.get("items", [])

    for item in items:

        item_id = item.get("id")
        title = item.get("name")
        price = item.get("price")
        seller = item.get("sellerId")
        created = item.get("created")
        updated = item.get("updated")

        if price is not None:
            price = int(price)

        rows.append(
            (item_id, title, price, seller, created, updated)
        )

    return rows


# -----------------------------
# TEST
# -----------------------------
def main():

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        print("OPEN:", TARGET_URL)

        page.goto(TARGET_URL)

        json_data =  fetch_page_json(page, TARGET_URL)

        if not json_data:
            print("JSON取得失敗")
            return

        rows = extract_items_from_json(json_data)

        print("TOTAL:", len(rows))

        for row in rows[:N]:
            print(row)

        browser.close()


if __name__ == "__main__":
    main()