from playwright.sync_api import sync_playwright
import json

URL = "https://jp.mercari.com/item/m34748992463"

with sync_playwright() as p:

    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    def handle_response(response):

        if "api.mercari.jp/items/get" in response.url:

            try:
                data = response.json()

                print("===== API RESPONSE =====")
                print(json.dumps(data, indent=2, ensure_ascii=False))

            except:
                print("JSON parse error")

    page.on("response", handle_response)

    page.goto(URL)

    page.wait_for_timeout(5000)

    browser.close()