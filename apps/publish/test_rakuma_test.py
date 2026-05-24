from playwright.sync_api import sync_playwright
import json


ITEM_HASH = "b251e7a4c0860a7aa6854fdcd2f2030b"


def handle_response(response):

    url = response.url


    if "api.fril.jp" in url:
        print("\n===================")
        print(url)
        print(response.status)

        try:

            data = response.json()

            print(
                json.dumps(
                    data,
                    indent=2,
                    ensure_ascii=False
                )[:5000]
            )

        except:
            pass


with sync_playwright() as p:

    browser = p.chromium.connect_over_cdp(
        "http://127.0.0.1:9222"
    )

    context = browser.contexts[0]

    page = context.pages[0]

    page.on("response", handle_response)

    page.goto(
        f"https://item.fril.jp/{ITEM_HASH}",
        wait_until="networkidle"
    )

    input("終了 Enter")