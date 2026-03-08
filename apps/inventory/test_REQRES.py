from playwright.sync_api import sync_playwright
import time


URL = "https://jp.mercari.com/search?keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3"

TEST_COUNT = 100

def main():

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.on(
            "request",
            lambda r: "entities:search" in r.url and print("REQ:", r.url, flush=True)
        )

        page.on(
            "response",
            lambda r: "entities:search" in r.url and print("RES:", r.url, flush=True)
        )

        for i in range(TEST_COUNT):

            print("TEST", i+1)

            try:
                page.goto(URL)

                page.wait_for_response(
                    lambda r: "entities:search" in r.url,
                    timeout=10000
                )

            except Exception as e:
                print("TIMEOUT")

            time.sleep(1)

        browser.close()


if __name__ == "__main__":
    main()