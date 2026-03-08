from playwright.sync_api import sync_playwright
import time

URL = "ここにメルカリ検索URL"
TEST_COUNT = 100

def main():

    req_count = 0
    res_count = 0
    timeout_count = 0

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for i in range(TEST_COUNT):

            req_hit = False
            res_hit = False

            def on_req(r):
                nonlocal req_hit
                if "entities:search" in r.url:
                    req_hit = True

            def on_res(r):
                nonlocal res_hit
                if "entities:search" in r.url:
                    res_hit = True

            page.on("request", on_req)
            page.on("response", on_res)

            try:
                page.goto(URL)

                page.wait_for_response(
                    lambda r: "entities:search" in r.url,
                    timeout=10000
                )

            except:
                timeout_count += 1

            if req_hit:
                req_count += 1

            if res_hit:
                res_count += 1

            page.remove_listener("request", on_req)
            page.remove_listener("response", on_res)

            print(f"test {i+1} done")

            time.sleep(1)

        browser.close()

    print("----- RESULT -----")
    print("REQ:", req_count)
    print("RES:", res_count)
    print("TIMEOUT:", timeout_count)

if __name__ == "__main__":
    main()