from playwright.sync_api import sync_playwright
import time


URL = "https://jp.mercari.com/search?keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3"

TEST_COUNT = 100

def main():

    with sync_playwright() as p:

        # ------------------------------
        # 1. ブラウザ起動 & ページ作成
        # ------------------------------
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # ------------------------------
        # 2. 通信ログ監視
        # entities:search API の
        # request / response をログ出力
        # ------------------------------
        page.on(
            "request",
            lambda r: "entities:search" in r.url and print("REQ:", r.url, flush=True)
        )

        page.on(
            "response",
            lambda r: "entities:search" in r.url and print("RES:", r.url, flush=True)
        )

        # ------------------------------
        # 3. TESTループ
        # ------------------------------
        for i in range(TEST_COUNT):

            print("TEST", i+1)

            try:

                # -------------------------------------------------
                # 4. 先に entities:search の response を待つ準備
                #   → Playwright が「次に来る response」を捕まえる
                # -------------------------------------------------
                with page.expect_response(
                    lambda r: "entities:search" in r.url,
                    timeout=10000
                ) as resp_info:

                    # -------------------------------------------------
                    # 5. ページを開く
                    #   → Mercari が entities:search API を呼ぶ
                    # -------------------------------------------------
                    page.goto(URL)

                # -------------------------------------------------
                # 6. entities:search の response を取得
                # -------------------------------------------------
                response = resp_info.value

                # 必要ならJSON取得
                # data = response.json()

            except Exception:
                print("TIMEOUT")

            time.sleep(1)

        # ------------------------------
        # 7. ブラウザ終了
        # ------------------------------
        browser.close()


if __name__ == "__main__":
    main()