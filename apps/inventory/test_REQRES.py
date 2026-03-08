from playwright.sync_api import sync_playwright
import time


URL = "https://jp.mercari.com/search?keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3"

TEST_COUNT = 100

def main():

    with sync_playwright() as p:

        # ------------------------------
        # 1. ブラウザ起動
        # ------------------------------
        browser = p.chromium.launch(headless=True)

        # ------------------------------
        # 2. TESTループ
        # ------------------------------
        for i in range(TEST_COUNT):

            print("TEST", i+1)

            # ------------------------------
            # TESTごとに新しいタブを作る
            # → SPA状態をリセット
            # ------------------------------
            page = browser.new_page()

            # ------------------------------
            # entities:search 通信ログ
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
            # URL
            # page_token を変える
            # ------------------------------
            URL = f"https://jp.mercari.com/search?keyword=%E3%83%B4%E3%82%A3%E3%83%88%E3%83%B3&page_token=v1:{i}"

            try:

                # -------------------------------------------------
                # entities:search response 待機
                # -------------------------------------------------
                with page.expect_response(
                    lambda r: "entities:search" in r.url,
                    timeout=10000
                ) as resp_info:

                    # -------------------------------------------------
                    # ページを開く
                    # -------------------------------------------------
                    page.goto(URL, wait_until="domcontentloaded")

                # -------------------------------------------------
                # response取得
                # -------------------------------------------------
                response = resp_info.value

                # 必要ならJSON
                # data = response.json()

            except Exception:
                print("TIMEOUT")

            time.sleep(1)

            # ------------------------------
            # タブを閉じる
            # ------------------------------
            page.close()

        # ------------------------------
        # ブラウザ終了
        # ------------------------------
        browser.close()

if __name__ == "__main__":
    main()