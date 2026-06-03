import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

TARGET_URL = "https://jp.mercari.com/mypage/purchases"


def main():
    options = Options()
    options.debugger_address = "127.0.0.1:9222"

    driver = webdriver.Chrome(options=options)

    driver.get(TARGET_URL)
    time.sleep(5)

    current_url = driver.current_url
    page_title = driver.title
    print(f"URL: {current_url}")
    print(f"Title: {page_title}")

    if "login" in current_url or "sign_in" in current_url or "signin" in current_url:
        print("NG: ログインされていない可能性あり")
        return

    body_text = driver.find_element("tag name", "body").text
    keywords = ["購入した商品", "購入履歴", "購入した", "mypage/purchases"]
    found = any(kw in body_text for kw in keywords)

    if found:
        print("OK: 購入履歴ページ表示確認成功")
    else:
        print("NG: 購入履歴ページを確認できない")
        print("--- body 先頭300文字 ---")
        print(body_text[:300])


if __name__ == "__main__":
    main()
