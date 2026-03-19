# test_mercari_status.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from apps.adapters.mercari_item_status import detect_status_from_mercari
import time

URL = "https://jp.mercari.com/item/m70979011584"


def run():
    options = Options()
    options.add_argument("--start-maximized")
    # メルカリにBotとバレにくくするためのUser-Agent設定（推奨）
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    driver = webdriver.Chrome(options=options)

    try:
        driver.get(URL)

        # --- ここを追加：画面が出てから3〜5秒ほど待機 ---
        # ネットワーク環境やPCスペックにより、JSONの展開に時間がかかるため
        time.sleep(5) 

        # 念のため、現在表示されているURLがリダイレクトされていないか確認
        print(f"Current URL: {driver.current_url}")

        status, price = detect_status_from_mercari(driver)

        print("STATUS:", status)
        print("PRICE:", price)

    finally:
        driver.quit()


if __name__ == "__main__":
    run()