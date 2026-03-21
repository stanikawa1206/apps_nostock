# filename: test_detect_status_from_mercari.py
from playwright.sync_api import sync_playwright
from apps.adapters.mercari_item_status import detect_status_from_mercari

URL = "https://jp.mercari.com/item/m55924954013"

def run():
    print(f"Checking URL: {URL}")
    
    # Playwrightを開始してpageを作成する
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # 本番と同じようにcontextを作成
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # 第一引数に page を渡す
        status, price = detect_status_from_mercari(page, URL)

        print("--- RESULT ---")
        print("STATUS:", status)
        print("PRICE:", price)
        
        browser.close()

if __name__ == "__main__":
    run()