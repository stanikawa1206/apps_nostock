from playwright.sync_api import sync_playwright
from apps.adapters.mercari_item_status import (
    detect_status_from_mercari,
    detect_status_from_mercari_shops
)

URL = "https://jp.mercari.com/shops/product/2JMuJobBJFXDw8iQ9gvanK"


def detect_status_auto(page, driver, url):
    if "/shops/" in url:
        driver.get(url)   
        return detect_status_from_mercari_shops(driver)
    else:
        return detect_status_from_mercari(page, url)
    
def run():
    print(f"Checking URL: {URL}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()
        
        from selenium import webdriver
        driver = webdriver.Chrome()
        # ★ここだけ変える
        status, price = detect_status_auto(page, driver, URL)

        print("--- RESULT ---")
        print("STATUS:", status)
        print("PRICE:", price)
        
        browser.close()

if __name__ == "__main__":
    run()