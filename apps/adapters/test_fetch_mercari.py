import json
import time
from playwright.sync_api import sync_playwright
from typing import Optional, Dict, Any

URL = "https://jp.mercari.com/shops/product/2JGVWPfEwRDg9sqRWqgUxE"

def fetch_shops_data(page, url: str):
    try:
        # XHRフック
        page.add_init_script("""
        (function() {
            const origOpen = XMLHttpRequest.prototype.open;
            const origSend = XMLHttpRequest.prototype.send;

            XMLHttpRequest.prototype.open = function(method, url) {
                this._url = url;
                return origOpen.apply(this, arguments);
            };

            XMLHttpRequest.prototype.send = function() {
                this.addEventListener('load', function() {
                    try {
                        if (this._url && this._url.includes('products') && this._url.includes('view=FULL')) {
                            const json = JSON.parse(this.responseText);
                            window.__XHR_DATA__ = json;
                        }
                    } catch (e) {}
                });
                return origSend.apply(this, arguments);
            };
        })();
        """)

        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # 最大10秒待つ
        data = None
        for _ in range(20):
            data = page.evaluate("() => window.__XHR_DATA__")
            if data:
                break
            page.wait_for_timeout(500)

        if not data:
            return None

        res = data

        detail = res.get("productDetail", {})
        shop = detail.get("shop", {})

        return {
            "vendor_name": "メルカリshops",
            "item_id": res.get("name"),
            "title_jp": res.get("displayName"),
            "price": int(res.get("price", 0)),
            "description": detail.get("description"),
            "images": detail.get("photos", []),
            "shipping_region": detail.get("shippingFromArea", {}).get("displayName"),
            "shipping_days": detail.get("shippingDuration", {}).get("displayName"),
            "seller_id": shop.get("name"),
            "seller_name": shop.get("displayName"),
            "rating_count": int(shop.get("shopStats", {}).get("reviewCount", 0)),
            "last_updated_str": res.get("updateTime"),
        }

    except Exception as e:
        print(f"Error during fetch: {e}")
        return None
    
def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )

        page = context.new_page()

        print(f"Testing Shops URL: {URL}")
        rec = fetch_shops_data(page, URL)

        if rec:
            print("\n" + "="*30)
            print("✨ SHOPS SCRAPE SUCCESS ✨")
            print("="*30)
            for k, v in rec.items():
                if k == "description" and v:
                    print(f"{k}: {v[:60].replace('\\n', ' ')}...")
                elif k == "images":
                    print(f"{k}: {len(v)}枚取得")
                else:
                    print(f"{k}: {v}")
        else:
            print("\n❌ Failed to capture payload.")

        time.sleep(5)
        browser.close()


if __name__ == "__main__":
    run()