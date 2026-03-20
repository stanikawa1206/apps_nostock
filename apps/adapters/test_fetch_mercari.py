import json
from playwright.sync_api import sync_playwright
from typing import Tuple, Optional, Dict, Any

URL = "https://jp.mercari.com/item/m38115011744"

def fetch_full_data(page, url: str) -> Optional[Dict[str, Any]]:
    """APIから全データを取得してrec形式に変換するテスト関数"""
    api_payload = {"data": None}

    def handle_response(response):
        if "api.mercari.jp/items/get?id=" in response.url:
            try:
                api_payload["data"] = response.json()
            except: pass

    page.on("response", handle_response)
    try:
        with page.expect_response(lambda res: "api.mercari.jp/items/get?id=" in res.url, timeout=10000):
            page.goto(url, wait_until="commit")
        
        res = api_payload["data"]
        if not res or res.get("result") != "OK":
            return None

        item = res.get("data", {})
        
        # --- recの組み立て ---
        rec = {
            "vendor_name": "メルカリ",
            "item_id": item.get("id"),
            "title_jp": item.get("name"),
            "price": item.get("price"),
            "description": item.get("description"),
            "images": item.get("photos", []),
            "shipping_region": item.get("shipping_from_area", {}).get("name"),
            "shipping_days": item.get("shipping_duration", {}).get("name"),
            "seller_id": str(item.get("seller", {}).get("id")),
            "seller_name": item.get("seller", {}).get("name"),
            "rating_count": item.get("seller", {}).get("num_ratings"),
            "status": item.get("status")
        }
        return rec
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        page.remove_listener("response", handle_response)

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0...")
        page = context.new_page()

        print(f"Testing URL: {URL}")
        rec = fetch_full_data(page, URL)

        if rec:
            print("\n--- SCRAPE RESULT ---")
            for key, val in rec.items():
                if key == "description":
                    print(f"{key}: {val[:50]}...") # 長いのでカット
                elif key == "images":
                    print(f"{key}: {len(val)}枚取得")
                else:
                    print(f"{key}: {val}")
        else:
            print("Failed to capture API.")
        
        browser.close()

if __name__ == "__main__":
    run()