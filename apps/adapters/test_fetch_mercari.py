import json
import time
from playwright.sync_api import sync_playwright
from typing import Optional, Dict, Any

URL = "https://jp.mercari.com/shops/product/2JGVWPfEwRDg9sqRWqgUxE"

def fetch_shops_data(page, url: str) -> Optional[Dict[str, Any]]:
    api_payload = {"data": None}

    def handle_response(response):
            # 画像にあるような、IDが含まれ、かつ 'view=FULL' が含まれるURLを狙い撃ちする
            if "view=FULL" in response.url:
                try:
                    # このレスポンスの中身に productDetail が入っているはずです
                    data = response.json()
                    print(f"🎯 ターゲットAPIを捕捉しました: {response.url[:50]}...")
                    api_payload["data"] = data
                except:
                    pass


    page.on("response", handle_response)
    
    try:
        # ヘッドレスでない状態で起動するので、動きが見えます
        page.goto(url, wait_until="load", timeout=30000)
        
        # 1. メルカリ特有の「同意する」ボタンなどがあれば適当にクリック（必要なら手動でもOK）
        # 2. APIが走るように少しスクロール
        for _ in range(5):
            if api_payload["data"]: break
            page.mouse.wheel(0, 500)
            page.wait_for_timeout(1000)

        # APIが捕捉できるまで最大10秒待機
        timeout = time.time() + 10
        while api_payload["data"] is None and time.time() < timeout:
            page.wait_for_timeout(500)

        res = api_payload["data"]
        if not res: return None

        # --- Shops専用マッピング ---
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
    finally:
        page.remove_listener("response", handle_response)

def run():
    with sync_playwright() as p:
        # ★ 目視確認のため headless=False に設定
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
            print("ブラウザ画面を確認してください。ボット判定や同意画面で止まっていませんか？")
        
        # 結果を確認するために少し待機してから閉じる
        time.sleep(5)
        browser.close()

if __name__ == "__main__":
    run()