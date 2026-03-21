import json
import time
import requests
from playwright.sync_api import sync_playwright

URL = "https://jp.mercari.com/shops/product/2JGVWPfEwRDg9sqRWqgUxE"


def build_cookie_header(cookies):
    return "; ".join([f"{c['name']}={c['value']}" for c in cookies])


def fetch_shops_data_requests(cookie_header: str):

    product_id = URL.split("/")[-1] 
    # ★ APIエンドポイント（Shops詳細）
    api_url = "https://api.mercari.jp/shops/v1/products/get"

    params = {
        "view": "FULL"
    }

    # ★ URLからID取り出し
    product_id = URL.split("/")[-1]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ja-JP,ja;q=0.9",
        "Content-Type": "application/json",
        "Origin": "https://jp.mercari.com",
        "Referer": URL,
        "Cookie": cookie_header,
    }

    params = {
        "id": product_id,
        "view": "FULL"
    }

    res = requests.get(api_url, headers=headers, params=params)

    if res.status_code != 200:
        print("❌ status:", res.status_code)
        return None

    data = res.json()

    detail = data.get("productDetail", {})
    shop = detail.get("shop", {})

    return {
        "vendor_name": "メルカリshops",
        "item_id": data.get("name"),
        "title_jp": data.get("displayName"),
        "price": int(data.get("price", 0)),
        "description": detail.get("description"),
        "images": detail.get("photos", []),
        "shipping_region": detail.get("shippingFromArea", {}).get("displayName"),
        "shipping_days": detail.get("shippingDuration", {}).get("displayName"),
        "seller_id": shop.get("name"),
        "seller_name": shop.get("displayName"),
        "rating_count": int(shop.get("shopStats", {}).get("reviewCount", 0)),
        "last_updated_str": data.get("updateTime"),
    }


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )

        page = context.new_page()

        # ★ cookie取得用に1回アクセス
        page.goto("https://jp.mercari.com", wait_until="domcontentloaded")

        cookies = context.cookies()
        cookie_header = build_cookie_header(cookies)

        print("🍪 Cookie取得完了")

        rec = fetch_shops_data_requests(cookie_header)

        if rec:
            print("\n" + "="*30)
            print("✨ REQUESTS SUCCESS ✨")
            print("="*30)
            for k, v in rec.items():
                if k == "description" and v:
                    print(f"{k}: {v[:60].replace('\\n', ' ')}...")
                elif k == "images":
                    print(f"{k}: {len(v)}枚取得")
                else:
                    print(f"{k}: {v}")
        else:
            print("\n❌ Failed")

        time.sleep(3)
        browser.close()


if __name__ == "__main__":
    run()