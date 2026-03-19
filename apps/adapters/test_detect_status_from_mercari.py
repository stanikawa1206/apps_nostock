# --- 修正後の呼び出し側イメージ ---
from apps.adapters.mercari_item_status import detect_status_from_mercari
URL = "https://jp.mercari.com/item/m14238614812"

def run():
    # Seleniumのdriverは不要なので削除
    print(f"Checking URL: {URL}")
    
    # 直接 URL を渡す
    status, price = detect_status_from_mercari(URL)

    print("STATUS:", status)
    print("PRICE:", price)

if __name__ == "__main__":
    run()