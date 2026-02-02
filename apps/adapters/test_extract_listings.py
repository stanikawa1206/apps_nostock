# -*- coding: utf-8 -*-
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.adapters.mercari_scraper import build_driver, safe_quit, extract_item_listings, setup_mercari_currency_jp, setup_mercari_currency_jp

def main():
    # テスト対象URL
    # 指定されたURL: https://jp.mercari.com/transaction/m37592038789
    # ※ transactionページはログインが必要な場合が多く、未ログインでは取得できない可能性があります。
    #    その場合は https://jp.mercari.com/item/m37592038789 (商品ページ) などに変更してください。
    url = "https://jp.mercari.com/item/m37592038789"
    
    print(f"URL: {url}")
    
    # ドライバー起動
    # 動作を目視したい場合は headless=False に変更してください
    driver = build_driver(headless=True)
    
    try:
        print("Setting up JPY cookies...")
        # ここで日本円設定を強制
        setup_mercari_currency_jp(driver)

        print("Setting up JPY cookies...")
        # ここで日本円設定を強制
        setup_mercari_currency_jp(driver)

        print("Loading page...")
        driver.get(url)
        
        print("Extracting listings...")
        # extract_item_listings はページ内の商品リンク(aタグ)を探して情報を抽出します
        items = extract_item_listings(driver)
        
        print(f"Found {len(items)} items.")
        for i, (item_id, title, price) in enumerate(items, 1):
            print(f"{i}. ID: {item_id}, Price: {price}, Title: {title}")
            
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        safe_quit(driver)

if __name__ == "__main__":
    main()
