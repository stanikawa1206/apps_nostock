import json
from playwright.sync_api import sync_playwright

def run(url):
    with sync_playwright() as p:
        # ブラウザを起動（headless=Falseで見守るのがおすすめ）
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # 通信を監視して、特定のURL（API）が含まれていたら中身を保存する
        captured_data = {}

        def handle_response(response):
            # 「items/get?id=」が含まれる通信だけを狙い撃ち
            if "items/get?id=" in response.url:
                try:
                    if "application/json" in response.headers.get("content-type", ""):
                        data = response.json()
                        captured_data["api_res"] = data
                        print(f"🎯 目的の商品詳細APIをキャプチャ: {response.url}")
                except:
                    pass

        # レスポンスイベントを登録
        page.on("response", handle_response)

        # 商品ページへ移動
        print(f"Navigating to: {url}")
        page.goto(url, wait_until="networkidle") # 通信が落ち着くまで待機

        # 少し待ってAPIが走るのを待つ
        page.wait_for_timeout(5000)

        if "api_res" in captured_data:
            with open("captured_mercari.json", "w", encoding="utf-8") as f:
                json.dump(captured_data["api_res"], f, indent=4, ensure_ascii=False)
            print("🎉 captured_mercari.json に保存完了！")
        else:
            print("❌ 目的のAPI通信が見つかりませんでした。")

        browser.close()

if __name__ == "__main__":
    target_url = "https://jp.mercari.com/item/m86222654096"
    run(target_url)