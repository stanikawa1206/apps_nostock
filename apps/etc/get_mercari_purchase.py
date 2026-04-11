from playwright.sync_api import sync_playwright
import time
import os
from datetime import datetime

TARGET_DATE = datetime(2026, 3, 16)

def parse_date(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")


def run():
    with sync_playwright() as p:
        # デスクトップのプロファイル
        save_path = os.path.join(os.environ["USERPROFILE"], "Desktop", "mercari_session")

        context = p.chromium.launch_persistent_context(
            user_data_dir=save_path,
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            headless=False,
            # 通信を軽くするための設定を追加
            bypass_csp=True, 
            ignore_https_errors=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process", # サイト間の制限を緩める
            ],
            ignore_default_args=["--enable-automation"]
        )

        # 最初のページを強制的にGoogleにする
        page = context.pages[0] if context.pages else context.new_page()
        
        print("ブラウザ起動。Googleへ直接移動を試みます...")
        
        try:
            # タイムアウトを15秒に絞って、まずGoogleへ
            page.goto("https://www.google.com", timeout=15000)
            print("Googleが開きました。次にメルカリへ移動します。")
            page.goto("https://jp.mercari.com/mypage/purchases")
        except Exception as e:
            print(f"移動中にエラー（またはタイムアウト）: {e}")
            print("手動でアドレスバーに URL を打ち込んでみてください。")

        print("--- 待機中 ---")
        input("ログインできたらEnterを押してください >>> ")
        context.close()

def run_bk():
    with sync_playwright() as p:
       
        context = p.chromium.launch_persistent_context(
            user_data_dir=r"C:\Users\stani\AppData\Local\Google\Chrome\User Data\Profile 7",
            executable_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            headless=False
        )

        page = context.new_page()

        latest_json = {}
        stop_flag = False

        def handle_response(response):
            nonlocal latest_json

            if "orders" in response.url:
                try:
                    data = response.json()
                    if "orders" in data:
                        latest_json = data
                        print("JSON取得:", len(data["orders"]))
                except:
                    pass

        page.on("response", handle_response)

        page.goto("https://jp.mercari.com/mypage/purchases")
        time.sleep(3)

        while not stop_flag:

            # もっと見るクリック
            try:
                page.click("text=もっと見る")
                time.sleep(2)
            except:
                print("もっと見るなし → 終了")
                break

            # JSON処理
            if "orders" in latest_json:
                for order in latest_json["orders"]:

                    create_time = parse_date(order["createTime"])

                    # 日付チェック
                    if create_time < TARGET_DATE:
                        print("指定日付に到達 → 終了")
                        stop_flag = True
                        break

                    try:
                        origin_id = order["orderDetail"]["lineItems"][0]["product"]["originId"]
                        print(origin_id)
                    except:
                        pass

        context.close()


if __name__ == "__main__":
    run()