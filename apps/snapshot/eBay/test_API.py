import sys
import json
from datetime import datetime, timezone
import requests

# 1. 'apps' フォルダの大元をパスに追加する
sys.path.append(r"\\MOUSE\apps_nostock")

# 2. 'apps.adapters.ebay_api' から関数をインポートする
from apps.adapters.ebay_api import get_access_token_new

def check_raw_transaction_data(account: str):
    """
    eBay Finances APIから最新のトランザクションを1件だけ取得し、
    どのようなデータが含まれているか生のJSONを表示します。
    """
    print(f"\n▶ [{account}] のアクセストークンを取得中...")
    access_token = get_access_token_new(account)
    
    if not access_token:
        print(f"❌ {account} のアクセストークンが取得できませんでした。")
        return

    url = "https://apiz.ebay.com/sell/finances/v1/transaction"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # テスト用なので、取得件数を「1件」に絞る
    now = datetime.now(timezone.utc)
    start_date = "2024-04-01T00:00:00.000Z"
    end_date = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    params = {
        "filter": f"transactionDate:[{start_date}..{end_date}]",
        "limit": 1  # 1件だけ取得
    }

    print("▶ APIへリクエストを送信中...\n")
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        transactions = data.get('transactions', [])
        
        if transactions:
            print("=== 取得できるデータの中身（1件目） ===")
            # json.dumps を使って、見やすくインデントして表示
            formatted_json = json.dumps(transactions[0], indent=4, ensure_ascii=False)
            print(formatted_json)
            print("=======================================")
        else:
            print("⚠️ 指定期間内にトランザクションデータがありませんでした。")

    except requests.exceptions.RequestException as e:
        print(f"❌ APIエラー: {e}")
        if e.response is not None:
            print(f"エラー詳細: {e.response.text}")


if __name__ == "__main__":
    account_list = [
        "BUZZ", "貴文", "貴文②", "川島", "谷川", "谷川②", "谷川③", "谷川④"
    ]

    print("=== 取得できるデータ確認ツール ===")
    for i, account in enumerate(account_list, 1):
        print(f"  {i}: {account}")
    
    print("-" * 30)
    user_input = input("確認したいアカウントの番号を入力 (例: 1) > ")

    try:
        selected_index = int(user_input) - 1 
        if 0 <= selected_index < len(account_list):
            selected_account = account_list[selected_index]
            check_raw_transaction_data(selected_account)
        else:
            print("❌ 無効な番号です。")
    except ValueError:
        print("❌ 数字を入力してください。")