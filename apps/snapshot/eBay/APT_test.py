import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests

# 1. 'apps' フォルダの大元をパスに追加する
sys.path.append(r"\\MOUSE\apps_nostock")

# 2. 'apps.adapters.ebay_api' から関数をインポートする
from apps.adapters.ebay_api import get_access_token_new

def fetch_all_data_with_pagination(url: str, headers: dict, params: dict, data_key: str, api_name: str) -> list:
    """
    ページネーション（次ページ）を辿って、指定期間のデータを全件取得します。
    """
    all_items = []
    page_count = 1
    current_url = url
    current_params = params

    try:
        while current_url:
            print(f"  └ {api_name}: {page_count}ページ目を取得中...")
            response = requests.get(current_url, headers=headers, params=current_params)
            response.raise_for_status()
            data = response.json()
            
            # データの取得と結合
            items = data.get(data_key, [])
            all_items.extend(items)
            
            # 次のページがあるか確認
            if 'next' in data and data['next']:
                current_url = data['next']
                current_params = None  # nextのURLにはパラメータが既に含まれているためクリア
                page_count += 1
                time.sleep(0.5) # API制限回避のための待機
            else:
                current_url = None

        print(f"✅ {api_name}: 合計 {len(all_items)} 件のデータを取得しました。")
        return all_items

    except requests.exceptions.RequestException as e:
        print(f"❌ {api_name} APIエラー: {e}")
        if e.response is not None:
            print(f"エラー詳細: {e.response.text}")
        return []

def get_1year_data_to_csv(account: str):
    print(f"\n▶ [{account}] のアクセストークンを取得中...")
    access_token = get_access_token_new(account)
    
    if not access_token:
        print(f"❌ {account} のアクセストークンが取得できませんでした。")
        return

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # ---------------------------------------------------------
    # 期間設定：現在から「365日前」を自動計算する
    # ---------------------------------------------------------
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=365) # 1年前
    
    start_date = start.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_date = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    print(f"📅 取得対象期間: {start_date} 〜 {end_date}")

    # 出力先フォルダの作成 (1年分のデータ用)
    output_dir = r"\\MOUSE\apps_nostock\apps\snapshot\eBay\1YearData"
    os.makedirs(output_dir, exist_ok=True)

    # APIごとの設定リスト
    api_configs = [
        {
            "name": "Transaction (会計明細)",
            "url": "https://apiz.ebay.com/sell/finances/v1/transaction",
            "filter_key": "transactionDate",
            "data_key": "transactions",
            "filename": f"{account}_Transaction_1Year.csv"
        },
        {
            "name": "Payout (出金履歴)",
            "url": "https://apiz.ebay.com/sell/finances/v1/payout",
            "filter_key": "payoutDate",
            "data_key": "payouts",
            "filename": f"{account}_Payout_1Year.csv"
        },
        {
            "name": "Fulfillment (受注情報)",
            "url": "https://api.ebay.com/sell/fulfillment/v1/order",
            "filter_key": "creationdate",
            "data_key": "orders",
            "filename": f"{account}_Fulfillment_1Year.csv"
        }
    ]

    for config in api_configs:
        print(f"\n--- {config['name']} の取得を開始 ---")
        
        # パラメータの組み立て（リミットは1回の通信の最大値100を指定）
        params = {
            "filter": f"{config['filter_key']}:[{start_date}..{end_date}]",
            "limit": 100
        }
        
        # 全件取得関数の呼び出し
        all_data = fetch_all_data_with_pagination(
            config['url'], headers, params, config['data_key'], config['name']
        )
        
        # データがあればCSVに変換して保存
        if all_data:
            df = pd.json_normalize(all_data)
            output_path = os.path.join(output_dir, config['filename'])
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"💾 CSV出力完了: {output_path}")
        else:
            print(f"⚠️ {config['name']} は出力するデータがありませんでした。")

if __name__ == "__main__":
    account_list = [
        "BUZZ", "貴文", "貴文②", "川島", "谷川", "谷川②", "谷川③", "谷川④"
    ]

    print("=== eBay API 1年分データ一括取得＆CSV出力ツール ===")
    for i, account in enumerate(account_list, 1):
        print(f"  {i}: {account}")
    
    print("-" * 30)
    user_input = input("取得したいアカウントの番号を入力 (例: 1) > ")

    try:
        selected_index = int(user_input) - 1 
        if 0 <= selected_index < len(account_list):
            selected_account = account_list[selected_index]
            get_1year_data_to_csv(selected_account)
        else:
            print("❌ 無効な番号です。")
    except ValueError:
        print("❌ 数字を入力してください。")