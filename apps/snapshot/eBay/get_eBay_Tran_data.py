import sys
import time
import csv
import os
from datetime import datetime, timezone
from typing import Optional
import requests

# 1. 'apps' フォルダの大元をパスに追加する
sys.path.append(r"\\MOUSE\apps_nostock")

# 2. 'apps.adapters.ebay_api' から関数をインポートする
from apps.adapters.ebay_api import get_access_token_new

def get_ebay_transactions(account: str, start_date: str, end_date: str) -> Optional[list]:
    """
    eBay Finances APIを使用して、トランザクション履歴（売上、返金、手数料など）を全件取得します。
    """
    access_token = get_access_token_new(account)
    
    if not access_token:
        print(f"❌ {account} のアクセストークンが取得できないため、中断します。")
        return None

    url = "https://apiz.ebay.com/sell/finances/v1/transaction"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    params = {
        "filter": f"transactionDate:[{start_date}..{end_date}]",
        "limit": 100 
    }

    all_transactions = []
    page_count = 1

    try:
        while url:
            print(f"  └ {page_count}ページ目を取得中...")
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() 
            data = response.json()
            
            current_transactions = data.get('transactions', [])
            all_transactions.extend(current_transactions)
            
            if 'next' in data and data['next']:
                url = data['next']
                params = None 
                page_count += 1
                time.sleep(0.5)
            else:
                url = None

        print(f"✅ 合計 {len(all_transactions)} 件のトランザクションデータを取得しました。")
        return all_transactions

    except requests.exceptions.RequestException as e:
        print(f"❌ APIエラー: {e}")
        if e.response is not None:
            print(f"エラー詳細: {e.response.text}")
        return None


if __name__ == "__main__":
    account_list = [
        "BUZZ", "貴文", "貴文②", "川島", "谷川", "谷川②", "谷川③", "谷川④"
    ]

    print("=== eBay トランザクション(売上/手数料)データ取得 ===")
    print("以下から対象のアカウント番号を選択してください:")
    for i, account in enumerate(account_list, 1):
        print(f"  {i}: {account}")
    
    print("-" * 30)
    user_input = input("番号を入力 (例: 1) > ")

    try:
        selected_index = int(user_input) - 1 
        if 0 <= selected_index < len(account_list):
            selected_account = account_list[selected_index]
        else:
            print("\n❌ エラー: 無効な番号です。プログラムを終了します。")
            sys.exit(1)
    except ValueError:
        print("\n❌ エラー: 数字を入力してください。プログラムを終了します。")
        sys.exit(1)

    print(f"\n▶ [{selected_account}] を選択しました。データ取得を開始します...")

    now = datetime.now(timezone.utc)
    # 開始日を 2024年4月1日 に固定設定する
    start_date = "2024-04-01T00:00:00.000Z"
    # 終了日は現在時刻をそのまま使用する
    end_date = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    print(f"取得期間: {start_date} 〜 {end_date}\n")

    transactions_data = get_ebay_transactions(selected_account, start_date, end_date)
    
    if transactions_data is not None and len(transactions_data) > 0:
        print("\n=== データ取得完了。CSVファイルへ出力します ===")
        
        # 出力先フォルダの設定
        output_dir = r"\\MOUSE\apps_nostock\apps\snapshot\eBay\transaction"
        os.makedirs(output_dir, exist_ok=True)
        
        csv_filename = f"eBay_Transactions_{selected_account}.csv"
        csv_filepath = os.path.join(output_dir, csv_filename)
        
        # 1つのセルに1つの値が入るように、各手数料の列を独立させました
        headers = [
            "Transaction Date (取引日)",
            "Transaction Type (タイプ)",
            "Order ID (オーダーID)",
            "Buyer Username (購入者ID)",
            "Gross Amount (総額)",
            "Final Value Fee (落札手数料)",           # 追加: 手数料内訳①
            "Fixed Fee (固定手数料)",                 # 追加: 手数料内訳②
            "International Fee (海外手数料)",         # 追加: 手数料内訳③
            "Total Fee (手数料合計)",
            "Net Amount (純額)",
            "Currency (通貨)",
            "Transaction Status (ステータス)",
            "Memo (メモ)"
        ]
        
        with open(csv_filepath, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            for t in transactions_data:
                # 基本情報の取得
                date = t.get('transactionDate', '')
                t_type = t.get('transactionType', '')
                order_id = t.get('orderId', '')
                buyer = t.get('buyer', {}).get('username', '')
                
                # 全体の金額関係
                gross_amount = t.get('totalFeeBasisAmount', {}).get('value', '0.0') # 総額
                total_fee = t.get('totalFeeAmount', {}).get('value', '0.0')         # 手数料合計
                net_amount = t.get('amount', {}).get('value', '0.0')                # 純額
                currency = t.get('amount', {}).get('currency', '')
                
                # 手数料の内訳を取得するための変数（初期値は空欄、または '0.0'）
                final_value_fee = "0.0"
                fixed_fee = "0.0"
                international_fee = "0.0"

                # ネストされたJSON（orderLineItems -> marketplaceFees）を展開して1つの値を取り出す
                order_line_items = t.get('orderLineItems', [])
                for item in order_line_items:
                    for fee in item.get('marketplaceFees', []):
                        fee_type = fee.get('feeType', '')
                        fee_value = fee.get('amount', {}).get('value', '0.0')
                        
                        # 手数料の種類ごとに変数を振り分ける
                        if fee_type == 'FINAL_VALUE_FEE':
                            final_value_fee = fee_value
                        elif fee_type == 'FINAL_VALUE_FEE_FIXED_PER_ORDER':
                            fixed_fee = fee_value
                        elif fee_type == 'INTERNATIONAL_FEE':
                            international_fee = fee_value

                status = t.get('transactionStatus', '')
                memo = t.get('transactionMemo', '')
                
                # CSVに書き込み (1つの変数につき、1つのセルに入る)
                writer.writerow([
                    date, t_type, order_id, buyer, 
                    gross_amount, 
                    final_value_fee, fixed_fee, international_fee, # 個別に取り出した手数料
                    total_fee, 
                    net_amount, currency, status, memo
                ])
                
        print(f"✅ 全 {len(transactions_data)} 件のデータを以下の場所に出力しました！\n📁 {csv_filepath}")
    else:
        print("⚠️ データの取得に失敗したか、指定期間にデータがありません。")