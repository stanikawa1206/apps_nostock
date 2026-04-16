import sys
import time
import csv
import os  # フォルダ作成やパス結合のために追加
from datetime import datetime, timedelta, timezone
from typing import Optional
import requests

# 1. 'apps' フォルダの大元をパスに追加する
sys.path.append(r"\\MOUSE\apps_nostock")

# 2. 'apps.adapters.ebay_api' から関数をインポートする
from apps.adapters.ebay_api import get_access_token_new

def get_ebay_payouts(account: str, start_date: str, end_date: str) -> Optional[list]:
    """
    eBay Finances APIを使用して、Payoneerへの出金履歴（Payout）を全件取得します。
    """
    access_token = get_access_token_new(account)
    
    if not access_token:
        print(f"❌ {account} のアクセストークンが取得できないため、中断します。")
        return None

    # Payout専用のURL
    url = "https://apiz.ebay.com/sell/finances/v1/payout"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # フィルターを payoutDate に変更
    params = {
        "filter": f"payoutDate:[{start_date}..{end_date}]",
        "limit": 100 
    }

    all_payouts = []
    page_count = 1

    try:
        while url:
            print(f"  └ {page_count}ページ目を取得中...")
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() 
            data = response.json()
            
            # 【重要】取得するキーを 'transactions' から 'payouts' に変更！
            current_payouts = data.get('payouts', [])
            all_payouts.extend(current_payouts)
            
            if 'next' in data and data['next']:
                url = data['next']
                params = None 
                page_count += 1
                time.sleep(0.5)
            else:
                url = None

        print(f"✅ 合計 {len(all_payouts)} 件の出金データを取得しました。")
        return all_payouts

    except requests.exceptions.RequestException as e:
        print(f"❌ APIエラー: {e}")
        if e.response is not None:
            print(f"エラー詳細: {e.response.text}")
        return None

if __name__ == "__main__":
    account_list = [
        "BUZZ", "貴文", "貴文②", "川島", "谷川", "谷川②", "谷川③", "谷川④"
    ]

    print("=== eBay 出金履歴(Payout)データ取得 ===")
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

    # 4. 期間の設定 (2024年4月1日から現在まで)
    now = datetime.now(timezone.utc)
    
    # 開始日を 2024年4月1日 に固定設定する
    start_date = "2024-04-01T00:00:00.000Z"
    
    # 終了日は現在時刻をそのまま使用する
    end_date = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    print(f"取得期間: {start_date} 〜 {end_date}\n")

    # API実行
    payouts_data = get_ebay_payouts(selected_account, start_date, end_date)
    
    if payouts_data is not None and len(payouts_data) > 0:
        print("\n=== データ取得完了。CSVファイルへ出力します ===")
        
        # 【追加・変更箇所】出力先ディレクトリの設定
        output_dir = r"\\MOUSE\apps_nostock\apps\snapshot\eBay\Payout"
        
        # フォルダが存在しない場合は作成
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存するCSVファイル名
        csv_filename = f"eBay_Payouts_{selected_account}.csv"
        
        # フルパスを生成
        csv_filepath = os.path.join(output_dir, csv_filename)
        
        # 出金履歴に必要な項目だけを綺麗に並べたヘッダー
        headers = [
            "Payout Date (出金日)",
            "Payout ID (出金ID)",
            "Amount (出金額 USD)",
            "Status (ステータス)",
            "Bank Reference (銀行振込参照番号)"
        ]
        
        # filepathを使用して書き込み
        with open(csv_filepath, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            for p in payouts_data:
                # Payout APIの構造に合わせてデータを取り出す
                date = p.get('payoutDate', '')
                p_id = p.get('payoutId', '')
                amount = p.get('amount', {}).get('value', '0.0')
                status = p.get('payoutStatus', '')
                bank_ref = p.get('bankReference', '')
                
                # CSVに書き込み
                writer.writerow([date, p_id, amount, status, bank_ref])
                
        print(f"✅ 全 {len(payouts_data)} 件の出金データを以下の場所に出力しました！\n📁 {csv_filepath}")
    else:
        print("⚠️ データの取得に失敗したか、指定期間にデータがありません。")