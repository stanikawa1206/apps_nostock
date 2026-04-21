import sys
import os
import time
import json
import pandas as pd
from datetime import datetime, timezone
import requests

# ---------------------------------------------------------
# 基本設定・モジュールインポート
# ---------------------------------------------------------
# 'apps' フォルダの大元をパスに追加
sys.path.append(r"\\MOUSE\apps_nostock")

# 認証トークン取得関数のインポート
from apps.adapters.ebay_api import get_access_token_new

# データ保存先フォルダの設定
BASE_DIR = r"Y:\ebay\data\API_data"
TARGET_DIR = r"Y:\ebay\data\Report"


def format_date_for_report(date_str):
    """ APIのISO日付文字列を 'Mar 31, 2026' のようなレポート標準形式に変換 """
    if not date_str or str(date_str) == 'nan':
        return "--"
    try:
        dt = pd.to_datetime(date_str)
        return dt.strftime('%b %-d, %Y')
    except:
        return str(date_str)


def fetch_api_data_all_pages(url: str, headers: dict, params: dict, data_key: str, api_name: str) -> list:
    """ ページネーション（Next）を辿ってAPIから対象期間の全データを取得する """
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
            
            items = data.get(data_key, [])
            all_items.extend(items)
            
            # 次のページがある場合はURLを更新してループ
            if 'next' in data and data['next']:
                current_url = data['next']
                current_params = None 
                page_count += 1
                time.sleep(0.5) # 連続アクセス防止の待機
            else:
                current_url = None

        print(f"  ✅ {api_name}: {len(all_items)} 件取得完了")
        return all_items

    except requests.exceptions.RequestException as e:
        print(f"  ❌ {api_name} APIエラー: {e}")
        if e.response is not None:
            print(f"  エラー詳細: {e.response.text}")
        return []


def generate_direct_ebay_report(account: str, start_date_str: str, end_date_str: str):
    """ メイン処理：データ取得からレポート生成までを一貫して行う """
    print(f"\n▶ [{account}] のアクセストークンを取得中...")
    access_token = get_access_token_new(account)
    
    if not access_token:
        print(f"❌ {account} のアクセストークンが取得できませんでした。処理を中断します。")
        return

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # フォルダの作成
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(TARGET_DIR, exist_ok=True)

    # APIリクエスト用の日付フォーマット作成 (UTC)
    try:
        start_dt = pd.to_datetime(start_date_str).strftime('%Y-%m-%dT00:00:00.000Z')
        end_dt = pd.to_datetime(end_date_str).strftime('%Y-%m-%dT23:59:59.000Z')
    except Exception as e:
        print(f"❌ 日付のフォーマットが間違っています。例: 2024-04-01\n{e}")
        return

    print(f"📅 取得対象期間: {start_dt} 〜 {end_dt}")

    # =========================================================
    # 1. APIからデータ全件取得
    # =========================================================
    print(f"\n--- API通信開始 ---")
    
    transactions = fetch_api_data_all_pages(
        "https://apiz.ebay.com/sell/finances/v1/transaction",
        headers, {"filter": f"transactionDate:[{start_dt}..{end_dt}]", "limit": 100}, "transactions", "Transaction"
    )
    
    if not transactions:
        print(f"⚠️ 指定期間に取引データがありませんでした。処理を終了します。")
        return

    payouts = fetch_api_data_all_pages(
        "https://apiz.ebay.com/sell/finances/v1/payout",
        headers, {"filter": f"payoutDate:[{start_dt}..{end_dt}]", "limit": 100}, "payouts", "Payout"
    )

    orders = fetch_api_data_all_pages(
        "https://api.ebay.com/sell/fulfillment/v1/order",
        headers, {"filter": f"creationdate:[{start_dt}..{end_dt}]", "limit": 100}, "orders", "Fulfillment"
    )

    # =========================================================
    # 2. 取得したデータをJSONとしてバックアップ保存
    # =========================================================
    print(f"\n▶ 取得したJSONデータをバックアップ保存しています...")
    file_suffix = f"{start_date_str}_to_{end_date_str}.json"
    
    with open(os.path.join(BASE_DIR, f"{account}_Transaction_{file_suffix}"), 'w', encoding='utf-8') as f:
        json.dump(transactions, f, indent=4, ensure_ascii=False)
    with open(os.path.join(BASE_DIR, f"{account}_Payout_{file_suffix}"), 'w', encoding='utf-8') as f:
        json.dump(payouts, f, indent=4, ensure_ascii=False)
    with open(os.path.join(BASE_DIR, f"{account}_Fulfillment_{file_suffix}"), 'w', encoding='utf-8') as f:
        json.dump(orders, f, indent=4, ensure_ascii=False)
        
    print(f"✅ JSONの保存が完了しました。\n📁 {BASE_DIR}")

    # =========================================================
    # 3. CSVレポート形式へのマッピング（整形）
    # =========================================================
    print(f"\n▶ eBayレポート形式にマッピング中...")
    
    # 検索高速化のための辞書化
    payout_dict = {p.get('payoutId'): p for p in payouts}
    fulfill_dict = {f.get('orderId'): f for f in orders}

    report_rows = []
    columns = [
        "Transaction creation date", "Type", "Order number", "Legacy order ID", 
        "Buyer username", "Buyer name", "Ship to city", "Ship to province/region/state", 
        "Ship to zip", "Ship to country", "Net amount", "Payout currency", "Payout date", 
        "Payout ID", "Payout method", "Payout status", "Reason for hold", "Item ID", 
        "Transaction ID", "Item title", "Custom label", "Quantity", "Item subtotal", 
        "Shipping and handling", "Seller collected tax", "eBay collected tax", 
        "Final Value Fee - fixed", "Final Value Fee - variable", "Regulatory operating fee", 
        "Very high \"item not as described\" fee", "Below standard performance fee", 
        "International fee", "Charity donation", "Deposit processing fee", 
        "Gross transaction amount", "Transaction currency", "Exchange rate", 
        "Reference ID", "Description"
    ]

    # ① Transaction（売上・各種手数料・返金など）の行を作成
    for t in transactions:
        row = {col: "--" for col in columns}
        
        t_type_raw = str(t.get('transactionType', ''))
        row["Transaction creation date"] = format_date_for_report(t.get('transactionDate'))
        row["Transaction ID"] = str(t.get('transactionId', ''))
        
        # 金額と通貨
        amount_obj = t.get('amount', {})
        row["Net amount"] = amount_obj.get('value', 0)
        row["Payout currency"] = str(amount_obj.get('currency', 'USD'))
        row["Gross transaction amount"] = amount_obj.get('value', 0) # デフォルト値
        row["Transaction currency"] = row["Payout currency"]
        
        # Payout(出金)情報の紐付け
        payout_id = str(t.get('payoutId', ''))
        if payout_id and payout_id != 'None':
            row["Payout ID"] = payout_id
            p_data = payout_dict.get(payout_id, {})
            if p_data:
                row["Payout date"] = format_date_for_report(p_data.get('payoutDate'))
                row["Payout status"] = str(p_data.get('payoutStatusDescription', 'Funds sent'))
                inst = p_data.get('payoutInstrument', {})
                instrument = str(inst.get('nickname', 'PAYONEER'))
                digits = str(inst.get('accountLastFourDigits', ''))
                row["Payout method"] = f"{instrument} *{digits}" if digits else instrument

        # 取引タイプのマッピング（Seller Hub仕様に合わせる）
        if t_type_raw == "SALE":
            row["Type"] = "Order"
        elif t_type_raw == "REFUND":
            row["Type"] = "Refund"
        elif t_type_raw == "NON_SALE_CHARGE":
            row["Type"] = "Other fee"
            row["Description"] = str(t.get('transactionMemo', ''))
        else:
            row["Type"] = t_type_raw

        # オーダーIDの取得と紐付け
        order_id = str(t.get('orderId', ''))
        if not order_id or order_id == 'None':
            for ref in t.get('references', []):
                if ref.get('referenceType') == 'ORDER_ID':
                    order_id = ref.get('referenceId')
                    break

        if order_id and order_id != 'None':
            row["Order number"] = order_id
            f_data = fulfill_dict.get(order_id, {})
            
            if f_data:
                row["Legacy order ID"] = str(f_data.get('legacyOrderId', '--'))
                
                # 住所・購入者情報
                buyer = f_data.get('buyer', {})
                row["Buyer username"] = str(buyer.get('username', '--'))
                
                reg_address = buyer.get('buyerRegistrationAddress', {})
                contact = reg_address.get('contactAddress', {})
                
                row["Buyer name"] = str(reg_address.get('fullName', '--'))
                row["Ship to city"] = str(contact.get('city', '--'))
                row["Ship to province/region/state"] = str(contact.get('stateOrProvince', '--'))
                row["Ship to zip"] = str(contact.get('postalCode', '--'))
                row["Ship to country"] = str(contact.get('countryCode', '--'))

                # 商品(SKU等)の情報
                line_items = f_data.get('lineItems', [])
                if line_items:
                    item = line_items[0]
                    row["Item ID"] = str(item.get('legacyItemId', '--'))
                    row["Item title"] = str(item.get('title', '--'))
                    row["Custom label"] = str(item.get('sku', '--'))
                    row["Quantity"] = str(item.get('quantity', '--'))
                    row["Item subtotal"] = item.get('lineItemCost', {}).get('value', '--')
                
                # 受注時の合計金額
                pricing = f_data.get('pricingSummary', {})
                total = pricing.get('total', {})
                if 'value' in total:
                    row["Gross transaction amount"] = str(total['value'])

        # Noneや空文字を '--' にクリーンアップ
        for k, v in row.items():
            if v is None or str(v) == 'None' or str(v) == 'nan' or str(v) == '':
                row[k] = "--"
                
        report_rows.append(row)

    # ② Payout（出金記録単体）の行を作成し、マイナス金額で追加
    for p in payouts:
        row = {col: "--" for col in columns}
        
        p_date = p.get('payoutDate')
        row["Transaction creation date"] = format_date_for_report(p_date)
        row["Type"] = "Payout"
        
        try:
            amt = float(p.get('amount', {}).get('value', 0))
            row["Net amount"] = f"-{amt}" if amt > 0 else str(amt)
            row["Gross transaction amount"] = row["Net amount"]
        except:
            pass

        row["Payout currency"] = str(p.get('amount', {}).get('currency', 'USD'))
        row["Transaction currency"] = row["Payout currency"]
        row["Payout date"] = format_date_for_report(p_date)
        row["Payout ID"] = str(p.get('payoutId', '--'))
        row["Payout status"] = str(p.get('payoutStatusDescription', 'Funds sent'))
        
        inst = p.get('payoutInstrument', {})
        instrument = str(inst.get('nickname', 'PAYONEER'))
        digits = str(inst.get('accountLastFourDigits', ''))
        row["Payout method"] = f"{instrument} *{digits}" if digits else instrument
        
        row["Description"] = str(p.get('payoutMemo', 'Scheduled payout.'))

        for k, v in row.items():
            if v is None or str(v) == 'None' or str(v) == 'nan' or str(v) == '':
                row[k] = "--"
                
        report_rows.append(row)

    # =========================================================
    # 4. 最終レポート(CSV)の出力とソート
    # =========================================================
    df_report = pd.DataFrame(report_rows, columns=columns)
    
    # 公式レポートに合わせて日付順(新しい順)にソート
    try:
        df_report['SortDate'] = pd.to_datetime(df_report['Transaction creation date'], errors='coerce')
        df_report = df_report.sort_values(by='SortDate', ascending=False).drop(columns=['SortDate'])
    except Exception as e:
        print(f"並び替えでエラーが発生しましたが、出力は続行します: {e}")

    csv_filename = f"{account}_TransactionReport_{start_date_str}_to_{end_date_str}.csv"
    output_path = os.path.join(TARGET_DIR, csv_filename)
    
    # B0M付きUTF-8で出力（Excel文字化け対策）
    df_report.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ すべての処理が完了しました！\nCSV出力先: 📁 {output_path}")


if __name__ == "__main__":
    account_list = ["BUZZ", "貴文", "貴文②", "川島", "谷川", "谷川②", "谷川③", "谷川④"]

    print("===================================================")
    print(" 📊 eBay Seller Hub トランザクションレポート 自動生成")
    print("===================================================")
    for i, account in enumerate(account_list, 1):
        print(f"  {i}: {account}")
    
    print("-" * 50)
    acc_input = input("対象のアカウント番号を入力してください > ")
    
    print("\n[注意] eBay APIの仕様上、開始日は「現在から2年以内」に設定してください。")
    start_input = input("開始日 (例: 2024-05-01) > ").strip()
    end_input = input("終了日 (例: 2026-04-20) > ").strip()

    if not start_input or not end_input:
        print("\n❌ 開始日と終了日の入力は必須です。処理を終了します。")
        sys.exit(1)

    try:
        idx = int(acc_input) - 1 
        if 0 <= idx < len(account_list):
            generate_direct_ebay_report(account_list[idx], start_input, end_input)
        else:
            print("\n❌ 無効な番号です。処理を終了します。")
    except ValueError:
        print("\n❌ 数字を入力してください。処理を終了します。")