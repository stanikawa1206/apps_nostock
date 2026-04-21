import sys
import os
import time
import json
import pandas as pd
from datetime import datetime, timezone
import requests

# ---------------------------------------------------------
# 基本設定
# ---------------------------------------------------------
sys.path.append(r"\\MOUSE\apps_nostock")
from apps.adapters.ebay_api import get_access_token_new

# データ保存先
BASE_DIR = r"Y:\ebay\data\API_data"
TARGET_DIR = r"Y:\ebay\data\Report"


def format_date_for_report(date_str):
    if not date_str or str(date_str) == 'nan':
        return "--"
    try:
        dt = pd.to_datetime(date_str)
        return dt.strftime('%b %-d, %Y')
    except:
        return str(date_str)


def fetch_api_data_all_pages(url: str, headers: dict, params: dict, data_key: str, api_name: str) -> list:
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
            if 'next' in data and data['next']:
                current_url = data['next']
                current_params = None 
                page_count += 1
                time.sleep(0.5)
            else:
                current_url = None
        return all_items
    except Exception as e:
        print(f"  ❌ {api_name} APIエラー: {e}")
        return []


def generate_direct_ebay_report(account: str, start_date_str: str, end_date_str: str):
    print(f"\n▶ [{account}] のアクセストークンを取得中...")
    access_token = get_access_token_new(account)
    if not access_token:
        print(f"❌ アクセストークン取得失敗")
        return

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(TARGET_DIR, exist_ok=True)

    try:
        start_dt = pd.to_datetime(start_date_str).strftime('%Y-%m-%dT00:00:00.000Z')
        end_dt = pd.to_datetime(end_date_str).strftime('%Y-%m-%dT23:59:59.000Z')
    except:
        print(f"❌ 日付形式エラー")
        return

    # API取得
    transactions = fetch_api_data_all_pages("https://apiz.ebay.com/sell/finances/v1/transaction", headers, {"filter": f"transactionDate:[{start_dt}..{end_dt}]", "limit": 100}, "transactions", "Transaction")
    if not transactions: return
    payouts = fetch_api_data_all_pages("https://apiz.ebay.com/sell/finances/v1/payout", headers, {"filter": f"payoutDate:[{start_dt}..{end_dt}]", "limit": 100}, "payouts", "Payout")
    orders = fetch_api_data_all_pages("https://api.ebay.com/sell/fulfillment/v1/order", headers, {"filter": f"creationdate:[{start_dt}..{end_dt}]", "limit": 100}, "orders", "Fulfillment")

    # JSON保存
    file_suffix = f"{start_date_str}_to_{end_date_str}.json"
    for data, name in [(transactions, "Transaction"), (payouts, "Payout"), (orders, "Fulfillment")]:
        with open(os.path.join(BASE_DIR, f"{account}_{name}_{file_suffix}"), 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    # 辞書化
    payout_dict = {p.get('payoutId'): p for p in payouts}
    fulfill_dict = {f.get('orderId'): f for f in orders}

    # カラム定義（最後に Account name を追加）
    columns = ["Transaction creation date", "Type", "Order number", "Legacy order ID", "Buyer username", "Buyer name", "Ship to city", "Ship to province/region/state", "Ship to zip", "Ship to country", "Net amount", "Payout currency", "Payout date", "Payout ID", "Payout method", "Payout status", "Reason for hold", "Item ID", "Transaction ID", "Item title", "Custom label", "Quantity", "Item subtotal", "Shipping and handling", "Seller collected tax", "eBay collected tax", "Final Value Fee - fixed", "Final Value Fee - variable", "Regulatory operating fee", "Very high \"item not as described\" fee", "Below standard performance fee", "International fee", "Charity donation", "Deposit processing fee", "Gross transaction amount", "Transaction currency", "Exchange rate", "Reference ID", "Description", "Account name"]

    report_rows = []

    # ① Transactionループ
    for t in transactions:
        row = {col: "--" for col in columns}
        row["Account name"] = account  # 🌟アカウント名セット
        
        t_type_raw = str(t.get('transactionType', ''))
        row["Transaction creation date"] = format_date_for_report(t.get('transactionDate'))
        row["Transaction ID"] = str(t.get('transactionId', ''))
        
        amt_val = float(t.get('amount', {}).get('value', 0))
        if str(t.get('bookingEntry', '')).upper() == 'DEBIT': amt_val = -abs(amt_val)
        
        row["Net amount"] = amt_val
        row["Gross transaction amount"] = amt_val
        row["Payout currency"] = str(t.get('amount', {}).get('currency', 'USD'))
        row["Transaction currency"] = row["Payout currency"]

        if t_type_raw == "SALE": row["Type"] = "Order"
        elif t_type_raw == "REFUND": row["Type"] = "Refund"
        elif t_type_raw == "NON_SALE_CHARGE":
            row["Type"] = "Other fee"
            row["Description"] = str(t.get('transactionMemo', ''))
        else:
            row["Type"] = t_type_raw.capitalize()
            row["Description"] = str(t.get('transactionMemo', ''))

        # Payout紐付け
        payout_id = str(t.get('payoutId', ''))
        if payout_id and payout_id != 'None':
            row["Payout ID"] = payout_id
            p_data = payout_dict.get(payout_id, {})
            if p_data:
                row["Payout date"] = format_date_for_report(p_data.get('payoutDate'))
                row["Payout status"] = str(p_data.get('payoutStatusDescription', 'Funds sent'))
                inst = p_data.get('payoutInstrument', {})
                row["Payout method"] = f"{inst.get('nickname', 'PAYONEER')} *{inst.get('accountLastFourDigits', '')}"

        # オーダー紐付け
        order_id = t.get('orderId')
        if not order_id:
            for ref in t.get('references', []):
                if ref.get('referenceType') == 'ORDER_ID':
                    order_id = ref.get('referenceId'); break
        if order_id:
            row["Order number"] = order_id
            f_data = fulfill_dict.get(order_id, {})
            if f_data:
                row["Legacy order ID"] = str(f_data.get('legacyOrderId', '--'))
                row["Buyer username"] = str(f_data.get('buyer', {}).get('username', '--'))
                row["Buyer name"] = str(f_data.get('buyer', {}).get('buyerRegistrationAddress', {}).get('fullName', '--'))
                items = f_data.get('lineItems', [])
                if items:
                    row["Item ID"] = str(items[0].get('legacyItemId', '--'))
                    row["Item title"] = str(items[0].get('title', '--'))
                    row["Custom label"] = str(items[0].get('sku', '--'))
                if row["Type"] == "Order":
                    row["Gross transaction amount"] = f_data.get('pricingSummary', {}).get('total', {}).get('value', amt_val)

        report_rows.append(row)

    # ② Payout単体ループ
    for p in payouts:
        row = {col: "--" for col in columns}
        row["Account name"] = account  # 🌟アカウント名セット
        p_date = p.get('payoutDate')
        row["Transaction creation date"] = format_date_for_report(p_date)
        row["Type"] = "Payout"
        amt = -abs(float(p.get('amount', {}).get('value', 0)))
        row["Net amount"] = amt
        row["Gross transaction amount"] = amt
        row["Payout currency"] = str(p.get('amount', {}).get('currency', 'USD'))
        row["Transaction currency"] = row["Payout currency"]
        row["Payout date"] = format_date_for_report(p_date)
        row["Payout ID"] = str(p.get('payoutId', '--'))
        row["Payout status"] = str(p.get('payoutStatusDescription', 'Funds sent'))
        inst = p.get('payoutInstrument', {})
        row["Payout method"] = f"{inst.get('nickname', 'PAYONEER')} *{inst.get('accountLastFourDigits', '')}"
        row["Description"] = str(p.get('payoutMemo', 'Scheduled payout.'))
        report_rows.append(row)

    df_report = pd.DataFrame(report_rows, columns=columns)
    df_report['SortDate'] = pd.to_datetime(df_report['Transaction creation date'], errors='coerce')
    df_report = df_report.sort_values(by='SortDate', ascending=False).drop(columns=['SortDate'])
    
    csv_filename = f"{account}_TransactionReport_{start_date_str}_to_{end_date_str}.csv"
    output_path = os.path.join(TARGET_DIR, csv_filename)
    df_report.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n✅ レポート出力完了: {output_path}")

if __name__ == "__main__":
    account_list = ["BUZZ", "貴文", "貴文②", "川島", "谷川", "谷川②", "谷川③", "谷川④"]
    acc_input = input("アカウント番号を入力 > ")
    start_input = input("開始日 (例: 2024-05-01) > ").strip()
    end_input = input("終了日 (例: 2026-04-20) > ").strip()
    if start_input and end_input:
        generate_direct_ebay_report(account_list[int(acc_input)-1], start_input, end_input)