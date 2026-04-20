import sys
import os
import pandas as pd
import json
from datetime import datetime

def format_date(date_str):
    """ '2026-04-14T03:41:26.000Z' のようなAPIの日付を 'Mar 31, 2026' 形式に変換 """
    if pd.isna(date_str) or not date_str:
        return "--"
    try:
        # タイムゾーンを考慮してパース
        dt = pd.to_datetime(date_str)
        # 'May 3, 2025' のようなフォーマットに変換
        return dt.strftime('%b %-d, %Y')
    except:
        return str(date_str)

def generate_ebay_standard_report(account: str):
    """
    3つのAPIデータを読み込み、eBay標準のトランザクションレポート形式に変換して出力します。
    """
    base_dir = r"\\MOUSE\apps_nostock\apps\snapshot\eBay\1YearData"
    target_dir = r"\\MOUSE\apps_nostock\apps\snapshot\eBay\Report"
    os.makedirs(target_dir, exist_ok=True)

    # 読み込みファイルのパス (今回はCSVではなく、すべての要素が含まれるJSONやDataFrameを扱う想定)
    # ※既にCSV化された _AllFields.csv などを読み込んでも良いですが、扱いやすいJSONをパースする前提のコード例です
    # （ここでは取得済みの1Year CSVを読み込みます）
    trans_csv = os.path.join(base_dir, f"{account}_Transaction_1Year.csv")
    payout_csv = os.path.join(base_dir, f"{account}_Payout_1Year.csv")
    fulfill_csv = os.path.join(base_dir, f"{account}_Fulfillment_1Year.csv")

    try:
        df_t = pd.read_csv(trans_csv)
        df_p = pd.read_csv(payout_csv)
        df_f = pd.read_csv(fulfill_csv)
    except Exception as e:
        print(f"❌ ファイルの読み込みに失敗しました。1年分のデータを取得済みか確認してください。\n{e}")
        return

    print(f"▶ {account} のデータを eBay Seller Hub 形式に変換中...")

    # Payout と Fulfillment を辞書化して検索を高速化
    payout_dict = df_p.set_index('payoutId').to_dict('index') if not df_p.empty else {}
    fulfill_dict = df_f.set_index('orderId').to_dict('index') if not df_f.empty else {}

    # 出力用リスト
    report_rows = []

    # eBayレポートのヘッダー定義
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

    for _, t in df_t.iterrows():
        # --- 基本変数の抽出 ---
        row = {col: "--" for col in columns} # 初期値は全て "--"
        
        t_type_raw = str(t.get('transactionType', ''))
        row["Transaction creation date"] = format_date(t.get('transactionDate'))
        row["Transaction ID"] = str(t.get('transactionId', ''))
        row["Net amount"] = t.get('amount.value', 0)
        row["Payout currency"] = str(t.get('amount.currency', 'USD'))
        row["Gross transaction amount"] = t.get('amount.value', 0) # デフォルトは純額
        row["Transaction currency"] = row["Payout currency"]
        
        payout_id = str(t.get('payoutId', ''))
        if payout_id != 'nan' and payout_id:
            row["Payout ID"] = payout_id
            p_data = payout_dict.get(payout_id, {})
            if p_data:
                row["Payout date"] = format_date(p_data.get('payoutDate'))
                row["Payout status"] = str(p_data.get('payoutStatusDescription', 'Funds sent'))
                # Payoneerの末尾4桁を追加
                instrument = str(p_data.get('payoutInstrument.nickname', 'PAYONEER'))
                digits = str(p_data.get('payoutInstrument.accountLastFourDigits', ''))
                row["Payout method"] = f"{instrument} *{digits}" if digits and digits != 'nan' else instrument

        # Transaction Type の変換 (API -> レポート表記)
        if t_type_raw == "SALE":
            row["Type"] = "Order"
        elif t_type_raw == "REFUND":
            row["Type"] = "Refund"
        elif t_type_raw == "NON_SALE_CHARGE":
            row["Type"] = "Other fee"
            row["Description"] = str(t.get('transactionMemo', ''))
        else:
            row["Type"] = t_type_raw

        # --- オーダー情報の紐付け ---
        order_id = str(t.get('orderId', ''))
        if order_id == 'nan' or not order_id:
            # referencesから探す（広告費など）
            ref_str = str(t.get('references', ''))
            if 'ORDER_ID' in ref_str:
                try:
                    refs = eval(ref_str)
                    for r in refs:
                        if r.get('referenceType') == 'ORDER_ID':
                            order_id = r.get('referenceId')
                            break
                except:
                    pass

        if order_id and order_id != 'nan':
            row["Order number"] = order_id
            f_data = fulfill_dict.get(order_id, {})
            
            if f_data:
                row["Legacy order ID"] = str(f_data.get('legacyOrderId', '--'))
                row["Buyer username"] = str(f_data.get('buyer.username', '--'))
                
                # 住所情報の取得
                row["Buyer name"] = str(f_data.get('buyer.buyerRegistrationAddress.fullName', '--'))
                row["Ship to city"] = str(f_data.get('buyer.buyerRegistrationAddress.contactAddress.city', '--'))
                row["Ship to province/region/state"] = str(f_data.get('buyer.buyerRegistrationAddress.contactAddress.stateOrProvince', '--'))
                row["Ship to zip"] = str(f_data.get('buyer.buyerRegistrationAddress.contactAddress.postalCode', '--'))
                row["Ship to country"] = str(f_data.get('buyer.buyerRegistrationAddress.contactAddress.countryCode', '--'))

                # LineItems (商品情報) の取得
                items_str = str(f_data.get('lineItems', ''))
                if items_str and items_str != 'nan':
                    try:
                        items = eval(items_str)
                        if isinstance(items, list) and len(items) > 0:
                            item = items[0]
                            row["Item ID"] = str(item.get('legacyItemId', '--'))
                            row["Item title"] = str(item.get('title', '--'))
                            row["Custom label"] = str(item.get('sku', '--'))
                            row["Quantity"] = str(item.get('quantity', '--'))
                            row["Item subtotal"] = item.get('lineItemCost', {}).get('value', '--')
                    except:
                        pass
                
                # 金額ベース（売上総額など）
                gross = str(f_data.get('pricingSummary.total.value', '--'))
                if gross != 'nan':
                    row["Gross transaction amount"] = gross

        # nanを "--" に変換するクリーンアップ
        for k, v in row.items():
            if str(v) == 'nan' or v is None:
                row[k] = "--"

        report_rows.append(row)

    # DataFrame化してCSV出力
    df_report = pd.DataFrame(report_rows, columns=columns)
    
    # 実際のeBayレポートのように、冒頭にダミーの注意書き行を入れたい場合は追加処理を行いますが、
    # ここではデータベースやExcelで扱いやすいようにヘッダーからの純粋なCSVとして出力します。
    output_path = os.path.join(target_dir, f"{account}_TransactionReport_Formatted.csv")
    df_report.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ {account} のレポート出力が完了しました！\n📁 {output_path}")

if __name__ == "__main__":
    account_list = [
        "BUZZ", "貴文", "貴文②", "川島", "谷川", "谷川②", "谷川③", "谷川④"
    ]

    print("=== eBay Seller Hub 形式レポート生成ツール ===")
    for i, account in enumerate(account_list, 1):
        print(f"  {i}: {account}")
    
    print("-" * 30)
    user_input = input("変換したいアカウントの番号を入力 (例: 1) > ")

    try:
        selected_index = int(user_input) - 1 
        if 0 <= selected_index < len(account_list):
            selected_account = account_list[selected_index]
            generate_ebay_standard_report(selected_account)
        else:
            print("❌ 無効な番号です。")
    except ValueError:
        print("❌ 数字を入力してください。")