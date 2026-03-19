# apps/inventory/fetch_orders_ebay.py

import csv
import os
from pathlib import Path
import sys
from datetime import datetime
from decimal import Decimal
import requests

from datetime import datetime, timezone, timedelta

# ==== VS Code ▶ 実行対応 ====
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ==== 既存資産 ====
from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import USD_JPY_RATE, get_sql_server_connection


def fetch_paid_orders(account: str):
    token = get_access_token_new(account)
    if not token:
        print("  ❌ access token 取得失敗")
        return []

    url = "https://api.ebay.com/sell/fulfillment/v1/order"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Content-Language": "en-US",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }

    all_orders = []
    offset = 0
    limit = 200
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=365*2 - 1)  # 

    start_str = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "filter": f"creationdate:[{start_str}..{end_str}]"
        }

        r = requests.get(url, headers=headers, params=params, timeout=30)

        if r.status_code != 200:
            print(f"  ❌ API error: {r.status_code}")
            print(r.text)
            break

        orders = r.json().get("orders", [])
        if not orders:
            break

        all_orders.extend(orders)

        print(f"  取得件数: {len(all_orders)}")

        # 次ページへ
        offset += limit

    return all_orders


# --------------------------------------------------
# アカウント取得
# --------------------------------------------------
def load_accounts():
    cn = get_sql_server_connection()
    cur = cn.cursor()
    cur.execute("""
        SELECT account
        FROM mst.ebay_accounts
        WHERE is_excluded = 0
            AND account = '川島'
        ORDER BY account
    """)
    accounts = [row[0] for row in cur.fetchall()]
    cur.close()
    cn.close()
    return accounts


# --------------------------------------------------
# メイン処理
# --------------------------------------------------
def run():
    print(f"START: {datetime.now():%Y-%m-%d %H:%M:%S}")
    
    download_dir = Path(os.path.expanduser("~")) / "Downloads"
    csv_file = download_dir / f"ebay_orders_{datetime.now():%Y%m%d_%H%M%S}.csv"
    
    # ★修正ポイント：BUYER をヘッダーに追加して列ズレを防止
    headers_csv = [
        "ACCOUNT", "ORDER_ID", "BUYER", "STATUS",
        "ebayID", "SKU", "QTY",
        "ORDER_DATE", "SHIP_BY",
        "PRICE_USD", "PRICE_JPY", "COUNTRY"
    ]    
    all_rows = []

    for account in load_accounts():
        print(f"[ACCOUNT] {account}")
        orders = fetch_paid_orders(account)

        if not orders:
            continue

        for order in orders:
            order_id = order.get("orderId")
            order_date = order.get("creationDate")
            # Buyerの取得
            buyer = order.get("buyer", {}).get("username")

            # --- COUNTRY の取得 ---
            f_instructions = order.get("fulfillmentStartInstructions", [])
            country = None
            if f_instructions:
                # 階層を深く掘り下げる
                ship_to = f_instructions[0].get("shippingStep", {}).get("shipTo", {})
                # address 直下、または contactAddress を確認
                addr = ship_to.get("contactAddress") or ship_to.get("address") or {}
                country = addr.get("countryCode")
                if f_instructions:
                    ship_by = (
                        f_instructions[0]
                        .get("shippingStep", {})
                        .get("shipByDate")
                    )

            for item in order.get("lineItems", []):
                ebay_id = item.get("legacyItemId")
                sku = item.get("sku")
                qty = item.get("quantity")
               
 
                price_usd = Decimal(item.get("lineItemCost", {}).get("value", "0"))
                price_jpy = int(price_usd * Decimal(USD_JPY_RATE))

                # カラムの順番を headers_csv と完全に一致させる
                row = [
                    account, order_id, buyer, ebay_id, sku, qty, 
                    order_date, ship_by, float(price_usd), price_jpy, country
                ]
                all_rows.append(row)

    # --- CSV書き出し ---
    if all_rows:
        with open(csv_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(headers_csv)
            writer.writerows(all_rows)
        print(f"✅ CSV保存完了: {csv_file} ({len(all_rows)} rows)")

if __name__ == "__main__":
    run()
