# apps/inventory/fetch_orders_ebay.py

import csv
import os
from pathlib import Path
import sys
from datetime import datetime
from decimal import Decimal
import requests

from datetime import datetime, timezone, timedelta
from apps.common.utils import get_sql_server_connection, send_mail

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

def send_new_order_mail(
    account: str,
    order_id: str,
    buyer: str,
    vendor_item_id: str,
    price_usd: float,
    country: str,
):
    cn = get_sql_server_connection()
    cur = cn.cursor()

    # -------------------------
    # vendor_item取得
    # -------------------------
    cur.execute("""
        SELECT vendor_name, image_url1
        FROM trx.vendor_item
        WHERE vendor_item_id = ?
    """, vendor_item_id)

    row = cur.fetchone()
    cur.close()
    cn.close()

    if not row:
        vendor_name = None
        image_url = None
    else:
        vendor_name, image_url = row

    # -------------------------
    # URL生成
    # -------------------------
    if vendor_name == "メルカリshops":
        item_url = f"https://mercari-shops.com/products/{vendor_item_id}"
    else:
        item_url = f"https://jp.mercari.com/item/{vendor_item_id}"

    # -------------------------
    # メール本文
    # -------------------------
    body = f"""
【NEW ORDER】

Account : {account}
OrderID : {order_id}
Buyer   : {buyer}
Price   : ${price_usd}
Country : {country}

▼商品ページ
{item_url}

▼画像
{image_url}
"""

    subject = f"[eBay] NEW ORDER {order_id}"

    # -------------------------
    # 送信
    # -------------------------
    send_mail(
        subject=subject,
        body=body
    )

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

    cn = get_sql_server_connection()
    cur = cn.cursor()

    for account in load_accounts():
        print(f"[ACCOUNT] {account}")
        orders = fetch_paid_orders(account)

        if not orders:
            continue

        for order in orders:
            order_id = order.get("orderId")
            order_date = order.get("creationDate")
            status = order.get("orderFulfillmentStatus")
            buyer = order.get("buyer", {}).get("username")

            # --- COUNTRY ---
            f_instructions = order.get("fulfillmentStartInstructions", [])
            country = None

            if f_instructions:
                shipping_step = f_instructions[0].get("shippingStep", {})
                ship_to = shipping_step.get("shipTo", {})
                addr = ship_to.get("contactAddress") or ship_to.get("address") or {}
                country = addr.get("countryCode")

            for item in order.get("lineItems", []):
                ebay_id = item.get("legacyItemId")
                vendor_item_id = item.get("sku")
                qty = item.get("quantity")
                price_usd = Decimal(item.get("lineItemCost", {}).get("value", "0"))

                # ---------------------------
                # 既存チェック（超重要）
                # ---------------------------
                cur.execute("""
                    SELECT 1
                    FROM trx.ebay_orders
                    WHERE order_id = ? AND ebay_id = ?
                """, order_id, ebay_id)

                if cur.fetchone():
                    continue  # 既存 → スキップ


                # 新規注文
                print(f"NEW ORDER: {order_id} / {vendor_item_id}")

                send_new_order_mail(
                    account=account,
                    order_id=order_id,
                    buyer=buyer,
                    vendor_item_id=vendor_item_id,
                    price_usd=float(price_usd),
                    country=country
                )

                # ---------------------------
                # INSERT
                # ---------------------------
                cur.execute("""
                    INSERT INTO trx.ebay_orders (
                        account,
                        order_id,
                        buyer,
                        status,
                        ebay_id,
                        sku,
                        qty,
                        order_date,
                        price_usd,
                        country
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    account,
                    order_id,
                    buyer,
                    status,
                    ebay_id,
                    sku,
                    qty,
                    order_date,
                    float(price_usd),
                    country
                )

                print(f"NEW ORDER: {order_id} / {sku}")

    cn.commit()
    cur.close()
    cn.close()

if __name__ == "__main__":
    run()
