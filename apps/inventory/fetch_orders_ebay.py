# apps/inventory/fetch_orders_ebay.py

from pathlib import Path
import sys
from datetime import datetime
from decimal import Decimal
import requests

# ==== VS Code ▶ 実行対応 ====
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ==== 既存資産 ====
from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import USD_JPY_RATE, get_sql_server_connection


# --------------------------------------------------
# PAID 注文取得（このファイル専用・外部に出さない）
# --------------------------------------------------
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
    params = {
        "filter": "orderFulfillmentStatus:{IN_PROGRESS}",
        "limit": 50,
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        print(f"  ❌ API error: {r.status_code}")
        print(r.text)
        return []

    return r.json().get("orders", [])

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

    for account in load_accounts():
        print("=" * 80)
        print(f"[ACCOUNT] {account}")

        orders = fetch_paid_orders(account)

        if not orders:
            print("  No PAID orders.")
            continue

        for order in orders:
            order_date = order.get("creationDate")
            ship_to = order.get("shippingAddress", {})
            country = ship_to.get("countryCode")

            for item in order.get("lineItems", []):
                ebay_id = item.get("legacyItemId")
                sku = item.get("sku")
                qty = item.get("quantity")

                price_usd = Decimal(
                    item["lineItemCost"]["value"]
                )
                price_jpy = int(price_usd * Decimal(USD_JPY_RATE))

                ship_by = (
                    item.get("shippingDetail", {})
                    .get("shipByDate")
                )

                print(
                    f"ebayID={ebay_id} | "
                    f"SKU={sku} | "
                    f"QTY={qty} | "
                    f"ORDER_DATE={order_date} | "
                    f"SHIP_BY={ship_by} | "
                    f"PRICE_USD={price_usd} | "
                    f"PRICE_JPY={price_jpy} | "
                    f"COUNTRY={country}"
                )

    print("END")


if __name__ == "__main__":
    run()
