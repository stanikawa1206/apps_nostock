# apps/inventory/fetch_orders_ebay.py

import csv
import os
from pathlib import Path
import sys
from datetime import datetime
from decimal import Decimal
import requests
import time

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
    limit = 50
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=10) 

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
    ebay_id: str, 
    price_usd: float,
    country: str,
):
    from apps.common.utils import get_sql_server_connection, send_mail
    import os
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    cn = get_sql_server_connection()
    cur = cn.cursor()

    cur.execute("""
        SELECT vendor_name, image_url1
        FROM trx.vendor_item
        WHERE vendor_item_id = ?
    """, vendor_item_id)

    row = cur.fetchone()
    cur.close()
    cn.close()

    vendor_name, image_url = row if row else (None, None)

    # -------------------------
    # URL
    # -------------------------
    if vendor_name == "メルカリshops":
        mercari_url = f"https://mercari-shops.com/products/{vendor_item_id}"
    else:
        mercari_url = f"https://jp.mercari.com/item/{vendor_item_id}"

    ebay_url = f"https://www.ebay.com/itm/{ebay_id}"

    # -------------------------
    # 件名
    # -------------------------
    subject = f"🟢【新規受注】{account}"

    # -------------------------
    # HTML本文
    # -------------------------
    body = f"""
<html>
<body style="font-family: Arial;">


    <!-- ①画像 -->
    <div>
        <img src="{image_url}" width="250">
    </div>

    <br>

    <!-- ③価格 -->
    <div style="font-size:18px;">
        💰 <b>${price_usd}</b>
    </div>

    <br>

    <!-- メルカリURL（ラベルなし） -->
    <div>
        <a href="{mercari_url}">{mercari_url}</a>
    </div>

    <br>

    <!-- eBay URL（ラベルなし） -->
    <div>
        <a href="{ebay_url}">{ebay_url}</a>
    </div>

    <br>

    <!-- その他 -->
    <div style="color:gray;">
        Buyer: {buyer}<br>
        Country: {country}<br>
        SKU: {vendor_item_id}
    </div>

</body>
</html>
"""

    # -------------------------
    # 送信（HTML）
    # -------------------------
    main_email = os.getenv("GMAIL_SENDER_EMAIL")
    main_password = os.getenv("GMAIL_APP_PASSWORD")
    
    # デフォルト設定（自分から自分へ）
    sender_email = main_email
    password = main_password
    receiver_email = main_email
    cc_email = None # 通常時はCCなし

    # ★ accountが「貴文②」のときだけ特別ルール
    if account == "貴文②":
        sender_email = os.getenv("TAKAFUMI2_EMAIL")
        password = os.getenv("TAKAFUMI2_PASSWORD")
        receiver_email = sender_email  # 貴文②本人へ
        cc_email = main_email          # 自分をCCに入れる        

    msg = MIMEMultipart("alternative")
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    if cc_email:
        msg["Cc"] = cc_email
    msg.attach(MIMEText(body, "html", "utf-8"))

    to_addrs = [receiver_email]
    if cc_email:
        to_addrs.append(cc_email)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg)

    print("📧 HTMLメール送信完了")

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

    while True:
        try:
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

                        # 既存チェック
                        cur.execute("""
                            SELECT 1
                            FROM trx.ebay_orders
                            WHERE order_id = ? AND ebay_id = ?
                        """, order_id, ebay_id)

                        if cur.fetchone():
                            continue

                        # 新規注文
                        print(f"NEW ORDER: {order_id} / {vendor_item_id}")

                        send_new_order_mail(
                            account=account,
                            order_id=order_id,
                            buyer=buyer,
                            vendor_item_id=vendor_item_id,
                            ebay_id=ebay_id,
                            price_usd=float(price_usd),
                            country=country
                        )

                        # INSERT
                        cur.execute("""
                            INSERT INTO trx.ebay_orders (
                                account,
                                order_id,
                                buyer,
                                status,
                                ebay_id,
                                vendor_item_id,
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
                            vendor_item_id,
                            qty,
                            order_date,
                            float(price_usd),
                            country
                        )

                        cn.commit()  # ←ここ重要（1件ごと）

            cur.close()
            cn.close()

        except Exception as e:
            print(f"❌ ERROR: {e}")

        # -------------------------
        # ループ間隔（超重要）
        # -------------------------
        time.sleep(10)  # ←10秒


if __name__ == "__main__":
    run()

