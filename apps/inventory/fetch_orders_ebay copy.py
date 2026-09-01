# apps/inventory/fetch_orders_ebay.py

import csv
import os
from pathlib import Path
import sys
from datetime import datetime
from decimal import Decimal
import requests
import time
import pyodbc

from datetime import datetime, timezone, timedelta
from apps.common.utils import (
    get_sql_server_connection, send_mail,
    EBAY_FEE_RATE, DOMESTIC_SHIPPING_JPY, INTL_SHIPPING_JPY, DUTY_RATE,
)

# ==== VS Code ▶ 実行対応 ====
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ==== 既存資産 ====
from apps.adapters.ebay_api import get_access_token_new, send_buyer_thankyou_message
from apps.common.utils import USD_JPY_RATE, get_sql_server_connection

_JST = timezone(timedelta(hours=9))
_rate_cache: dict = {"value": None, "expires": 0.0}


# ==== GA（Authenticity Guarantee）センター住所定義 ====
# 将来的にセンターが増えた場合はここにエントリを追加する
GA_ADDRESSES = [
    {
        "postal_code": "143-0006",
        "city": "Ota-ku",
        "country": "JP",
    },
]


def _is_ga_address(addr: dict) -> bool:
    """配送先住所がGAセンターと一致するか判定する。"""
    postal  = (addr.get("postalCode")   or "").strip()
    city    = (addr.get("city")         or "").strip()
    country = (addr.get("countryCode")  or "").strip()
    for ga in GA_ADDRESSES:
        if postal == ga["postal_code"] and city == ga["city"] and country == ga["country"]:
            return True
    return False


# --------------------------------------------------
# 為替レート取得（5分キャッシュ）
# --------------------------------------------------
def _get_usd_jpy_rate() -> float:
    now_ts = time.time()
    if _rate_cache["value"] and now_ts < _rate_cache["expires"]:
        return _rate_cache["value"]
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/USDJPY%3DX",
            params={"interval": "1d", "range": "1d"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        rate = r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]
        _rate_cache["value"] = rate
        _rate_cache["expires"] = now_ts + 300  # 5分キャッシュ
        print(f"  USD/JPY: {rate}")
        return rate
    except Exception as e:
        print(f"  [WARN] 為替レート取得失敗: {e}")
        return _rate_cache["value"] or USD_JPY_RATE


# --------------------------------------------------
# vendor情報取得（仕入先名・仕入値）
# --------------------------------------------------
def _get_vendor_info(vendor_item_id: str):
    if not vendor_item_id:
        return None, None
    cn = get_sql_server_connection()
    try:
        cur = cn.cursor()
        cur.execute(
            "SELECT vendor_name, price FROM trx.vendor_item WHERE vendor_item_id = ?",
            vendor_item_id,
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)
    finally:
        cn.close()


# --------------------------------------------------
# Access「日常」テーブルへ1レコードINSERT
# --------------------------------------------------
def _insert_access_nichinichi(
    account: str,
    order_id: str,
    vendor_item_id: str,
    ebay_id: str,
    item_title,
    order_date_str: str,
    ship_by_date_str,
    is_ag: int,
    country,
    price_usd: float,
    rate: float,
    vendor_name,
    cost_jpy,
):
    # order_date → JST変換後、日付部分のみ（時刻不要）
    try:
        dt_utc = datetime.fromisoformat(order_date_str.replace("Z", "+00:00"))
        dt_jst = dt_utc.astimezone(_JST)
    except Exception:
        dt_jst = datetime.now(_JST)
    dt_date = datetime(dt_jst.year, dt_jst.month, dt_jst.day)  # 日付のみ

    # ship_by_date → JST の日付部分のみ（時刻は00:00:00）
    ship_by = None
    if ship_by_date_str:
        try:
            sb_utc = datetime.fromisoformat(ship_by_date_str.replace("Z", "+00:00"))
            sb_jst = sb_utc.astimezone(_JST)
            ship_by = datetime(sb_jst.year, sb_jst.month, sb_jst.day)
        except Exception:
            pass

    # 国：GA対象は"GA"、それ以外はcountryをそのまま
    kuni = "GA" if is_ag == 1 else (country or "")

    # 売価・手数料・送料計算
    sale_price = round(price_usd * rate)
    fee        = round(sale_price * EBAY_FEE_RATE)
    if kuni in ("GA", "JP"):
        shipping = DOMESTIC_SHIPPING_JPY
    elif kuni == "US":
        shipping = round(sale_price * DUTY_RATE) + INTL_SHIPPING_JPY
    else:
        shipping = INTL_SHIPPING_JPY

    try:
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb;"
        )
        cur  = conn.cursor()

        # 重複チェック（同一注文の2重登録防止）
        cur.execute(
            "SELECT COUNT(*) FROM 日常 WHERE amazon注文番号=? AND 注文ID=?",
            order_id or "", vendor_item_id or ""
        )
        if cur.fetchone()[0] > 0:
            print(f"    [Access] SKIP: already exists ({order_id} / {vendor_item_id})")
            return

        cur.execute("""
            INSERT INTO 日常 (
                品目text, 販売, 区分, 仕入元,
                店舗, 仕入日, 売上確定日,
                amazon注文番号, 注文ID, eBayID, アカウント,
                ShiipBy, 国,
                売価, 仕入, 手数料, 自己発送送料
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            item_title or "",
            "eBay",
            "eMS",
            "電脳",
            vendor_name or "",
            dt_date,        # 仕入日：日付のみ
            dt_date,        # 売上確定日：日付のみ
            order_id or "",
            vendor_item_id or "",
            ebay_id,        # None の場合は NULL として格納（or "" は使わない）
            account or "",
            ship_by,
            kuni,
            Decimal(str(sale_price)),
            Decimal(str(int(cost_jpy or 0))),
            Decimal(str(fee)),
            Decimal(str(shipping)),
        )
        conn.commit()
        print(f"    [Access] INSERT OK: sale={sale_price} fee={fee} ship={shipping}")
    except Exception as e:
        print(f"    [Access] INSERT ERROR: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_paid_orders(account: str, start: datetime, end: datetime):
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

    start_str = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str   = end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

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

    print("Mail sent OK")

# --------------------------------------------------
# DB最終受注日時取得
# --------------------------------------------------
def _get_max_order_date():
    """trx.ebay_orders の MAX(order_date) を返す。テーブルが空なら None。"""
    cn = get_sql_server_connection()
    try:
        cur = cn.cursor()
        cur.execute("SELECT MAX(order_date) FROM trx.ebay_orders")
        val = cur.fetchone()[0]
        if val is None:
            return None
        if isinstance(val, str):
            val = datetime.fromisoformat(val.replace("Z", "+00:00"))
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val
    finally:
        cn.close()


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
# shipByDate 待機ポーリング（最大5分 / 30回×10秒）
# --------------------------------------------------
def _wait_for_ship_by_date(account: str, order_id: str, ebay_id: str, max_retries: int = 30):
    """
    単一注文エンドポイントを 10 秒ごとにポーリングし、
    shipByDate が設定されるまで待機する（最大 5 分）。
    戻り値: (ship_by_date, country, is_ag)
    """
    token = get_access_token_new(account)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }

    ship_by_date = None
    country      = None
    is_ag        = 0

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(
                f"https://api.ebay.com/sell/fulfillment/v1/order/{order_id}",
                headers=headers,
                timeout=30,
            )
            if r.status_code != 200:
                print(f"  [wait {attempt}/{max_retries}] API error {r.status_code}")
                time.sleep(10)
                continue

            data = r.json()

            # country / is_ag を最新値で更新
            fi = data.get("fulfillmentStartInstructions", [])
            if fi:
                addr = (fi[0].get("shippingStep", {})
                           .get("shipTo", {})
                           .get("contactAddress") or {})
                country = addr.get("countryCode") or country
                is_ag   = 1 if _is_ga_address(addr) else 0

            # 対象 lineItem の shipByDate を確認
            for li in data.get("lineItems", []):
                if li.get("legacyItemId") == ebay_id:
                    sbd = li.get("lineItemFulfillmentInstructions", {}).get("shipByDate")
                    if sbd:
                        ship_by_date = sbd
                        print(f"  [wait {attempt}] shipByDate OK: {ship_by_date}")
                        return ship_by_date, country, is_ag
                    break

            print(f"  [wait {attempt}/{max_retries}] shipByDate 未設定 - 10秒後リトライ")

        except Exception as e:
            print(f"  [wait {attempt}] error: {e}")

        time.sleep(10)

    print(f"  [wait timeout] 5分経過 - NULL で登録")
    return ship_by_date, country, is_ag


# --------------------------------------------------
# メイン処理
# --------------------------------------------------
def run():
    import traceback
    print(f"START: {datetime.now():%Y-%m-%d %H:%M:%S}")

    while True:
        try:
            now = datetime.now(timezone.utc)

            # ---- 取得ウィンドウを決定 ----
            max_date = _get_max_order_date()

            if max_date is None:
                start_time = now - timedelta(days=720)
                print(f"  [初回] フル取得: {start_time:%Y-%m-%d} ～ {now:%Y-%m-%d %H:%M:%S} UTC")
            else:
                start_time = max_date - timedelta(minutes=5)
                print(f"  [差分] {start_time:%Y-%m-%d %H:%M:%S} ～ {now:%Y-%m-%d %H:%M:%S} UTC")

            cn = get_sql_server_connection()
            cur = cn.cursor()

            rate = _get_usd_jpy_rate()

            for account in load_accounts():
                print(f"[ACCOUNT] {account}")
                orders = fetch_paid_orders(account, start_time, now)

                if not orders:
                    continue

                for order in orders:
                    order_id   = order.get("orderId")
                    order_date = order.get("creationDate")
                    buyer      = order.get("buyer", {}).get("username")

                    # 初回APIレスポンスの country / is_ag（メール送信用）
                    fi = order.get("fulfillmentStartInstructions", [])
                    initial_country = None
                    initial_is_ag   = 0
                    if fi:
                        addr0 = (fi[0].get("shippingStep", {})
                                     .get("shipTo", {})
                                     .get("contactAddress")
                                 or fi[0].get("shippingStep", {})
                                         .get("shipTo", {})
                                         .get("address")
                                 or {})
                        initial_country = addr0.get("countryCode")
                        initial_is_ag   = 1 if _is_ga_address(addr0) else 0

                    thankyou_sent = False  # 注文単位で1回だけ送信

                    for item in order.get("lineItems", []):
                        ebay_id        = item.get("legacyItemId")
                        vendor_item_id = item.get("sku")
                        qty            = item.get("quantity")
                        price_usd      = Decimal(item.get("lineItemCost", {}).get("value", "0"))
                        item_title     = item.get("title")
                        initial_ship_by = item.get("lineItemFulfillmentInstructions", {}).get("shipByDate")

                        # 重複チェック
                        cur.execute("""
                            SELECT 1 FROM trx.ebay_orders
                            WHERE order_id = ? AND ebay_id = ?
                        """, order_id, ebay_id)
                        if cur.fetchone():
                            continue

                        # ① メール送信（最優先・即時）
                        send_new_order_mail(
                            account=account,
                            order_id=order_id,
                            buyer=buyer,
                            vendor_item_id=vendor_item_id,
                            ebay_id=ebay_id,
                            price_usd=float(price_usd),
                            country=initial_country,
                        )

                        # ② バイヤーへサンキューメッセージ（注文単位で1回のみ）
                        if not thankyou_sent:
                            send_buyer_thankyou_message(
                                account=account,
                                order_id=order_id,
                                buyer_username=buyer,
                                ebay_id=ebay_id,
                            )
                            thankyou_sent = True

                        # ④ shipByDate 待機
                        # 初回レスポンスで取得済みならそのまま進む
                        # 未設定なら最大5分ポーリング（country / is_ag も更新）
                        if initial_ship_by:
                            ship_by_date = initial_ship_by
                            country      = initial_country
                            is_ag        = initial_is_ag
                        else:
                            print(f"  shipByDate 未設定 - 最大5分待機")
                            ship_by_date, country, is_ag = _wait_for_ship_by_date(
                                account, order_id, ebay_id
                            )
                            # country が取得できなかった場合は初回値を保持
                            if country is None:
                                country = initial_country
                            # is_ag が後から GA 確定した場合も反映
                            if is_ag == 0 and initial_is_ag == 1:
                                is_ag = initial_is_ag

                        print(f"  NEW ORDER: {order_id} / {vendor_item_id} "
                              f"/ is_ag={is_ag} / ship_by={ship_by_date}")

                        # ⑤ SQL Server INSERT
                        cur.execute("""
                            INSERT INTO trx.ebay_orders (
                                account, order_id, buyer, ebay_id, vendor_item_id,
                                qty, order_date, price_usd, country, is_ag,
                                item_title, ship_by_date
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            account, order_id, buyer, ebay_id, vendor_item_id,
                            qty, order_date, float(price_usd), country, is_ag,
                            item_title, ship_by_date,
                        )
                        cn.commit()

                        # ⑥ Access「日常」テーブルへ書き込み
                        vendor_name, cost_jpy = _get_vendor_info(vendor_item_id)
                        _insert_access_nichinichi(
                            account=account,
                            order_id=order_id,
                            vendor_item_id=vendor_item_id,
                            ebay_id=ebay_id,
                            item_title=item_title,
                            order_date_str=order_date,
                            ship_by_date_str=ship_by_date,
                            is_ag=is_ag,
                            country=country,
                            price_usd=float(price_usd),
                            rate=rate,
                            vendor_name=vendor_name,
                            cost_jpy=cost_jpy,
                        )

            cur.close()
            cn.close()

        except Exception as e:
            print(f"[ERROR] {e}")
            traceback.print_exc()

        time.sleep(10)


if __name__ == "__main__":
    run()

