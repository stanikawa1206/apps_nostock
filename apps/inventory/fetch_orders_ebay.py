# apps/inventory/fetch_orders_ebay.py

import csv
import json
import logging
import os
from logging.handlers import RotatingFileHandler
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

# ==== Access DB ====
_ACCESS_CONN_STR = r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=Y:\ヤフオクDB.accdb;"
_JST = timezone(timedelta(hours=9))
_rate_cache: dict = {"value": None, "expires": 0.0}


# --------------------------------------------------
# ロギング設定
# --------------------------------------------------
# 過去に print() が Windows コンソールへの書き込み(WriteConsoleW)でブロックし、
# メインループ全体が停止する障害が発生したため、コンソール出力には依存しない。
# ログはファイルのみに出力する（安全側・最小構成）。
_LOG_DIR = _PROJECT_ROOT / "logs"
_LOG_FILE = _LOG_DIR / "fetch_orders_ebay.log"
_HEARTBEAT_FILE = _LOG_DIR / "fetch_orders_ebay.heartbeat.json"


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("fetch_orders_ebay")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            str(_LOG_FILE), maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
    except Exception:
        # ファイルログの初期化自体に失敗しても、コンソールへはフォールバックしない
        # （コンソール書き込みブロックの再発を避けるため）。ログなしで処理を継続する。
        logger.addHandler(logging.NullHandler())
    return logger


log = _setup_logging()


def _update_heartbeat(**kwargs) -> None:
    """稼働監視用の最終時刻を記録する（last_loop_start / last_api_access /
    last_db_commit / last_success）。書き込みに失敗しても処理は継続する。"""
    try:
        data = {}
        if _HEARTBEAT_FILE.exists():
            try:
                data = json.loads(_HEARTBEAT_FILE.read_text(encoding="utf-8"))
            except Exception:
                data = {}
        data.update(kwargs)
        tmp = _HEARTBEAT_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(_HEARTBEAT_FILE)
    except Exception:
        log.warning("heartbeat書き込みに失敗しました", exc_info=True)

# ==== LINE Messaging API トークン ====
_LINE_TOKEN_TAKAFUMI = "fsrkRPiEQ5Lyb/vV2NSbNeI9CeT5nkjIFvUrh5H2k+Ubi06UaZob4kRlh5ox/+q+Mt7ahfkb4BX/0PSYAaisFT/qSeCsHHjl1p095GRmJKOT7K8u0O+AEr8VO9oV4ShIEX2Yd5RMbICIpkJzvwc2kgdB04t89/1O/w1cDnyilFU="
_LINE_TOKEN_BUZZ     = "wrbm40jAHmim1bRL+xvJz0ZApOxzBAYYbbK4G//NXvGrsOxv3vmk5GloFQviuWmCJDsDnwMUciQqs4BcIiTINB5AAvVXtCqfv5JrARfgqtQS5c8qIl/XNH0fOQ6N9A2m5e6MPlWEmbr5JRWLnDe4DQdB04t89/1O/w1cDnyilFU="
_LINE_TOKEN_DEFAULT  = "Shdz82NrcFpbUirZ57RcFWDr8cHaP84QN4LOTiXGwnm0nQHpChPzOJ3J/G6H1Y/IDllje+wiDPSQ0diuYIN5Iau04MwMov89AIg9YSRdCGyQ3ByW7JL/plDYSEe4NutFqM07npe1gxSF+cYocFOduQdB04t89/1O/w1cDnyilFU="


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
        log.info(f"USD/JPY: {rate}")
        return rate
    except Exception as e:
        log.warning(f"為替レート取得失敗: {e}", exc_info=True)
        return _rate_cache["value"] or USD_JPY_RATE


# --------------------------------------------------
# vendor情報取得（仕入先名・仕入値）
# --------------------------------------------------
_DEADLOCK_SQLSTATE = "40001"
_VENDOR_INFO_MAX_ATTEMPTS = 3
_VENDOR_INFO_BACKOFF_SECONDS = (0.5, 1, 2)


def _get_vendor_info(vendor_item_id: str, order_id: str = None, ebay_id: str = None):
    """
    trx.vendor_item から仕入先名・仕入値を取得する。

    SQL Serverのデッドロック(SQLSTATE 40001)が発生した場合のみ、
    短い待機を挟んで最大3回まで再試行する。デッドロック以外のエラーは
    再試行せずそのまま呼び出し元へ伝播させる。
    order_id / ebay_id はログに文脈を残すためだけの任意引数（呼び出し側で
    分かっていれば渡す。クエリ自体には使わない）。
    """
    if not vendor_item_id:
        return None, None

    for attempt in range(1, _VENDOR_INFO_MAX_ATTEMPTS + 1):
        cn = get_sql_server_connection()
        try:
            cur = cn.cursor()
            cur.execute(
                "SELECT vendor_name, price FROM trx.vendor_item WHERE vendor_item_id = ?",
                vendor_item_id,
            )
            row = cur.fetchone()
            return (row[0], row[1]) if row else (None, None)
        except pyodbc.Error as e:
            sqlstate = e.args[0] if e.args else ""
            is_deadlock = (sqlstate == _DEADLOCK_SQLSTATE)
            is_last_attempt = (attempt >= _VENDOR_INFO_MAX_ATTEMPTS)

            if is_deadlock and not is_last_attempt:
                wait_sec = _VENDOR_INFO_BACKOFF_SECONDS[attempt - 1]
                log.warning(
                    f"[_get_vendor_info] デッドロック検知・再試行 "
                    f"(attempt {attempt}/{_VENDOR_INFO_MAX_ATTEMPTS}) "
                    f"order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id} "
                    f"- {wait_sec}秒後リトライ: {e}"
                )
                time.sleep(wait_sec)
                continue

            if is_deadlock:
                log.error(
                    f"[_get_vendor_info] デッドロック再試行上限({_VENDOR_INFO_MAX_ATTEMPTS}回)到達 "
                    f"order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id}: {e}",
                    exc_info=True,
                )
            raise
        finally:
            cn.close()

    return None, None


# --------------------------------------------------
# Access「日常」テーブル: 存在確認・INSERT
# --------------------------------------------------
def _access_order_exists(order_id: str, vendor_item_id: str) -> bool:
    """
    Access「日常」テーブルに対象注文（order_id + vendor_item_id(SKU)）が
    すでに存在するかを確認する。_insert_access_nichinichi()の重複防止と、
    run()側の「SQL Serverには存在するがAccessには存在しない」検出の両方で使う。
    """
    conn = None
    try:
        conn = pyodbc.connect(_ACCESS_CONN_STR)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM 日常 WHERE amazon注文番号=? AND 注文ID=?",
            order_id or "", vendor_item_id or ""
        )
        return cur.fetchone()[0] > 0
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


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
) -> bool:
    """
    Access「日常」テーブルへ1レコードINSERTする。

    戻り値: 呼び出し完了後にAccess側へレコードが存在していればTrue
    （すでに存在していてスキップした場合を含む）、INSERTに失敗した場合はFalse。
    冪等: 同じ注文（order_id + vendor_item_id）で複数回呼び出しても、
    実際にINSERTされるのは1回だけ。
    """
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

    log.info(
        f"[Access] Access登録開始: order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id}"
    )

    # 重複チェック（同一注文の2重登録防止。冪等性の担保はここで行う）
    if _access_order_exists(order_id, vendor_item_id):
        log.info(
            f"[Access] SKIP: already exists order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id}"
        )
        return True

    conn = None
    try:
        conn = pyodbc.connect(_ACCESS_CONN_STR)
        cur  = conn.cursor()

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
        log.info(
            f"[Access] Access登録成功: order_id={order_id} ebay_id={ebay_id} "
            f"sku={vendor_item_id} sale={sale_price} fee={fee} ship={shipping}"
        )
        return True
    except Exception as e:
        log.error(
            f"[Access] Access登録失敗: order_id={order_id} ebay_id={ebay_id} "
            f"sku={vendor_item_id} error={e}",
            exc_info=True,
        )
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def fetch_paid_orders(account: str, start: datetime, end: datetime):
    token = get_access_token_new(account)
    if not token:
        log.error("access token 取得失敗")
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
            log.error(f"API error: {r.status_code} {r.text}")
            break

        orders = r.json().get("orders", [])
        if not orders:
            break

        all_orders.extend(orders)

        log.info(f"取得件数: {len(all_orders)}")

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

    log.info("Mail sent OK")


# --------------------------------------------------
# LINE通知
# --------------------------------------------------
def _get_line_token(account: str) -> str:
    if account == "貴文②":
        return _LINE_TOKEN_TAKAFUMI
    elif account == "BUZZ②":
        return _LINE_TOKEN_BUZZ
    return _LINE_TOKEN_DEFAULT


def send_line_broadcast(token: str, text: str = None, image_url: str = None):
    """LINE Broadcast APIでテキスト・画像を送信する。"""
    url = "https://api.line.me/v2/bot/message/broadcast"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    messages = []
    if text:
        messages.append({"type": "text", "text": text[:5000]})
    if image_url:
        # LINE画像メッセージにはHTTPS公開URLが必要
        messages.append({
            "type": "image",
            "originalContentUrl": image_url,
            "previewImageUrl": image_url,
        })
    if not messages:
        return
    r = requests.post(url, headers=headers, json={"messages": messages}, timeout=10)
    if r.status_code == 200:
        log.info("LINE broadcast OK")
    else:
        log.warning(f"LINE broadcast failed: {r.status_code} {r.text}")


def send_line_new_order(
    account: str,
    vendor_item_id: str,
    ebay_id: str,
    price_usd: float,
    country: str,
    buyer: str,
):
    """受注時にLINE Broadcast通知を送信する。失敗しても処理を止めない。"""
    try:
        token = _get_line_token(account)

        # DBから画像URL取得（send_new_order_mail と同じソース）
        image_url = None
        try:
            cn = get_sql_server_connection()
            cur = cn.cursor()
            cur.execute(
                "SELECT image_url1, vendor_name FROM trx.vendor_item WHERE vendor_item_id = ?",
                vendor_item_id,
            )
            row = cur.fetchone()
            cur.close()
            cn.close()
            if row:
                image_url = row[0]
                vendor_name = row[1]
            else:
                image_url = None
                vendor_name = None
        except Exception as e:
            log.warning(f"LINE: 画像URL取得失敗: {e}", exc_info=True)
 
        ebay_url = f"https://www.ebay.com/itm/{ebay_id}" if ebay_id else ""

        if vendor_item_id:
            if vendor_item_id.startswith("m"):
                # 通常メルカリ
                mercari_url = (
                    f"https://jp.mercari.com/item/{vendor_item_id}"
                    f"?openExternalBrowser=1"
                )
            else:
                # メルカリShops
                mercari_url = (
                    f"https://jp.mercari.com/shops/product/{vendor_item_id}"
                    f"?openExternalBrowser=1"
                )
        else:
            mercari_url = ""

        text = (
            f"🟢【新規受注】{account}\n"
            f"\n"
            f"メルカリ:\n"
            f"{mercari_url}\n"
            f"\n"
            f"eBay:\n"
            f"{ebay_url}\n"
            f"\n"
            f"💰 ${price_usd}\n"
            f"\n"
            f"Buyer: {buyer}\n"
            f"Country: {country}"
        )

        send_line_broadcast(token, text=text, image_url=image_url)


    except Exception as e:
        log.warning(f"LINE通知エラー: {e}", exc_info=True)


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
                log.warning(f"[wait {attempt}/{max_retries}] API error {r.status_code}")
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
                        log.info(f"[wait {attempt}] shipByDate OK: {ship_by_date}")
                        return ship_by_date, country, is_ag
                    break

            log.info(f"[wait {attempt}/{max_retries}] shipByDate 未設定 - 10秒後リトライ")

        except Exception as e:
            log.warning(f"[wait {attempt}] error: {e}", exc_info=True)

        time.sleep(10)

    log.warning("[wait timeout] 5分経過 - NULL で登録")
    return ship_by_date, country, is_ag


# --------------------------------------------------
# メイン処理
# --------------------------------------------------
def run():
    log.info(f"START: {datetime.now():%Y-%m-%d %H:%M:%S}")

    while True:
        _update_heartbeat(last_loop_start=datetime.now(timezone.utc).isoformat())
        try:
            now = datetime.now(timezone.utc)

            # ---- 取得ウィンドウを決定 ----
            max_date = _get_max_order_date()

            if max_date is None:
                start_time = now - timedelta(days=720)
                log.info(f"[初回] フル取得: {start_time:%Y-%m-%d} ～ {now:%Y-%m-%d %H:%M:%S} UTC")
            else:
                start_time = max_date - timedelta(minutes=5)
                log.info(f"[差分] {start_time:%Y-%m-%d %H:%M:%S} ～ {now:%Y-%m-%d %H:%M:%S} UTC")

            cn = get_sql_server_connection()
            cur = cn.cursor()

            rate = _get_usd_jpy_rate()

            for account in load_accounts():
                log.info(f"[ACCOUNT] {account}")
                orders = fetch_paid_orders(account, start_time, now)
                _update_heartbeat(last_api_access=datetime.now(timezone.utc).isoformat())

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

                        # 重複チェック（trx.ebay_ordersに存在するか）
                        cur.execute("""
                            SELECT account, order_date, price_usd, country, is_ag,
                                   item_title, ship_by_date
                            FROM trx.ebay_orders
                            WHERE order_id = ? AND ebay_id = ?
                        """, order_id, ebay_id)
                        existing = cur.fetchone()

                        if existing:
                            # trx.ebay_ordersには存在する。
                            # Accessにも存在するなら本当に処理済み → 何もしない。
                            # Accessに存在しないなら、以前の実行がAccess登録の手前で
                            # 中断した状態（今回のバグの再発パターン）なので、
                            # メール・LINE・サンキュー・trx.ebay_ordersへの再INSERTは
                            # 一切行わず、Access登録だけを再試行する。
                            if _access_order_exists(order_id, vendor_item_id):
                                continue

                            log.warning(
                                f"[RECONCILE] SQL Serverには存在するがAccessに存在しない注文を検出: "
                                f"order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id}"
                            )

                            (existing_account, existing_order_date, existing_price_usd,
                             existing_country, existing_is_ag, existing_item_title,
                             existing_ship_by) = existing

                            try:
                                vendor_name, cost_jpy = _get_vendor_info(
                                    vendor_item_id, order_id=order_id, ebay_id=ebay_id
                                )
                            except Exception as e:
                                log.error(
                                    f"[RECONCILE] vendor情報取得に失敗したため今回は見送ります"
                                    f"（次回ループで再試行）: order_id={order_id} ebay_id={ebay_id} "
                                    f"sku={vendor_item_id} error={e}",
                                    exc_info=True,
                                )
                                continue

                            reconcile_ok = _insert_access_nichinichi(
                                account=existing_account,
                                order_id=order_id,
                                vendor_item_id=vendor_item_id,
                                ebay_id=ebay_id,
                                item_title=existing_item_title,
                                order_date_str=(
                                    existing_order_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                                    if existing_order_date else None
                                ),
                                ship_by_date_str=(
                                    existing_ship_by.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                                    if existing_ship_by else None
                                ),
                                is_ag=int(existing_is_ag) if existing_is_ag is not None else 0,
                                country=existing_country,
                                price_usd=float(existing_price_usd) if existing_price_usd is not None else 0.0,
                                rate=rate,
                                vendor_name=vendor_name,
                                cost_jpy=cost_jpy,
                            )
                            log.info(
                                f"[RECONCILE] Accessのみ再登録した結果: "
                                f"order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id} "
                                f"result={'成功' if reconcile_ok else '失敗'}"
                            )
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

                        # ① LINE通知（追加 / メール送信に続いて即時送信）
                        send_line_new_order(
                            account=account,
                            vendor_item_id=vendor_item_id,
                            ebay_id=ebay_id,
                            price_usd=float(price_usd),
                            country=initial_country,
                            buyer=buyer,
                        )

                        # ② バイヤーへサンキューメッセージ（注文単位で1回のみ）
                        if not thankyou_sent:
                            thankyou_result = send_buyer_thankyou_message(
                                account=account,
                                order_id=order_id,
                                buyer_username=buyer,
                                ebay_id=ebay_id,
                                logger=log,
                            )
                            if thankyou_result.get("success"):
                                log.info(
                                    f"[ThankYou] 呼び出し結果: 成功 order_id={order_id} "
                                    f"ebay_id={ebay_id} buyer={buyer}"
                                )
                            else:
                                log.warning(
                                    f"[ThankYou] 呼び出し結果: 失敗 order_id={order_id} "
                                    f"ebay_id={ebay_id} buyer={buyer} detail={thankyou_result}"
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
                            log.info("shipByDate 未設定 - 最大5分待機")
                            ship_by_date, country, is_ag = _wait_for_ship_by_date(
                                account, order_id, ebay_id
                            )
                            # country が取得できなかった場合は初回値を保持
                            if country is None:
                                country = initial_country
                            # is_ag が後から GA 確定した場合も反映
                            if is_ag == 0 and initial_is_ag == 1:
                                is_ag = initial_is_ag

                        log.info(f"NEW ORDER: {order_id} / {vendor_item_id} "
                                 f"/ is_ag={is_ag} / ship_by={ship_by_date}")

                        # ⑤ vendor情報取得（trx.ebay_orders INSERTより前に取得する。
                        # デッドロック再試行の上限に達して失敗しても、trx.ebay_ordersへの
                        # 登録自体は止めない。vendor_nameがNoneのままAccess登録を試み、
                        # それも失敗すれば次回ループの[RECONCILE]経路が拾う）
                        try:
                            vendor_name, cost_jpy = _get_vendor_info(
                                vendor_item_id, order_id=order_id, ebay_id=ebay_id
                            )
                            vendor_info_ok = True
                        except Exception as e:
                            log.error(
                                f"[VENDOR] vendor情報取得に失敗しました（再試行上限到達）: "
                                f"order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id} error={e}",
                                exc_info=True,
                            )
                            vendor_name, cost_jpy = None, None
                            vendor_info_ok = False

                        # ⑥ SQL Server INSERT
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
                        _update_heartbeat(last_db_commit=datetime.now(timezone.utc).isoformat())
                        log.info(
                            f"[SQL] SQL Server登録成功: order_id={order_id} ebay_id={ebay_id} "
                            f"sku={vendor_item_id}"
                        )

                        # ⑦ Access「日常」テーブルへ書き込み
                        # vendor情報取得に失敗した場合は今回はAccess登録を見送る
                        # （trx.ebay_ordersには登録済みなので、次回ループの[RECONCILE]経路が
                        # Access登録だけを改めて再試行する）
                        if vendor_info_ok:
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
                        else:
                            log.warning(
                                f"[Access] vendor情報未取得のためAccess登録を見送りました。"
                                f"次回ループで再試行されます: order_id={order_id} ebay_id={ebay_id} "
                                f"sku={vendor_item_id}"
                            )

            cur.close()
            cn.close()
            _update_heartbeat(last_success=datetime.now(timezone.utc).isoformat())

        except Exception as e:
            log.error(f"run()ループでエラー: {e}", exc_info=True)

        time.sleep(10)


if __name__ == "__main__":
    run()

