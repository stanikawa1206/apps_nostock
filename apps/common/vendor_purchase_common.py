"""
複数の仕入サイト（メルカリ・ラクマ・Yahoo!フリマ等）で共通の

・Access「日常」への反映（ステータス／到着日／送り状番号・配送会社）
・ヤマト運輸／日本郵便の追跡（requestsのみ、Selenium不要）
・trx.vendor_message への保存

をまとめたモジュール。

サイト固有のスクレイピング（購入一覧の取得・個別取引ページの解析・メッセージ抽出）は
各サイトのスクリプト（apps/etc/mercari_purchase.py, rakuma_purchase.py,
yahoo_furima_purchase.py 等）側に置き、本モジュールは呼び出さない
（Selenium/Playwrightに依存しない）。
"""
import re
import ssl
from datetime import date

import pyodbc
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

from apps.common.utils import get_sql_server_connection  # noqa: F401  (呼び出し元の便宜のため re-export)

# ============================================================
# Access「日常」
# ============================================================
ACCESS_DB_PATH = r"Y:\ヤフオクDB.accdb"
ACCESS_TABLE = "日常"


def get_access_connection():
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={ACCESS_DB_PATH};"
    )
    return pyodbc.connect(conn_str)


# ============================================================
# ステータス判定（mercari_purchase.py の determine_status と同じ語彙・ロジック）
# ============================================================
# 到着済みを表す共通の生ステータス値。呼び出し元（各サイトのスクレイパー）は
# 自サイトの表示を "発送前" / "発送済み" / ARRIVED_STATUS のいずれかに正規化して渡す。
ARRIVED_STATUS = "☆出荷可能"
ARRIVED_STATUS_BY_SALES_CHANNEL = {
    "eBay": "☆出荷可能",
    "amazon": "☆到着済",
}

# 配送会社の追跡で「配達予定日」は取得できたが、まだ配達完了は確認できていない状態。
# 配送会社側で実際の配達完了を確認できるまでは、この状態のまま追跡を継続する
# （実配達確認済みという意味ではない）。
ESTIMATED_ARRIVAL_STATUS = "到着予定"

# eBayステータスの「進行度」。日常.eBayステータスは、この値が後退する方向へは
# 上書きしない（サイト側の表示がまだ追いついていないだけで、配送会社の追跡が
# 既に到着予定日／実到着日を確定させている場合に、古い状態へ巻き戻さないため）。
# ランクが定義されていない値（人手入力など）は保護対象外とし、従来通り上書きする。
# 【2026-09-01修正】「出荷済み」（フリマ側到着後、人がeBayへ発送した際に手入力する
# 最終状態）が未定義だったため .get(new_status, -1) で最下位(-1)扱いになり、
# フリマ側の再スクレイプ結果（☆出荷可能等）で無条件に上書き＝後退してしまう
# 実害（発送日が入っているのにeBayステータスが☆出荷可能へ戻る）が発生していた。
# 「出荷済み」はフリマ側進捗より後の段階のため、既存の最上位(3)より高いランクとする。
_STATUS_RANK = {
    "【購入済】": 0,
    "連絡あり": 0,
    "発送済み": 1,
    "調査中": 1,
    ESTIMATED_ARRIVAL_STATUS: 2,
    "☆出荷可能": 3,
    "☆到着済": 3,
    "出荷済み": 4,
}


def is_shipped_status(status: str) -> bool:
    """
    日常.eBayステータスが「発送済み」以上（発送済み／到着予定／到着済み系）かどうか。
    メッセージ本文の文言では判定せず、既存の購入スクレイピングが実際の取引ステータス・
    配送状況から書き込んだこの値を正として使う（無言発送でも取得できるため）。
    """
    return _STATUS_RANK.get(status, -1) >= _STATUS_RANK["発送済み"]


def determine_access_status(raw_status: str, has_seller_message: bool) -> str:
    """
    raw_status: "発送前" / "発送済み" / ARRIVED_STATUS のいずれか。
    発送前の場合のみ、出品者からのメッセージ有無で「連絡あり」/「【購入済】」に分岐する。
    """
    if raw_status != "発送前":
        return raw_status
    return "連絡あり" if has_seller_message else "【購入済】"


def write_ebay_status_if_advancing(access_cur, order_id: str, new_status: str) -> bool:
    """
    日常.eBayステータスを、状態が後退しない場合のみ上書きする
    （【購入済】/連絡あり(0) < 発送済み(1) < 到着予定(2) < ☆出荷可能/☆到着済(3)）。
    ランク不明の現在値（人手入力など）は保護対象外とし、これまで通り上書きする。
    戻り値: 該当行が存在し実際に更新できたか。
    """
    row = access_cur.execute(
        f"SELECT eBayステータス FROM {ACCESS_TABLE} WHERE 注文ID = ?", order_id
    ).fetchone()
    if row is None:
        return False

    new_rank = _STATUS_RANK.get(new_status, 999)
    current_rank = _STATUS_RANK.get(row[0], -1)
    if new_rank < current_rank:
        return False

    access_cur.execute(
        f"UPDATE {ACCESS_TABLE} SET eBayステータス = ? WHERE 注文ID = ?",
        new_status, order_id
    )
    return access_cur.rowcount > 0


FIXED_SOURCE = "電脳"
UNENTERED_STATUS = "未入力"
FIXED_CATEGORY = "その他"


def ensure_daily_record(access_conn, vendor_name: str, order_id: str, item_name, purchase_date, purchase_price) -> bool:
    """
    日常.注文ID = order_id のレコードが存在しない場合のみ、新規レコードを追加する。
    対象は主に「仕入れたが仕入入力を忘れている」「私用で購入したため、もともと
    仕入入力していない」の2パターン。既存レコードがある場合は何もしない
    （更新は update_daily_purchase_status / write_ebay_status_if_advancing /
    sync_carrier_tracking_to_daily など既存の処理がそのまま担当する）。

    新規追加する項目: 品目text=item_name, 注文ID=order_id, 仕入元="電脳"(固定),
    店舗=vendor_name, 仕入日=purchase_date, 仕入=purchase_price,
    eBayステータス="未入力"(固定)、区分="その他"(固定)。
    区分は「到着日入力」フォームの表示条件（区分<>"ama輸出"。NULLだと表示されない）
    を満たすために必須。本/HW等の判定は行わず、常に固定値とする。
    到着日等はここでは設定しない（後続の既存処理が注文ID一致で見つけて更新するため）。
    戻り値: 新規作成したか否か。
    """
    access_cur = access_conn.cursor()
    try:
        row = access_cur.execute(
            f"SELECT 注文ID FROM {ACCESS_TABLE} WHERE 注文ID = ?", order_id
        ).fetchone()
        if row is not None:
            return False

        access_cur.execute(
            f"""INSERT INTO {ACCESS_TABLE}
                ([品目text], [注文ID], [仕入元], [店舗], [仕入日], [仕入], [eBayステータス], [区分])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (item_name or "")[:100], order_id, FIXED_SOURCE, vendor_name,
            purchase_date, purchase_price, UNENTERED_STATUS, FIXED_CATEGORY
        )
        access_conn.commit()
        return True
    finally:
        access_cur.close()


def update_daily_purchase_status(access_conn, order_id: str, raw_status: str, has_seller_message: bool) -> bool:
    """
    日常.eBayステータスを注文ID一致で更新する。到着日はここでは更新しない
    （到着日は sync_carrier_tracking_to_daily がヤマト／日本郵便の追跡結果から更新する）。
    到着済み(ARRIVED_STATUS)の場合のみ、日常.販売（eBay/amazon）に応じて表示文言を分ける
    （販売が想定外の値の場合はARRIVED_STATUSのまま扱う）。
    状態が後退する更新（例: 到着予定→発送済み）は行わない
    （サイト側の表示が配送会社の追跡結果に追いついていないだけの場合があるため）。
    戻り値: 該当する日常行が存在し更新できたか。
    """
    status = determine_access_status(raw_status, has_seller_message)

    access_cur = access_conn.cursor()
    try:
        if status == ARRIVED_STATUS:
            sales_row = access_cur.execute(
                f"SELECT 販売 FROM {ACCESS_TABLE} WHERE 注文ID = ?", order_id
            ).fetchone()
            sales_channel = sales_row[0] if sales_row else None
            status = ARRIVED_STATUS_BY_SALES_CHANNEL.get(sales_channel, ARRIVED_STATUS)

        updated = write_ebay_status_if_advancing(access_cur, order_id, status)
        access_conn.commit()
    finally:
        access_cur.close()

    return updated


def update_daily_tracking_info(access_conn, order_id: str, tracking_number, carrier) -> bool:
    """
    送り状番号・配送会社を日常へ注文ID一致で保存する。
    tracking_numberが取得できなかった場合は何もしない（既存値を消さない）。
    戻り値: 該当する日常行が存在し更新できたか。
    """
    if not tracking_number:
        return False

    access_cur = access_conn.cursor()
    try:
        access_cur.execute(
            f"UPDATE {ACCESS_TABLE} SET tracking_number = ?, carrier = ? WHERE 注文ID = ?",
            tracking_number, carrier, order_id
        )
        updated = access_cur.rowcount > 0
        access_conn.commit()
    finally:
        access_cur.close()

    return updated


# ============================================================
# ヤマト運輸・日本郵便の追跡（requestsのみ、Selenium不要）
# ============================================================
YAMATO_TRACKING_URL = "https://toi.kuronekoyamato.co.jp/cgi-bin/tneko"
JAPANPOST_TRACKING_URL = "https://trackings.post.japanpost.jp/services/srv/search/direct"

YAMATO_DATE_RE = re.compile(r"(\d{1,2})[月/](\d{1,2})")
JAPANPOST_DATE_RE = re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})")


class _LegacyTLSAdapter(HTTPAdapter):
    """
    toi.kuronekoyamato.co.jp はTLS設定が古く、Pythonの既定のSSL設定では
    ハンドシェイクに失敗する（実機確認済み）。SECLEVEL=1に緩和して接続する。
    """
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


def _parse_yamato_date(text, reference_date):
    """
    ヤマトの日付表記（「08/20」「08月20日 09:40」など年を含まない）から date を組み立てる。
    年の表記が無いため、購入日以降で最も早く到達する年を採用する。
    """
    m = YAMATO_DATE_RE.search(text)
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    base_year = reference_date.year
    for year in (base_year, base_year + 1):
        try:
            d = date(year, month, day)
        except ValueError:
            continue
        if d >= reference_date.date():
            return d
    try:
        return date(base_year, month, day)
    except ValueError:
        return None


def fetch_yamato_tracking(tracking_number, reference_date):
    """
    ヤマト運輸の追跡ページ(POST、requestsのみ)から配達状況を取得する。
    戻り値: (arrival_date, confirmed)
      confirmed=True:  履歴に「配達完了」があり、その日付が実到着日
      confirmed=False: 未配達だが「お届け予定日時」が取得できた（到着予定日）
      どちらも取れなければ (None, False)
    """
    session = requests.Session()
    session.mount("https://", _LegacyTLSAdapter())
    resp = session.post(
        YAMATO_TRACKING_URL,
        data={"number01": tracking_number, "category": "1"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    detail = soup.select_one(".tracking-invoice-block-detail")
    if detail:
        for li in detail.select("li"):
            item_el = li.select_one(".item")
            date_el = li.select_one(".date")
            if item_el and date_el and "配達完了" in item_el.get_text():
                d = _parse_yamato_date(date_el.get_text(strip=True), reference_date)
                if d:
                    return d, True

    summary = soup.select_one(".tracking-invoice-block-summary")
    if summary:
        for li in summary.select("li"):
            label_el = li.select_one(".item")
            data_el = li.select_one(".data")
            if label_el and data_el and "お届け予定日時" in label_el.get_text():
                d = _parse_yamato_date(data_el.get_text(strip=True), reference_date)
                if d:
                    return d, False

    return None, False


def fetch_japanpost_tracking(tracking_number):
    """
    日本郵便の追跡ページ(GET、requestsのみ)から実到着日を取得する。
    「お届け先にお届け済み」の行があれば、その状態発生日を実到着日として返す。
    未配達時の到着予定日は現時点では取得しない。
    戻り値: (arrival_date, confirmed)。取れなければ (None, False)。
    """
    resp = requests.get(
        JAPANPOST_TRACKING_URL,
        params={"searchKind": "S002", "locale": "ja", "reqCodeNo1": tracking_number},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", summary="履歴情報")
    if not table:
        return None, False

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        status_text = cells[1].get_text(strip=True)
        if "お届け済み" in status_text:
            m = JAPANPOST_DATE_RE.search(cells[0].get_text(strip=True))
            if m:
                year, month, day = (int(x) for x in m.groups())
                try:
                    return date(year, month, day), True
                except ValueError:
                    return None, False

    return None, False


CARRIER_TRACKING_FETCHERS = {
    "ヤマト": lambda tracking_number, reference_date: fetch_yamato_tracking(tracking_number, reference_date),
    "日本郵便": lambda tracking_number, reference_date: fetch_japanpost_tracking(tracking_number),
}


def sync_carrier_tracking_to_daily(access_conn):
    """
    Access「日常」テーブルの tracking_number / carrier が判明している行のうち、
    まだ追跡を続ける必要がある行だけを対象に、ヤマト運輸／日本郵便の公式追跡ページ
    (requestsのみ)へ問い合わせ、結果を「日常.到着日」「日常.eBayステータス」へ反映する。
    vendor_name/仕入サイトを問わず日常のtracking_number/carrierだけを見るため、
    メルカリ・ラクマ・Yahoo!フリマいずれの購入にもそのまま使える。

    ステータスの反映ルール:
      - 配達完了を確認できた場合 → 従来通り販売(eBay/amazon)に応じた到着済みステータス
        （☆出荷可能／☆到着済）へ進める。
      - 配達完了はまだだが配達予定日を取得できた場合 → ESTIMATED_ARRIVAL_STATUS(到着予定)。
        実配達確認済みという意味ではないため、この状態のままでは対象から外れず、
        次回以降も追跡を継続する。
    いずれも write_ebay_status_if_advancing により、既存のeBayステータスより
    後退する更新は行わない（サイト側スクレイパーの表示が追いついていないだけで
    巻き戻さないようにするため）。

    confirmed フラグは持たず、既存の eBayステータス / 到着日 の組み合わせだけで
    「まだ追跡を続ける必要があるか」を判断する:
      - eBayステータスがまだ到着済み系（☆出荷可能／☆到着済）でない
        → まだ到着を検知していないので、到着日の値に関わらず必ず対象にする
          （ヤマトの到着予定日を過ぎても、実際に配達完了するまで追跡を継続するため）。
      - eBayステータスが既に到着済み系で、かつ到着日が既に今日より前
        → 実到着日が確定済みとみなして対象から外す。
    到着予定日（今日以降の日付）が入っているだけでは対象から外さない。

    呼び出し元は本関数を、日常.eBayステータスを更新する処理より先に呼ぶこと
    （対象判定に使うeBayステータスが「前回実行終了時点」の値になるようにするため）。
    """
    with access_conn.cursor() as cur:
        cur.execute(f"""
            SELECT 注文ID, tracking_number, carrier, 仕入日, 到着日
            FROM {ACCESS_TABLE}
            WHERE tracking_number IS NOT NULL
              AND carrier IS NOT NULL
              AND (
                    eBayステータス NOT IN ('☆出荷可能', '☆到着済')
                 OR eBayステータス IS NULL
                 OR 到着日 IS NULL
                 OR 到着日 >= ?
              )
        """, date.today())
        rows = cur.fetchall()

    checked = 0
    updated_actual = 0
    updated_estimated = 0
    for order_id, tracking_number, carrier, purchase_date, _current_arrival in rows:
        fetcher = CARRIER_TRACKING_FETCHERS.get(carrier)
        if fetcher is None:
            continue

        try:
            arrival_date, is_confirmed = fetcher(tracking_number, purchase_date or date.today())
        except Exception as e:
            print(f"WARN: 追跡取得失敗 注文ID={order_id} carrier={carrier}: {e}")
            continue

        checked += 1
        if arrival_date is None:
            continue

        with access_conn.cursor() as cur:
            cur.execute(
                f"UPDATE {ACCESS_TABLE} SET 到着日 = ? WHERE 注文ID = ?",
                arrival_date, order_id
            )
        access_conn.commit()

        if is_confirmed:
            # 実際に配達完了を確認できた場合のみ、従来通り販売(eBay/amazon)に応じた
            # 到着済みステータスへ進める。
            update_daily_purchase_status(access_conn, order_id, ARRIVED_STATUS, False)
            updated_actual += 1
        else:
            # 配達完了はまだ確認できていないが、配達予定日は取得できた状態。
            # 実配達確認済みという意味ではないため、到着済み系のステータスにはしない。
            access_cur = access_conn.cursor()
            try:
                write_ebay_status_if_advancing(access_cur, order_id, ESTIMATED_ARRIVAL_STATUS)
                access_conn.commit()
            finally:
                access_cur.close()
            updated_estimated += 1

    print(
        f"配送追跡({ACCESS_TABLE}): 対象{len(rows)}件中 問い合わせ{checked}件"
        f"（実到着日で更新{updated_actual}件, 予定日で更新{updated_estimated}件）"
    )


# ============================================================
# trx.vendor_message への保存
# ============================================================
SQL_UPSERT_VENDOR_MESSAGE = """
MERGE INTO trx.vendor_message WITH (HOLDLOCK) AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS src
    (vendor_name, vendor_item_id, message_no, sender_name, sender_type, message_datetime_text, message_body)
ON (tgt.vendor_name = src.vendor_name
    AND tgt.vendor_item_id = src.vendor_item_id
    AND tgt.message_no = src.message_no)
WHEN MATCHED THEN
    UPDATE SET
        sender_name           = src.sender_name,
        sender_type           = src.sender_type,
        message_datetime_text = src.message_datetime_text,
        message_body          = src.message_body,
        updated_at            = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (vendor_name, vendor_item_id, message_no, sender_name, sender_type, message_datetime_text, message_body, updated_at)
    VALUES (src.vendor_name, src.vendor_item_id, src.message_no, src.sender_name, src.sender_type, src.message_datetime_text, src.message_body, GETDATE());
"""


def save_vendor_messages(sql_conn, vendor_name: str, vendor_item_id: str, messages: list) -> None:
    """
    messages: [{"message_no": int, "sender_name": str, "sender_type": "出品者"|"購入者",
                "message_datetime_text": str, "message_body": str}, ...]
    message_datetime_text は各サイトの表示をそのまま保存する（Yahoo!フリマの「12時間前」
    のような相対表記も、無理に絶対日時へ変換せずそのまま保存する）。
    """
    with sql_conn.cursor() as cur:
        for msg in messages:
            cur.execute(
                SQL_UPSERT_VENDOR_MESSAGE,
                (
                    vendor_name,
                    vendor_item_id,
                    msg["message_no"],
                    msg["sender_name"],
                    msg["sender_type"],
                    msg["message_datetime_text"],
                    msg["message_body"],
                )
            )
    sql_conn.commit()
