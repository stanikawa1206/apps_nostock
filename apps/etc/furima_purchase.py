"""
フリマ購入管理（メルカリ・PayPayフリマ・ラクマ）の統合スクリプト。

【2026-08-29 統合】以下9ファイルの機械的統合（ロジックはそのまま・同名衝突のみ
mercari_/paypay_/rakuma_ 接頭辞で区別）による新規作成。統合時点では旧ファイルを
削除せず、呼び出し元切替・回帰確認が完了するまで旧ファイルが単独で動作し続ける
想定だったが、【2026-09-01確認】Access「ヤフオク.accdb」Form_到着日入力のVBA
（Shell()呼び出し）を実機で確認したところ、既に
`python.exe D:\apps_nostock\apps\etc\furima_purchase.py` を直接起動する形に
切り替わっており、本ファイルが唯一の本番経路になっている。旧9ファイル・
furima_purchase_runner.py（下記「実行オーケストレーション」節参照）は
現在どこからも呼ばれていない想定だが、削除はまだ行っていない。

  - apps/etc/mercari_purchase.py            → 本ファイルの「メルカリ固有」節
  - apps/etc/yahoo_furima_purchase.py       → 本ファイルの「PayPayフリマ固有」節
  - apps/etc/rakuma_purchase.py             → 本ファイルの「ラクマ固有」節
  - apps/common/vendor_purchase_common.py   → 本ファイルの「共通処理」節（既存の共通名のまま）
  - apps/common/vendor_message_reply.py     → 本ファイルの「共通処理」節（既存の共通名のまま）
  - apps/common/mercari_send_reply.py       → send_mercari_reply()
  - apps/common/paypay_send_reply.py        → send_paypay_reply()
  - apps/common/rakuma_send_reply.py        → send_rakuma_reply()
  - apps/etc/furima_purchase_runner.py      → 「実行オーケストレーション」節（main()）

方針（今回の機械的統合段階でのルール）:
  - 既存ロジックは可能な限りそのまま移動した（判定条件・SQL・待機時間・リトライ回数等、
    数値・文言も含めて一切変更していない）。
  - 3サイトそれぞれで同名だった関数・定数（例: ensure_chrome_debugger, VENDOR_NAME,
    get_raw_status, get_tracking_info, get_messages, send_chat_message, main 等）は
    mercari_ / paypay_ / rakuma_ 接頭辞で区別した。Chrome起動管理のように内容が
    実質同一なものも含め、今回は「同名なら分ける」を機械的に適用しており、
    まだ統合（1つにまとめる）はしていない。
  - 既に3サイト共通だった処理（旧vendor_purchase_common.py / vendor_message_reply.py）は
    元の関数名・定数名のまま移動した。
  - サイト固有の処理内容そのもの（DOM/API/セレクタ・判定文言等）は一切変更していない。
  - Chrome管理・入力処理・CDP捕捉ループ等の重複の共通化、message_no/expected_countの
    設計変更、PayPay/ラクマへのMercari同等機能の追加（trx.vendor_purchase・到着日補完・
    未登録検知・driver.quit()等）は、本段階では行っていない（機械的統合→回帰確認が
    完了してから、1つずつ回帰確認しながら実施する）。
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import json
import os
import re
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

import psutil
import pyodbc
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from apps.common.utils import get_sql_server_connection


# ============================================================================
# ============================================================================
# 共通処理（旧 apps/common/vendor_purchase_common.py）
# 3サイトが元から共通で使っていた処理のため、関数名・定数名は変更していない。
# ============================================================================
# ============================================================================

# ------------------------------------------------------------
# Access「日常」
# ------------------------------------------------------------
ACCESS_DB_PATH = r"Y:\ヤフオクDB.accdb"
ACCESS_TABLE = "日常"


def get_access_connection():
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={ACCESS_DB_PATH};"
    )
    return pyodbc.connect(conn_str)


# ------------------------------------------------------------
# ステータス判定（mercari_get_raw_status 等と同じ語彙・ロジック）
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# ヤマト運輸・日本郵便の追跡（requestsのみ、Selenium不要）
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# trx.vendor_message への保存
# ------------------------------------------------------------
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


# ============================================================================
# ============================================================================
# 共通処理（旧 apps/common/vendor_message_reply.py）
# /messages画面用の共通処理。関数名・定数名は変更していない。
# ============================================================================
# ============================================================================

# 画面表示・対象抽出の対象とするvendor_name（既存のtrx.vendor_message表記に合わせる）。
TARGET_VENDOR_NAMES = ("メルカリ", "ＰａｙＰａｙフリマ", "ラクマ")

# 各サイトの個別取引ページURL。既存の購入スクレイパーが実際に使用/取得している
# URL形式をそのまま流用する（新規に推測しない）。
#   メルカリ: f"https://jp.mercari.com/transaction/{iid}"
#   ラクマ: https://fril.jp/transaction?item_id={id} （実機確認済み）
#   ＰａｙＰａｙフリマ: 取引ページ(/item/{id}/trade/buyer)は「ご指定のページが
#     見つかりませんでした」になることを実機確認済みのため、商品ページ
#     (https://paypayfleamarket.yahoo.co.jp/item/{id}) を使う。
TRANSACTION_URL_BUILDERS = {
    "メルカリ": lambda oid: f"https://jp.mercari.com/transaction/{oid}",
    "ラクマ": lambda oid: f"https://fril.jp/transaction?item_id={oid}",
    "ＰａｙＰａｙフリマ": lambda oid: f"https://paypayfleamarket.yahoo.co.jp/item/{oid}",
}


# Access バックエンド（実データ本体。Y:\ヤフオクDB.accdb）に対して、フロントエンド
# ヤフオク.accdb の保存済みクエリ「到着日入力」と同じ抽出条件を直接実行する。
# フロントエンドの pyodbc 直結（旧get_access_frontend_connection）は、人がAccessで
# フロントエンドを開いている間ロック競合(-3810)を起こすことが実機で複数回確認された。
# 日常・ASINはいずれもフロントエンド側では単なるリンクテーブルで、実体はこのバックエンドに
# あるため、バックエンドに直結すればフロントエンドの開閉状態に影響されない
# （フロントエンドを開いたままバックエンドへ直結できることも実機確認済み）。
#
# 「到着日入力」クエリ自体（WHERE条件）はバックエンド側にはオブジェクトとして
# 保存されていないため、実機調査済みの条件をここに複製している:
#     仕入日 >= 2024/7/1 AND 区分 <> "ama輸出"
#     AND 返品依頼番号 IS NULL AND 発送日 IS NULL AND SKU IS NULL
#     AND 入金日 IS NULL AND 出品日 IS NULL
# 今後Access側でこの条件が変更された場合はここも追従して直す必要がある
# （以前は「クエリの実行結果を正として条件を再実装しない」方針だったが、
# ロック競合の解消を優先しバックエンド直結・条件複製の方針に変更した）。
#
# 商品名はフロントエンドの「到着日入力」と同じ導出方法（ASIN.品目があればそれを、
# 無ければ日常.品目text を使う）をそのまま再現する。
FETCH_ACTIVE_ORDERS_SQL = """
    SELECT 日常.注文ID,
           IIf(IsNull(ASIN.品目), 日常.品目text, ASIN.品目) AS 商品名,
           日常.eBayステータス,
           日常.店舗
    FROM 日常 LEFT JOIN ASIN ON 日常.ASIN = ASIN.ASIN
    WHERE 日常.仕入日 >= #7/1/2024#
      AND 日常.区分 <> 'ama輸出'
      AND 日常.返品依頼番号 IS NULL
      AND 日常.発送日 IS NULL
      AND 日常.SKU IS NULL
      AND 日常.入金日 IS NULL
      AND 日常.出品日 IS NULL
"""


def fetch_active_orders(access_conn) -> dict:
    """
    バックエンド（Y:\\ヤフオクDB.accdb）へ直結し、「到着日入力」と同じ条件で
    現在の抽出結果（注文ID・商品名・eBayステータス・店舗）を返す。

    店舗列は、trx.vendor_messageに保存されている(vendor_name, vendor_item_id)全129件と
    突き合わせて実機検証済み（一致126件・不一致0件・NULL0件。残り3件は日常に
    レコード自体が無いだけで矛盾ではない）。trx.vendor_messageに一度も履歴の無い
    「無言発送」の取引でも、この店舗列だけでvendor_nameを特定できる
    （trx.vendor_messageへダミー行を作らず、新規テーブルも増やさずに済む）。

    注文IDが日常テーブル上で複数行になっている場合（実データで実例あり。1回の
    購入で複数ASINを別行として記録している等）、それらをまとめて同一取引として扱う。
    eBayステータス・店舗は注文単位の情報で、既存の更新処理（write_ebay_status_if_advancing
    等）が常に注文ID一致の全行へUPDATEするため、対象行間で値が揃っている前提で
    先に見つかった行の値を採用する。商品名だけは行ごとに異なりうるため、
    重複を除いて出現順に全件保持する（1件も取りこぼさない）。

    戻り値: {注文ID: {"product_names": [str, ...], "ebay_status": str, "vendor_name": str}, ...}
    """
    result = {}
    with access_conn.cursor() as cur:
        cur.execute(FETCH_ACTIVE_ORDERS_SQL)
        for order_id, product_name, ebay_status, vendor_name in cur.fetchall():
            if not order_id:
                continue
            entry = result.setdefault(order_id, {
                "product_names": [],
                "ebay_status": ebay_status,
                "vendor_name": vendor_name,
            })
            if product_name and product_name not in entry["product_names"]:
                entry["product_names"].append(product_name)
    return result


# ------------------------------------------------------------
# 対象抽出
# ------------------------------------------------------------
def fetch_pending_seller_messages(sql_conn, access_frontend_conn):
    """
    対象抽出は次の順序で絞り込む（trx.vendor_messageの蓄積量に対象件数が
    連動して増え続けないようにするため、Access「到着日入力」の現在の対象を起点にする）:
      ① Access「到着日入力」に現在表示される注文ID（= trx.vendor_messageのvendor_item_id）
         と、そのeBayステータス・商品名・店舗（=vendor_name）を取得
      ② 店舗がTARGET_VENDOR_NAMESの注文IDについて、対応する trx.vendor_message の
         メッセージ履歴を取得する（履歴が1件も無い＝一度もメッセージが交換されて
         いない「無言発送」の注文IDも、店舗からvendor_nameが分かるため対象に含める）
      ③ 次のいずれかに該当する取引だけを対象にする（人が「返信不要」にした対象は除く）
           A. 最新メッセージが出品者（sender_type='出品者'）
           B. 発送済み（is_shipped_status）で、まだ返信2相当を送っていない
              （無言発送も対象に含まれる。最新メッセージが誰からでも良い）

    戻り値: [
        {
            "vendor_name": str,
            "vendor_item_id": str,
            "product_names": [str, ...],     # 同一注文IDに複数商品がある場合は全件
            "transaction_url": str|None,
            "seller_name": str,             # 履歴中の出品者メッセージの sender_name（無ければNone）
            "latest_message": {同形式の辞書}|None,
            "history": [同形式の辞書, ...],  # message_no昇順
            "suggested_reply": {"text": str, "source": str|None, "template_key": str|None},
            "can_skip": bool,               # 「返信不要」ボタンを表示してよいか
        },
        ...
    ]
    最新メッセージが新しい順（updated_atが無いものは末尾）で返す。
    """
    active_orders = fetch_active_orders(access_frontend_conn)
    if not active_orders:
        return []

    target_order_ids = [
        oid for oid, info in active_orders.items()
        if info["vendor_name"] in TARGET_VENDOR_NAMES
    ]
    if not target_order_ids:
        return []

    history_by_key = _fetch_histories_for_orders(sql_conn, target_order_ids)

    # trx.vendor_messageに一度も履歴が無い「無言発送」の注文IDも、店舗（=vendor_name）から
    # 判明するので、空の履歴として対象に加える（メッセージが無いこと自体は正常な状態）。
    for oid in target_order_ids:
        key = (active_orders[oid]["vendor_name"], oid)
        history_by_key.setdefault(key, [])

    items = []
    for (vendor_name, vendor_item_id), history in history_by_key.items():
        order_info = active_orders.get(vendor_item_id)
        if order_info is None:
            continue

        is_shipped = is_shipped_status(order_info["ebay_status"])
        latest = history[-1] if history else None
        suggested_reply = determine_suggested_reply(history, is_shipped)

        include = False

        # 条件A: 最新メッセージが出品者で、まだ「返信不要」にされていない
        # （reply_skippedはそのメッセージ行自体のフラグ。新しい出品者メッセージが
        # 来ると新しい行が追加され、そちらはreply_skipped=0がデフォルトなので
        # 自動的に再び対象になる＝eBay Messagesのskip_replyと同じ仕組み）。
        if latest and latest["sender_type"] == "出品者" and not latest["reply_skipped"]:
            include = True

        # 条件B: 発送済みで、まだ返信2相当を送っていない（無言発送も含む）。
        # メッセージが1件も無いことがあるため、reply_skippedを乗せる行が無く、
        # このケースは「返信不要」を保存しない（ボタン自体を表示しない）。
        if is_shipped and suggested_reply["template_key"] == "shipped_2":
            include = True

        if not include:
            continue

        # 返信不要ボタンは、実際にフラグを立てられる対象（条件Aで、かつ既に
        # 返信不要済みでない出品者メッセージが存在する場合）にのみ表示する。
        can_skip = bool(latest and latest["sender_type"] == "出品者" and not latest["reply_skipped"])

        seller_messages = [m for m in history if m["sender_type"] == "出品者"]
        items.append({
            "vendor_name": vendor_name,
            "vendor_item_id": vendor_item_id,
            "product_names": order_info["product_names"],
            "transaction_url": _build_transaction_url(vendor_name, vendor_item_id),
            "seller_name": seller_messages[-1]["sender_name"] if seller_messages else None,
            "is_shipped": is_shipped,
            "latest_message": latest,
            "history": history,
            "suggested_reply": suggested_reply,
            "can_skip": can_skip,
        })

    items.sort(key=lambda it: it["latest_message"]["message_no"] if it["latest_message"] else -1, reverse=True)
    return items


def _build_transaction_url(vendor_name, vendor_item_id):
    builder = TRANSACTION_URL_BUILDERS.get(vendor_name)
    return builder(vendor_item_id) if builder else None


def _fetch_histories_for_orders(sql_conn, order_ids):
    """
    order_ids（Access「到着日入力」の現在の対象注文ID）に含まれ、かつ
    vendor_nameがTARGET_VENDOR_NAMESであるtrx.vendor_messageの全メッセージを取得する。
    戻り値: {(vendor_name, vendor_item_id): [メッセージ辞書, ...]（message_no昇順）}
    """
    order_ids = list(order_ids)
    if not order_ids:
        return {}

    vendor_placeholders = ", ".join(["?"] * len(TARGET_VENDOR_NAMES))
    order_id_placeholders = ", ".join(["?"] * len(order_ids))
    params = list(TARGET_VENDOR_NAMES) + order_ids

    with sql_conn.cursor() as cur:
        cur.execute(f"""
            SELECT vendor_name, vendor_item_id, message_id, message_no, sender_name, sender_type,
                   message_datetime_text, message_datetime, message_body, reply_skipped
            FROM trx.vendor_message
            WHERE vendor_name IN ({vendor_placeholders})
              AND vendor_item_id IN ({order_id_placeholders})
            ORDER BY vendor_name, vendor_item_id, message_no
        """, params)
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    history_by_key = {}
    for row in rows:
        key = (row["vendor_name"], row["vendor_item_id"])
        # message_datetime（メルカリ・絶対日時）が入っていればそれを表示用文字列にする。
        # まだ移行していないサイト（ＰａｙＰａｙフリマ・ラクマ）や過去の未補正行は
        # message_datetime_text（画面表示の相対時刻等）にフォールバックする。
        dt = row["message_datetime"]
        display_datetime_text = dt.strftime("%Y/%m/%d %H:%M:%S") if dt else row["message_datetime_text"]
        history_by_key.setdefault(key, []).append({
            "message_id": row["message_id"],
            "message_no": row["message_no"],
            "sender_name": row["sender_name"],
            "sender_type": row["sender_type"],
            "message_datetime_text": display_datetime_text,
            "message_body": row["message_body"],
            "reply_skipped": bool(row["reply_skipped"]),
        })
    return history_by_key


# ------------------------------------------------------------
# 「返信不要」の永続化（trx.vendor_message.reply_skipped）
# ------------------------------------------------------------
# eBay Messages（trx.ebay_messages.skip_reply）と同じ考え方: 対応不要と判断した
# 「特定のメッセージ行」にフラグを立てるだけで、別テーブルは持たない。
# 常にその取引の最新メッセージ行のフラグで対象かどうかを判定するため、
# 新しい出品者メッセージが来ると新しい行（reply_skipped=0がデフォルト）が
# 最新行になり、自動的に再び対象へ戻る。
# 再スクレイピングのUPSERT（SQL_UPSERT_VENDOR_MESSAGE）はreply_skippedを
# UPDATE SET句に含めていないため、同じmessage_noの本文が更新されてもフラグは保持される。
#
# メッセージが1件も無い「無言発送」（発送済みで返信2待ちだが会話が無いケース）は、
# フラグを乗せる行が存在しないため、今回は返信不要を保存しない
# （fetch_pending_seller_messages側でcan_skip=Falseとしてボタン自体を出さない）。
def mark_reply_skipped(sql_conn, vendor_name: str, vendor_item_id: str, message_no: int) -> bool:
    """
    「返信不要」ボタン押下時に呼ぶ。指定した既存のメッセージ行のreply_skippedを1にする。
    戻り値: 該当行が存在し更新できたか。
    """
    with sql_conn.cursor() as cur:
        cur.execute("""
            UPDATE trx.vendor_message SET reply_skipped = 1
            WHERE vendor_name = ? AND vendor_item_id = ? AND message_no = ?
        """, vendor_name, vendor_item_id, message_no)
        updated = cur.rowcount > 0
    sql_conn.commit()
    return updated


# ------------------------------------------------------------
# 返信案（定型文判定）
# ------------------------------------------------------------
# 発送済み（無言発送含む）に対する定型文。到着報告・受取通知の一般的な断りを含む。
# 発送1回につき1回だけ提案する（履歴内に既に同趣旨の返信が無い場合のみ）。
TEMPLATE_SHIPPED = (
    "早々に発送いただきありがとうございます。\n"
    "到着を楽しみに待ってます。\n"
    "受取通知はなるべく早くできるように心がけておりますが、\n"
    "仕事等の事情により、少し遅くなる場合もございます。\n"
    "恐縮ですが、お待ちいただけますと助かります。"
)
# 既にこの趣旨の返信を送信済みかどうかの判定に使うキーワード（部分一致）。
_TEMPLATE_SHIPPED_DETECT_RE = re.compile("到着を楽しみに|受取通知")

# 出品者からのメッセージが「発送完了」の連絡かどうかの判定。
# 「発送」という文字を含むだけでは判定しない（「明日発送します」「発送予定です」
# 「発送手続きする予定です」「まだ発送していません」「発送が遅れています」
# 「発送できていません」等の未完了・予定・否定表現を誤検知しないため）。
# 「発送(手配)?(いた)?しました」の形の完了表現（発送しました／発送いたしました／
# 発送手配いたしました等）に加え、「発送手続き完了しました」も明確な発送完了表現の
# positive evidenceとして追加する（実例m95550121828で実際に出品者から届いた文言。
# 2026-09-01追加）。negative/未来表現の除外リストは追加せず、あくまで確認できた
# 完了表現を列挙する方式を維持する。
# ただし「発送手続き完了しましたら」（「〜たら」＝未来の条件節。実例m95550121828の
# message_no=1「発送手続き完了しましたらまたご連絡いたします」＝まだ発送していない
# 時点の予告文）は完了報告ではないため、直後に「ら」が続く場合は除外する
# （否定語の列挙ではなく、追加した完了表現そのものの精度を上げるための否定先読み）。
# メルカリ側の配送ステータス反映には時間差があるため（例: ゆうパケットポストの
# 投函直後など）、出品者が発送完了を明言した時点で発送のお礼を出したい、という
# 実運用上の要望に基づく。実際に発送済みかどうかの追跡はこの判定の責任範囲外
# （既存の通常の配送ステータス取得処理に任せる）。
_SHIPPED_COMPLETE_MESSAGE_RE = re.compile("発送(?:(?:手配)?(?:いた)?しました|手続き完了しました(?!ら))")

# 出品者からの最初のメッセージに対する返信（まだ発送前・まだ一度も返信していない場合のみ）。
TEMPLATE_FIRST_REPLY_ONEGAI = (
    "こちらこそ、お手数をおかけしますが、\n"
    "お取引終了まで、何卒、よろしくお願いいたします。"
)
TEMPLATE_FIRST_REPLY_PLAIN = (
    "お手数をおかけしますが、\n"
    "お取引終了まで、何卒、よろしくお願いいたします。"
)
# 出品者の文言に「お願いします」系が含まれるかどうかの判定（1-1 / 1-2の分岐）。
# 過剰な意味解析はせず、「お願い」の一般的な表記揺れ（します/いたします/致します）のみ拾う。
_ONEGAI_RE = re.compile("お願いします|お願いいたします|お願い致します")


def determine_suggested_reply(history: list, is_shipped: bool) -> dict:
    """
    history: message_no昇順の会話履歴（sender_type='出品者'|'購入者'）。空のこともある
             （無言発送で一度もメッセージが交換されていない場合）。
    is_shipped: 既存の購入スクレイピングが取得した取引ステータス・配送状況から判定した
                「発送済み以上」かどうか。無言発送（メッセージが一切無い発送）でも
                「2」を提案できるよう、メッセージ本文とは別に必要な情報。

    優先順位:
      1. 次のいずれかを満たせば「発送済み」とみなし、まだ返信2相当を送っていない
         場合に限り「2」を提案する（履歴中に既に同趣旨の返信があれば、二重に提案しない）。
           A. is_shipped（既存の配送ステータス取得処理が「発送済み」と確認済み）かつ、
              出品者からの未返信の最新メッセージが発送完了を否定するような内容
              （不明・発送予定・発送方法の相談等）になっていないこと。
           B. 出品者からの最新メッセージが「発送しました」等の発送完了連絡
              （_SHIPPED_COMPLETE_MESSAGE_RE。メルカリ側の配送ステータス反映の
              時間差を待たず、出品者本人の発送完了連絡を優先する）。
      2. 上記に該当しなければ、出品者からのメッセージがこれまでにちょうど1件だけあり、
         かつその出品者メッセージより後に自分からの返信がまだ無い場合に限り
         「1-1」または「1-2」を提案する（出品者からのメッセージが今回で初めてのケース。
         出品者メッセージが既に2件以上ある取引には提案しない＝人が判断する）。
         出品者のそのメッセージに「お願いします」系が含まれれば1-1、それ以外は1-2。
         購入直後に買い手自身が送る挨拶（出品者メッセージより前に存在する自分の
         メッセージ）は「返信済み」の判定に使わない（実データm11655754962で、
         購入直後の挨拶のせいで本来出すべき初回提案が出ない不具合があったため）。
      3. それ以外は自動提案しない（人が判断する。「返信不要」の対象になりうる）。

    【2026-08-30 発送済み誤判定の修正】is_shippedはAccess「日常」のeBayステータスに
    由来するが、write_ebay_status_if_advancing()は状態が後退する更新を行わない設計のため、
    過去に（既に修正済みの）get_raw_status()の不具合等で誤って一度「発送済み」以上へ
    書き込まれてしまった値は、その後に正しいステータスが取れるようになっても訂正されず
    残り続けることがある（実例: ラクマ ab9e1ed4354125c5aecd60b37a47c82c。実際には
    出品者が「宅急便コンパクトに変更させてほしい」と発送前の相談をしてきているのに、
    stale化したis_shipped=Trueだけでshipped_2を誤提案していた）。
    このため、出品者からの最新メッセージが未返信のまま残っている場合は、その内容が
    発送完了を明確に確認できるもの（_SHIPPED_COMPLETE_MESSAGE_RE）でない限り、
    is_shippedだけでは発送済み扱いにしない（サイト別のraw_status取得処理は変更せず、
    3サイト共通のこの判定関数だけで対応する）。無言発送（履歴が空）や、既に自分が
    返信済みで最新メッセージが自分のものである場合は、この制約の対象外（従来通り
    is_shippedのみで判定できる）。

    【2026-09-01 スタンプによる誤判定の修正】メルカリのスタンプメッセージは実際の
    絵柄・文言を取得できないため、mercari_get_messages()が固定のプレースホルダー
    文字列"スタンプ"を本文として保存する（推測ではなく既存コードの既知の仕様）。
    このプレースホルダーは出品者からの通常のテキストメッセージと同じ
    sender_type='出品者'の1行としてhistoryに残るため、実際のテキストの後にスタンプが
    届いただけで「出品者からのメッセージが2件になった」「最新の出品者メッセージが
    スタンプになった」と誤認し、本来出すべき提案（1-1/1-2やshipped_2）が出なくなる
    不具合があった（実例: m95550121828。出品者の本文1件の直後にスタンプが届いた
    ことで、優先順位2の「ちょうど1件」判定が崩れ候補なしになっていた）。
    このため、優先順位2の件数判定・優先順位1Bの「最新の出品者メッセージ」判定では、
    本文が"スタンプ"のプレースホルダーと完全一致するメッセージだけを対象から除外する
    （空メッセージ・絵文字・短文等の推測による除外は行わない）。history自体・
    replied_after等の他の判定は変更せず、全メッセージをそのまま使う。
    """
    own_messages = [m for m in history if m["sender_type"] == "購入者"]
    seller_messages = [m for m in history if m["sender_type"] == "出品者"]
    latest_message = history[-1] if history else None

    # メルカリのスタンプメッセージのプレースホルダー（mercari_get_messages参照）。
    # 返信内容の判断上は実質的な出品者メッセージとして数えない。
    meaningful_seller_messages = [
        m for m in seller_messages if (m["message_body"] or "") != "スタンプ"
    ]

    seller_announced_shipped = bool(
        meaningful_seller_messages
        and _SHIPPED_COMPLETE_MESSAGE_RE.search(meaningful_seller_messages[-1]["message_body"] or "")
    )

    # 出品者からの最新メッセージがまだ自分から返信されておらず（＝会話全体の最新が
    # 出品者のメッセージ）、かつそのメッセージ自体が発送完了を確認できる内容でない場合は、
    # 「不明・発送予定・発送方法の相談」等の可能性があるため、is_shippedが立っていても
    # 発送済み扱いにしない（stale化したis_shippedによる誤判定を防ぐ）。
    seller_pending_unconfirmed = bool(
        latest_message
        and latest_message["sender_type"] == "出品者"
        and not seller_announced_shipped
    )

    if seller_announced_shipped or (is_shipped and not seller_pending_unconfirmed):
        already_sent = any(
            _TEMPLATE_SHIPPED_DETECT_RE.search(m["message_body"] or "") for m in own_messages
        )
        if not already_sent:
            return {"text": TEMPLATE_SHIPPED, "source": "template", "template_key": "shipped_2"}
        return {"text": "", "source": None, "template_key": None}

    if len(meaningful_seller_messages) == 1:
        first_seller_message = meaningful_seller_messages[0]
        replied_after = any(
            m["sender_type"] == "購入者" and m["message_no"] > first_seller_message["message_no"]
            for m in history
        )
        if not replied_after:
            seller_text = first_seller_message["message_body"] or ""
            if _ONEGAI_RE.search(seller_text):
                return {"text": TEMPLATE_FIRST_REPLY_ONEGAI, "source": "template", "template_key": "first_reply_onegai"}
            return {"text": TEMPLATE_FIRST_REPLY_PLAIN, "source": "template", "template_key": "first_reply_plain"}

    return {"text": "", "source": None, "template_key": None}


# ============================================================================
# ============================================================================
# Chromeタブ管理（3サイト共通）
#
# 【2026-08-31 実機不具合を受けて追加】driver.switch_to.new_window('tab')は
# 新規タブを作成すると同時にそのタブをアクティブ化するため、Chromeウィンドウが
# OSの前面に上がってしまう（実機確認済み）。また、旧実装ではPayPayフリマ・
# ラクマのmain()に処理用タブを閉じる処理が一切無く、メルカリのmain()も
# 「前回実行のMERCARI_TARGET_URLページ」以外の残骸タブ（他サイトの取引ページ・
# 空の「新しいタブ」等）は一切閉じていなかった。この結果、同一Chromeプロファイル内に
# 処理用タブが際限なく蓄積し、蓄積したタブ数が多い状態で新規タブを作ると
# メルカリの購入一覧ページの中身（取引カード部分）が描画されず0件取得になる、
# という実害を実機で確認した（2026-08-31、m45811815800が一覧取得から
# 欠落した事例）。
#
# 対策として、処理用タブは必ずTarget.createTarget(background=True)で作成し
# （アクティブ化を伴わずChromeウィンドウを前面化しないことを実機確認済み。
# その後のdriver.switch_to.window()・driver.get()も前面化しないことを実機確認済み）、
# 使用後は成功・失敗にかかわらずTarget.closeTargetで必ずそのタブだけを閉じる
# （driver.close()はSeleniumの「現在のウィンドウ」という概念に依存し、途中で
# current_window_handleが変わっていると失敗しうるため使わない）。
# これにより処理を繰り返してもタブ数が増えなくなる。
#
# 送信処理（*_send_chat_message）でのPage.bringToFrontは、CDPのInput.insertTextが
# 対象タブの前面化・フォーカスを要求するため引き続き必要であり、変更しない。
#
# ユーザーが自分で開いている既存タブには一切触れない（自分が作った処理用タブ
# だけを対象にする。既存タブ全般の一括整理は行わない）。
def _create_processing_tab(driver) -> str:
    """
    処理用タブをbackgroundで作成し、Selenium操作対象をそのタブへ切り替えたうえで
    target_idを返す。作成・切替のみでアクティブ化・前面化はしない。

    【2026-08-31 実機調査で判明】Mercariの購入一覧ページは、document.hasFocus()が
    falseの状態（＝タブがbackground/非アクティブのまま）だと、一覧データの取得
    そのものを行わない（JSエラーは出ず、購入一覧専用APIの呼び出し自体が発生しない）
    ことを実機確認済み。人間が実際に同じChromeプロファインでタブを手動表示した場合は
    正常に一覧が表示されることも確認済みで、アカウント・ログイン・レート制限の問題では
    ないと判断した。CDPのEmulation.setFocusEmulationEnabledで「フォーカスされている
    ことにする」ことで、OSの前面に出さずにdocument.hasFocus()をtrueにできることを
    実機確認済み（実際にウィンドウを前面化する必要が無い）。この設定は処理用タブ
    共通で有効にしておく（PayPayフリマ・ラクマで同種の問題が将来起きた場合の予防にもなる）。
    """
    result = driver.execute_cdp_cmd("Target.createTarget", {"url": "about:blank", "background": True})
    tab_id = result["targetId"]
    driver.switch_to.window(tab_id)
    driver.execute_cdp_cmd("Emulation.setFocusEmulationEnabled", {"enabled": True})
    return tab_id


def _close_processing_tab(driver, tab_id) -> None:
    """
    _create_processing_tab()で作成した処理用タブだけを確実に閉じる。
    呼び出し元のfinallyから必ず呼ばれる想定のため、失敗しても例外を投げない
    （既に閉じている・tab_idがNone等でも安全に無視する）。
    """
    if not tab_id:
        return
    try:
        driver.execute_cdp_cmd("Target.closeTarget", {"targetId": tab_id})
    except Exception:
        pass


# ============================================================================
# ============================================================================
# メルカリ固有（旧 apps/etc/mercari_purchase.py）
# 同名衝突があった識別子は mercari_ / MERCARI_ 接頭辞で区別している。
# ============================================================================
# ============================================================================
MERCARI_CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
MERCARI_PROFILE_DIR = r"D:\apps_nostock\selenium_profile"
MERCARI_DEBUG_PORT = 9223
MERCARI_LAUNCH_TIMEOUT_SEC = 30

MERCARI_TARGET_URL = "https://jp.mercari.com/mypage/purchases"

MERCARI_CURRENT_URL_RETRY_COUNT = 5
MERCARI_CURRENT_URL_RETRY_INTERVAL_SEC = 1

# trx.vendor_purchase / trx.vendor_message / trx.vendor_purchase_unregistered の
# vendor_name。既存システムで使われている表記に合わせる。
MERCARI_VENDOR_NAME = "メルカリ"

# status が以下の場合は人手入力とみなして上書きしない
MERCARI_STATUS_NO_OVERWRITE = ("GA鑑定待ち", "出荷済み", "◎有在庫")

MERCARI_SQL_UPSERT_VENDOR_PURCHASE = """
MERGE INTO trx.vendor_purchase WITH (HOLDLOCK) AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?)) AS src
    (vendor_name, vendor_item_id, purchase_datetime, purchase_price, status, item_name)
ON (tgt.vendor_name = src.vendor_name AND tgt.vendor_item_id = src.vendor_item_id)
WHEN MATCHED THEN
    UPDATE SET
        purchase_datetime = src.purchase_datetime,
        purchase_price    = src.purchase_price,
        status            = CASE
                                WHEN tgt.status IN (N'GA鑑定待ち', N'出荷済み', N'◎有在庫') THEN tgt.status
                                ELSE src.status
                            END,
        item_name         = src.item_name,
        arrival_datetime  = CASE
                                WHEN src.status = N'☆出荷可能' AND tgt.arrival_datetime IS NULL THEN CAST(GETDATE() AS DATE)
                                ELSE tgt.arrival_datetime
                            END,
        updated_at        = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (vendor_name, vendor_item_id, purchase_datetime, purchase_price, status, item_name, arrival_datetime, updated_at)
    VALUES (
        src.vendor_name, src.vendor_item_id, src.purchase_datetime, src.purchase_price, src.status, src.item_name,
        CASE WHEN src.status = N'☆出荷可能' THEN CAST(GETDATE() AS DATE) ELSE NULL END,
        GETDATE()
    );
"""

MERCARI_SQL_UPSERT_VENDOR_MESSAGE_BY_ID = """
MERGE INTO trx.vendor_message WITH (HOLDLOCK) AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS src
    (vendor_name, vendor_item_id, message_id, message_no, sender_name, sender_type, message_datetime, message_body)
ON (tgt.vendor_name = src.vendor_name
    AND tgt.vendor_item_id = src.vendor_item_id
    AND tgt.message_id = src.message_id)
WHEN NOT MATCHED THEN
    INSERT (vendor_name, vendor_item_id, message_id, message_no, sender_name, sender_type, message_datetime, message_body, updated_at)
    VALUES (src.vendor_name, src.vendor_item_id, src.message_id, src.message_no, src.sender_name, src.sender_type, src.message_datetime, src.message_body, GETDATE());
"""
# 既存の(vendor_name, vendor_item_id, message_id)に一致する行は意図的に一切更新しない
# （message_body/message_datetime/sender_name/sender_type/updated_at/reply_skippedを含む）。
# 新しいmessage_idが増えた分だけINSERTする。理由:
#   - message_idはメルカリ内部の安定した一意IDで、後から内容が変わることは想定しない
#   - reply_skippedやupdated_atを毎回のスクレイプで意図せず上書きしないため
# message_noは(vendor_name, vendor_item_id, message_no)の既存PRIMARY KEYを満たすために
# 引き続き採番して保存するが、識別・突き合わせにはmessage_idを使う（message_noは将来的に
# 廃止予定。PayPayフリマ・ラクマは今回未対応のためPRIMARY KEYからは外せない）。


# ------------------------------------------------------------
# Chrome起動・タブ管理
# ------------------------------------------------------------
def _mercari_debugger_alive(port: int) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


def mercari_ensure_chrome_debugger(port: int = MERCARI_DEBUG_PORT, profile_dir: str = MERCARI_PROFILE_DIR,
                                    timeout: int = MERCARI_LAUNCH_TIMEOUT_SEC) -> None:
    """デバッグポートで応答するChromeがなければ起動し、応答するまで待つ"""
    if _mercari_debugger_alive(port):
        print(f"OK: 起動済みのChrome(ポート{port})を利用します")
        return

    print(f"Chromeをリモートデバッグモードで起動します（ポート{port}）...")
    subprocess.Popen([
        MERCARI_CHROME_EXE,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
    ])

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _mercari_debugger_alive(port):
            print("OK: Chrome起動完了")
            return
        time.sleep(0.5)

    raise RuntimeError(f"Chromeの起動確認がタイムアウトしました（{timeout}秒）")


def _get_current_url_with_retry(driver, retries: int = MERCARI_CURRENT_URL_RETRY_COUNT,
                                 interval: float = MERCARI_CURRENT_URL_RETRY_INTERVAL_SEC) -> str:
    """ナビゲーション直後の一時的な execution context 喪失に備えてリトライする"""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return driver.current_url
        except TimeoutException as e:
            last_error = e
            print(f"  ({attempt}/{retries}) current_url取得失敗、リトライします: {e.msg}")
            time.sleep(interval)
    raise last_error


CHECKBOX_CHECK_RETRY_COUNT = 3
CHECKBOX_CHECK_RETRY_INTERVAL_SEC = 1.5


def _ensure_in_transaction_checkbox_checked(driver, retries: int = CHECKBOX_CHECK_RETRY_COUNT,
                                             interval: float = CHECKBOX_CHECK_RETRY_INTERVAL_SEC) -> None:
    """
    「取引中の商品」チェックボックスをON にする。
    Seleniumのネイティブclick()はこのチェックボックス（Reactの制御コンポーネント）に対して
    例外を出さずに反映されない（is_selected()がFalseのまま）ことが実機検証で確認されており、
    その場合 _collect_transaction_urls() は常に0件を返し、処理対象が丸ごと欠落する
    （かつ呼び出し元はエラーに気づけない）。そのためJSクリックへのフォールストと、
    クリック後の状態検証を必須にする。検証できない場合は呼び出し元で明示的に失敗させる。
    """
    checkbox = driver.find_element(
        By.CSS_SELECTOR,
        '[data-testid="user-listing-inTransactionItemsCheckbox"]'
    )
    if checkbox.is_selected():
        print("OK: 「取引中の商品」は既にチェック済みです")
        return

    for attempt in range(1, retries + 1):
        checkbox.click()
        time.sleep(interval)
        if checkbox.is_selected():
            print("OK: 「取引中の商品」にチェックを入れました")
            return

        print(f"  ({attempt}/{retries}) クリックしてもチェック状態を確認できません。JSクリックで再試行します")
        driver.execute_script("arguments[0].click();", checkbox)
        time.sleep(interval)
        if checkbox.is_selected():
            print("OK: 「取引中の商品」にチェックを入れました（JSクリック）")
            return

    raise RuntimeError(
        "「取引中の商品」チェックボックスをチェックできませんでした"
        "（クリックしても状態が変化しません）"
    )


LINK_COLLECTION_RETRY_COUNT = 10
LINK_COLLECTION_RETRY_INTERVAL_SEC = 2.0

MORE_BUTTON_CLICK_MAX_COUNT = 30
MORE_BUTTON_CLICK_WAIT_SEC = 2.0


def _expand_all_transactions(driver, max_clicks: int = MORE_BUTTON_CLICK_MAX_COUNT,
                              wait_sec: float = MORE_BUTTON_CLICK_WAIT_SEC) -> None:
    """
    「取引中の商品」一覧は初期表示だけでは全件出ず、末尾の「もっと見る」ボタンを
    押すたびに残りが追加読み込みされる（実機検証済み: 初期46件→1クリックで84件。
    未読込のまま放置すると、その分の取引が丸ごとスクレイピング対象から漏れる）。
    ボタンが表示されなくなるまで繰り返し押す。
    """
    for _ in range(max_clicks):
        more_buttons = [
            b for b in driver.find_elements(By.TAG_NAME, "button")
            if "もっと見る" in (b.text or "") and b.is_displayed()
        ]
        if not more_buttons:
            return

        button = more_buttons[0]
        driver.execute_script("arguments[0].scrollIntoView(true);", button)
        time.sleep(0.3)
        try:
            button.click()
        except Exception:
            driver.execute_script("arguments[0].click();", button)
        time.sleep(wait_sec)

    print(f"WARN: 「もっと見る」を{max_clicks}回押しても表示され続けています。安全のため打ち切ります")


def _collect_transaction_urls(driver, retries: int = LINK_COLLECTION_RETRY_COUNT,
                               interval: float = LINK_COLLECTION_RETRY_INTERVAL_SEC):
    """
    取引一覧はチェックボックス操作後に非同期で読み込まれる（スケルトン表示中は
    リンクが存在しない）ため、/transaction/ を含むリンクが見つかるまでポーリングする。
    最初のリンクが見つかった後は、「もっと見る」が無くなるまで押してから
    改めて全件を集め直す（初期表示分だけでは全件にならないため）。

    【2026-08-31 実機不具合を受けて修正】ポーリングを最後まで繰り返しても
    1件もリンクが見つからない場合、旧実装は無条件に空リストを返しており、
    「本当に取引が0件」なのか「一覧ページの描画・読み込みに失敗しただけ」なのかを
    区別できなかった。実機確認済みの事例（2026-08-31、Chromeタブの蓄積が原因で
    購入一覧ページの中身が描画されず、実際には取引が存在するのに0件のまま
    正常終了扱いになり、m45811815800が取得から欠落した）を受けて、
    retries回（既定20秒）待っても1件も見つからない場合は、0件と決めつけず
    例外を送出するようにした。呼び出し元（mercari_main）で捕捉されず、
    このサイトの実行全体がエラー扱いになる（正常終了扱いにしない）。
    """
    def _current_urls():
        seen = set()
        urls = []
        for link in driver.find_elements(By.TAG_NAME, "a"):
            href = link.get_attribute("href")
            if href and "/transaction/" in href and href not in seen:
                seen.add(href)
                urls.append(href)
        return urls

    for attempt in range(1, retries + 1):
        if _current_urls():
            break
        if attempt < retries:
            time.sleep(interval)
    else:
        raise RuntimeError(
            f"購入した商品の一覧が読み込めませんでした（{retries * interval:.0f}秒待っても"
            "取引リンクが1件も見つかりません）。一覧ページの描画に失敗している可能性が"
            "あるため、0件と決めつけず処理を中断します。"
        )

    _expand_all_transactions(driver)
    return _current_urls()


# ------------------------------------------------------------
# Access（日常テーブル）への同期
# ------------------------------------------------------------
def sync_arrival_status_to_access(sql_conn):
    """
    trx.vendor_purchase(vendor_name=MERCARI_VENDOR_NAME) の status / 到着日を、
    日常テーブルの eBayステータス / 到着日 へ 注文ID(=vendor_item_id) をキーに反映する。
    到着日は「日常.到着日が現在NULLのときだけ」arrival_datetime（メルカリのステータスから
    到着を検知した日）で埋める。既に値がある場合は上書きしない
    ——より正確な到着日は sync_carrier_tracking_to_daily() がヤマト運輸／日本郵便の
    追跡結果から直接更新するため、ここで古い検知日に巻き戻さないようにするのが目的。
    このため mercari_main() では本関数を sync_carrier_tracking_to_daily() の後に呼ぶこと。
    status が到着済み（☆出荷可能）の場合のみ、日常.販売（eBay/amazon）に応じて
    書き込む文言を分ける（販売がそれ以外の値の場合は想定外のため☆出荷可能のまま扱う）。
    eBayステータスは write_ebay_status_if_advancing により、状態が後退する更新
    （例: 到着予定→発送済み）は行わない（配送会社の追跡が既にmercari側の表示より
    進んでいる場合に巻き戻さないため）。
    """
    with sql_conn.cursor() as cur:
        cur.execute("""
            SELECT vendor_item_id, status, arrival_datetime
            FROM trx.vendor_purchase
            WHERE vendor_name = ?
        """, MERCARI_VENDOR_NAME)
        rows = cur.fetchall()

    access_conn = get_access_connection()
    try:
        access_cur = access_conn.cursor()
        updated = 0
        for vendor_item_id, status, arrival_datetime in rows:
            access_status = status
            if status == ARRIVED_STATUS:
                sales_row = access_cur.execute(
                    f"SELECT 販売 FROM {ACCESS_TABLE} WHERE 注文ID = ?", vendor_item_id
                ).fetchone()
                sales_channel = sales_row[0] if sales_row else None
                access_status = ARRIVED_STATUS_BY_SALES_CHANNEL.get(sales_channel, ARRIVED_STATUS)

            write_ebay_status_if_advancing(access_cur, vendor_item_id, access_status)
            access_cur.execute(
                f"UPDATE {ACCESS_TABLE} SET 到着日 = IIF(到着日 IS NULL, ?, 到着日) WHERE 注文ID = ?",
                arrival_datetime, vendor_item_id
            )
            updated += access_cur.rowcount
        access_conn.commit()
        access_cur.close()
    finally:
        access_conn.close()

    print(f"Access同期({ACCESS_TABLE}): {len(rows)}件中 {updated}行を更新")


def sync_unregistered_daily_items(sql_conn):
    """
    trx.vendor_purchase(vendor_name=MERCARI_VENDOR_NAME) のうち、日常テーブルに対応する
    注文IDのレコードが存在しないものを trx.vendor_purchase_unregistered に記録する。
    （逆に日常にはあるがメルカリの取引中に無いケースは、日常側がカード履歴と突合して
    いずれ判明するため対象外）
    日常に登録されて解消されたものはリストから自動的に外す。
    """
    with sql_conn.cursor() as cur:
        cur.execute("SELECT vendor_item_id FROM trx.vendor_purchase WHERE vendor_name = ?", MERCARI_VENDOR_NAME)
        vendor_ids = {row[0] for row in cur.fetchall()}

    access_conn = get_access_connection()
    try:
        access_cur = access_conn.cursor()
        access_cur.execute(f"SELECT DISTINCT 注文ID FROM {ACCESS_TABLE} WHERE 注文ID IS NOT NULL")
        daily_ids = {row[0] for row in access_cur.fetchall()}
        access_cur.close()
    finally:
        access_conn.close()

    missing = vendor_ids - daily_ids

    with sql_conn.cursor() as cur:
        cur.execute("SELECT vendor_item_id FROM trx.vendor_purchase_unregistered WHERE vendor_name = ?", MERCARI_VENDOR_NAME)
        tracked = {row[0] for row in cur.fetchall()}

        newly_missing = missing - tracked
        resolved = tracked - missing

        for vendor_item_id in newly_missing:
            cur.execute(
                "INSERT INTO trx.vendor_purchase_unregistered (vendor_name, vendor_item_id, detected_at) VALUES (?, ?, GETDATE())",
                MERCARI_VENDOR_NAME, vendor_item_id
            )
        for vendor_item_id in resolved:
            cur.execute(
                "DELETE FROM trx.vendor_purchase_unregistered WHERE vendor_name = ? AND vendor_item_id = ?",
                MERCARI_VENDOR_NAME, vendor_item_id
            )
    sql_conn.commit()

    print(f"日常未登録チェック: 現在{len(missing)}件（新規{len(newly_missing)}件, 解消{len(resolved)}件）")


# ------------------------------------------------------------
# 取引ページのスクレイピング
# ------------------------------------------------------------
def get_vendor_item_id(url):
    return url.rstrip("/").split("/")[-1]


def parse_japanese_datetime(text):
    m = re.match(r"(\d+)年(\d+)月(\d+)日\s+(\d+):(\d+)", text.strip())
    if not m:
        raise ValueError(f"日時のパースに失敗: {text!r}")
    year, month, day, hour, minute = [int(x) for x in m.groups()]
    return datetime(year, month, day, hour, minute)


GET_RAW_STATUS_RETRY_COUNT = 3
GET_RAW_STATUS_RETRY_INTERVAL_SEC = 2.0

# プログレスバーの各ステップ文字列のうち、実際に買い手の手元に到着したことを示すもの
ARRIVED_STEP_LABELS = ("配達済み", "受取")

# [data-testid="status-heading"] に表示され、買い手側の受け取りが既に完了していることを示す見出し
# （出品者評価待ち／取引完了のいずれも買い手側は到着済みで対応不要のため☆出荷可能扱い）
STATUS_HEADING_ARRIVED_PREFIXES = ("受取評価をしました", "取引が完了しました")


def mercari_get_raw_status(driver, retries: int = GET_RAW_STATUS_RETRY_COUNT,
                            retry_interval: float = GET_RAW_STATUS_RETRY_INTERVAL_SEC):
    """配送状態を取得する。発送前の場合は '発送前' を返す（購入済/連絡あり への分岐は呼び出し元で行う）"""
    for attempt in range(1, retries + 1):
        waiting = driver.find_elements(
            By.XPATH,
            "//p[contains(text(), '発送をお待ちください')]"
        )
        if waiting:
            return "発送前"

        status_heading = driver.find_elements(
            By.CSS_SELECTOR,
            '[data-testid="status-heading"]'
        )
        if status_heading:
            heading_text = status_heading[0].text.strip()
            if heading_text.startswith(STATUS_HEADING_ARRIVED_PREFIXES):
                return "☆出荷可能"

        progress_bar = driver.find_elements(
            By.CSS_SELECTOR,
            '[data-testid="transaction:shippingStatus.progressBar"]'
        )
        if progress_bar:
            current_step = progress_bar[0].find_elements(
                By.CSS_SELECTOR,
                '[aria-current="step"]'
            )
            if not current_step:
                # 配送業者からの追跡情報が一時的に取得できない状態。実機確認済みで、
                # [data-testid="transaction:shipping-status"] 配下に「調査中」の見出しが
                # 表示され、プログレスバーのどのステップも現在地としてマークされない。
                investigating = driver.find_elements(
                    By.XPATH,
                    "//section[@data-testid='transaction:shipping-status']"
                    "//p[normalize-space(text())='調査中']"
                )
                if investigating:
                    return "調査中"
                raise RuntimeError("aria-current='step' の要素が見つかりません")

            step_text = current_step[0].text.strip()
            if not step_text:
                raise RuntimeError("aria-current='step' のテキストが空です")

            if step_text in ARRIVED_STEP_LABELS:
                return "☆出荷可能"

            # 発送済み/輸送中/配達中など、到着前の中間ステップは一律「発送済み」として扱う
            return "発送済み"

        aside = driver.find_elements(
            By.CSS_SELECTOR,
            'aside[aria-label="受取評価が行われていません"]'
        )
        if aside:
            return "発送済み"

        # 「あんしん鑑定」（第三者鑑定）を利用している商品は専用バナーで状態が表示される。
        # 鑑定事業者への発送中/鑑定完了待ち/受取評価待ちのいずれも買い手にはまだ届いていない
        # 段階のため、既存の分類にならい一律「発送済み」として扱う。
        kantei_banner = driver.find_elements(
            By.CSS_SELECTOR,
            'aside.merInformationBubble p.merText'
        )
        if kantei_banner:
            return "発送済み"

        # ページの描画がまだ間に合っていないだけの可能性があるためリトライする
        if attempt < retries:
            time.sleep(retry_interval)

    raise RuntimeError("配送ステータス要素が見つかりません")


TRANSACTION_PAGE_READY_RETRY_COUNT = 8
TRANSACTION_PAGE_READY_RETRY_INTERVAL_SEC = 1.0


def _wait_for_transaction_page_ready(driver, retries: int = TRANSACTION_PAGE_READY_RETRY_COUNT,
                                      interval: float = TRANSACTION_PAGE_READY_RETRY_INTERVAL_SEC) -> None:
    """
    個別取引ページへの遷移直後、get_item_name()・get_purchase_info()が参照する要素が
    実際にDOM上へ現れるまで待つ。

    【2026-09-02 実機不具合を受けて追加】旧実装は遷移後に固定でtime.sleep(4)するだけで、
    get_item_name()・get_purchase_info()自体はリトライを持たず「要素が無ければ即例外」
    という設計だった。実機で、この4秒がまれに足りず、該当取引1件だけが
    per-item try/exceptで静かにスキップされる事例を確認した
    （実例: m44049599421、2026-09-01朝の実行）。mercari_get_raw_status()は既に
    自前のリトライを持つため対象外。

    ここでは実際に必要な要素の出現を確認してから処理を進める（単純なsleep延長ではない）。
    タイムアウトしてもこの関数自体は例外を投げない。要素が最終的に見つからない場合は、
    従来どおりget_item_name()/get_purchase_info()自身が明確なエラーメッセージ付きで
    例外を送出する（エラーメッセージの二重管理を避けるため）。
    """
    for attempt in range(1, retries + 1):
        item_name_ready = bool(driver.find_elements(
            By.CSS_SELECTOR, '[data-testid="transaction:information-for-buyer.item-object-itemLabel"]'
        ))
        purchase_date_ready = bool(driver.find_elements(
            By.CSS_SELECTOR, '[data-partner-id="purchase-date"]'
        ))
        if item_name_ready and purchase_date_ready:
            return
        if attempt < retries:
            time.sleep(interval)


def get_item_name(driver):
    els = driver.find_elements(
        By.CSS_SELECTOR,
        '[data-testid="transaction:information-for-buyer.item-object-itemLabel"]'
    )
    if not els:
        raise RuntimeError("商品名要素が見つかりません (transaction:information-for-buyer.item-object-itemLabel)")
    return els[0].text.strip()


def get_purchase_info(driver):
    date_els = driver.find_elements(
        By.CSS_SELECTOR,
        '[data-partner-id="purchase-date"]'
    )
    if not date_els:
        raise RuntimeError("購入日時要素が見つかりません (data-partner-id='purchase-date')")
    purchase_datetime = parse_japanese_datetime(date_els[0].text.strip())

    price_els = driver.find_elements(
        By.CSS_SELECTOR,
        "span.number__6b270ca7"
    )
    if not price_els:
        raise RuntimeError("購入金額要素が見つかりません (span.number__6b270ca7)")
    price_text = price_els[0].text.strip().replace(",", "")
    if not price_text.isdigit():
        raise ValueError(f"購入金額のパースに失敗: {price_text!r}")
    purchase_price = int(price_text)

    return purchase_datetime, purchase_price


MESSAGES_API_URL_SUBSTR = "transaction_messages/get_messages"
EVIDENCE_API_URL_SUBSTR = "transaction_evidences/get"
MERCARI_SEND_MESSAGE_API_URL_SUBSTR = "transaction_messages/post"

JST = timezone(timedelta(hours=9))


def _get_ws_debugger_url(port: int, url_substr: str) -> str:
    tabs = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list", timeout=10).read())
    for t in tabs:
        if t.get("type") == "page" and url_substr in t.get("url", ""):
            return t["webSocketDebuggerUrl"]
    raise RuntimeError(f"対象タブが見つかりません（url_substr={url_substr!r}）")


def _capture_mercari_api_responses(driver, order_id: str, port: int = MERCARI_DEBUG_PORT, timeout_sec: float = 15) -> dict:
    """
    transaction_evidences/get・transaction_messages/get_messages は、DPoP（リクエスト
    ごとに署名されたワンタイムJWT）等の認証ヘッダーが必須で、こちらからfetch()を
    直接発行しても401/400になることを実機確認済み。ページ自身が正規に発行する
    リクエストをChrome DevTools Protocol経由で横取りする方式にした。

    取引ページ（driverが現在開いているタブ）をCDP経由でリロードし、両APIの
    レスポンス本文を捕捉して返す。selenium(driver)自体の状態には触れない
    （別途webSocket接続でCDPコマンドを送るのみ）。

    戻り値: {"evidence": dict, "messages": dict}
    """
    import websocket as _ws_client  # ローカルimport: このAPI捕捉専用のため使用箇所を限定する

    ws_url = _get_ws_debugger_url(port, f"transaction/{order_id}")
    ws = _ws_client.create_connection(ws_url, timeout=timeout_sec, suppress_origin=True)
    try:
        next_id = [0]

        def send(method, params=None):
            next_id[0] += 1
            ws.send(json.dumps({"id": next_id[0], "method": method, "params": params or {}}))
            return next_id[0]

        def wait_for_id(target_id, deadline):
            while time.time() < deadline:
                ws.settimeout(max(0.1, deadline - time.time()))
                try:
                    raw = ws.recv()
                except Exception:
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                if data.get("id") == target_id:
                    return data
            return None

        send("Network.enable")
        send("Page.reload")

        want = (EVIDENCE_API_URL_SUBSTR, MESSAGES_API_URL_SUBSTR)
        pending_request_ids = {}  # requestId -> url
        bodies = {}  # url_substr -> parsed json
        deadline = time.time() + timeout_sec

        while time.time() < deadline and len(bodies) < len(want):
            ws.settimeout(max(0.1, deadline - time.time()))
            try:
                raw = ws.recv()
            except Exception:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            method = data.get("method")
            if method == "Network.responseReceived":
                p = data["params"]
                url = p["response"]["url"]
                for w in want:
                    if w in url and w not in bodies:
                        pending_request_ids[p["requestId"]] = w
            elif method == "Network.loadingFinished":
                req_id = data["params"]["requestId"]
                if req_id in pending_request_ids:
                    w = pending_request_ids.pop(req_id)
                    get_id = send("Network.getResponseBody", {"requestId": req_id})
                    resp = wait_for_id(get_id, time.time() + 5)
                    if resp and "result" in resp:
                        bodies[w] = json.loads(resp["result"]["body"])

        missing = [w for w in want if w not in bodies]
        if missing:
            raise RuntimeError(f"APIレスポンスの捕捉に失敗しました: {missing}")

        return {"evidence": bodies[EVIDENCE_API_URL_SUBSTR], "messages": bodies[MESSAGES_API_URL_SUBSTR]}
    finally:
        ws.close()


def mercari_get_messages(driver, order_id: str):
    """
    取引メッセージ全件を、DOM解析ではなくメルカリ内部APIから取得する
    （実機確認済み: transaction_messages/get_messagesがスレッド全件を1回で返すため、
    個別取引ページの「メッセージをもっと見る」を押す必要が無く、DOM側の展開状態に
    依存しないため取りこぼしが起きない）。

    出品者/購入者の判定は、transaction_evidences/getのbuyer_idと各メッセージの
    user_idを比較して行う（report-buttonの有無で判定していた旧DOM方式は、
    スタンプメッセージにreport-buttonが無いため出品者のスタンプを購入者と誤判定する
    バグがあったため廃止）。

    戻り値: [
        {
            "message_id": int,           # メルカリ内部の一意なメッセージID
            "message_no": int,           # API配列の並び順(1始まり)。会話は追記のみで
                                          # 既存メッセージの順序は変わらない前提の連番。
            "sender_name": str,
            "message_body": str,         # スタンプの場合は絵柄不明のため"スタンプ"固定
            "message_datetime": datetime,# JST・秒まで（tzinfoなし）
            "is_from_seller": bool,
        },
        ...
    ]
    """
    captured = _capture_mercari_api_responses(driver, order_id)
    evidence = captured["evidence"]
    if evidence.get("result") != "OK":
        raise RuntimeError(f"transaction_evidences取得に失敗しました: {evidence}")
    buyer_id = evidence["data"]["buyer_id"]

    messages_resp = captured["messages"]
    if messages_resp.get("result") != "OK":
        raise RuntimeError(f"transaction_messages取得に失敗しました: {messages_resp}")

    messages = []
    for i, m in enumerate(messages_resp.get("data", []), start=1):
        is_from_seller = m["user_id"] != buyer_id
        sender_name = m.get("user", {}).get("name") if is_from_seller else "自分"

        created = m.get("created")
        message_datetime = datetime.fromtimestamp(created, tz=JST).replace(tzinfo=None) if created else None

        message_body = m.get("body") or ""
        if not message_body and m.get("stamp"):
            # スタンプの絵柄・文言はAPI上もaria-label同様に取得できないため推測しない。
            message_body = "スタンプ"

        messages.append({
            "message_id": m["id"],
            "message_no": i,
            "sender_name": sender_name,
            "message_body": message_body,
            "message_datetime": message_datetime,
            "is_from_seller": is_from_seller,
        })

    return messages


# ------------------------------------------------------------
# メッセージ送信（/messages画面からの実送信）
# ------------------------------------------------------------
# 本文入力欄・送信ボタンは実機DOM調査で特定済み（推測ではない）。
#   本文入力欄: [data-testid="transaction:chat-textarea"] 配下の textarea[name="chat"]
#   送信ボタン: [data-partner-id="send-chat"] 配下の button[type="submit"]（文言「取引メッセージを送る」）
# スタンプ機能（[data-testid="stamp-popup-trigger"]）・定型文チップ
# （[data-testid="message-template-chip"]、例:「購入後のあいさつをする」）とは
# data-testid/data-partner-idが完全に別で、本文入力欄・送信ボタンには一切触れない。
CHAT_TEXTAREA_SELECTOR = '[data-testid="transaction:chat-textarea"] textarea[name="chat"]'
CHAT_SEND_BUTTON_SELECTOR = '[data-partner-id="send-chat"] button[type="submit"]'

MERCARI_SEND_RESPONSE_WAIT_SEC = 10.0


def mercari_send_chat_message(driver, order_id: str, expected_count: int, reply_text: str,
                               expected_last_message_id=None) -> dict:
    """
    メルカリの取引ページへ実際にメッセージを送信する（誤送信防止のため必ずこの手順で行う）。

    【2026-08-30 message_id必須化に伴う不具合修正】旧実装は新着確認の基準として
    expected_last_message_id（/messages画面表示時点の最新message_id）を必須にしていたため、
    会話履歴が0件（まだ一度もメッセージが無い取引に初めて送る）場合にmessages_blueprint.py側で
    「message_idが必要です」となり送信できない不具合があった（実例: m18387945456）。
    これはPayPayフリマの「message_noが必要です」不具合と同根の問題のため、
    3サイト共通の新着確認基準として expected_count（0以上の整数。メッセージ無しなら0）を
    導入し、この関数もexpected_countを主基準にした。メルカリはexpected_count>0の場合のみ、
    従来のmessage_id一致確認を追加の安全確認として維持する（expected_last_message_id、
    件数が一致していても万一メッセージの入れ替わりがあった場合に検知するため）。
    expected_count>0なのにexpected_last_message_idが渡されなかった場合は、安全のため
    送信せずエラーを返す（既存メッセージがあるのにmessage_idで確認できない状態を許可しない）。

    【2026-08-28 実機調査により全面改修】
    旧実装は「クリックが例外を投げなかったこと」のみを成功条件にしていたが、実機調査で
    以下が判明したため、入力・クリック・成功判定のすべてを実際の通信結果ベースに変更した。

    - Seleniumのクリック座標がこの環境（debugger_addressで外部起動Chromeにアタッチ）では
      実際の描画座標とズレており、textarea/送信ボタンへの.click()が document.elementFromPoint()
      で確認すると何にも命中していなかった（document.activeElementが常にBODYのまま）。
        → JSの.focus()で確実にフォーカスし、ボタンクリックもSelenium座標クリックではなく
          DOM直接の .click()（execute_scriptでelement.click()を呼ぶ）に変更した。
    - textarea.send_keys()や旧来のnative setter単体では、ReactのvalueTracker（Reactが
      「本当に変化したか」を内部で照合する仕組み）を正しく更新できず、送信ボタンが
      disabledのまま変わらないことがあった（クリックしても何も起きない）。
        → CDPの Input.insertText（実際のトラステッド入力として扱われる）で入力することで、
          Reactの内部stateも含めて確実に更新され、送信ボタンが有効化されることを実機確認済み。

    手順:
      1. 取引ページを開き、現在のメッセージ履歴をAPI経由で取得する
      2. /messages画面表示時点の件数(expected_count)と実際の件数を比較し、新しい
         メッセージが増えていないか確認する（増えていれば送信せず中止）。expected_count>0の
         場合はさらに、実際の最新message_idがexpected_last_message_idと一致するかも確認する
         （件数が同じでも内容が入れ替わっている可能性を検知する追加の安全確認）。
      3. 対象タブのCDP WebSocketに直接接続しNetwork.enableする（Seleniumのdriverとは
         別の監視専用コネクション。Selenium側の操作には影響しない）
      4. Page.bringToFront → JS focus() → CDP Input.insertText で本文を入力し、valueの
         読み返しと送信ボタンのdisabled解除を確認する
      5. 送信ボタンをDOM直接clickで「1回だけ」クリックする（このスクリプト内で再クリックは
         一切行わない。失敗時も自動リトライしない＝二重送信防止を最優先する）
      6. クリック後に発生する実際の transaction_messages/post のレスポンス（HTTPステータス・
         本文）をCDP経由で捕捉し、result=="OK" かつ本物の message_id (data.id) が
         返ってきたことをもって初めて成功と判定する（クリックの成否では判定しない）

    戻り値: {
        "ok": bool, "error": str|None,
        "message_id": int|None,       # メルカリが実際に採番した本物の message_id（成功時のみ）
        "message_no": int|None,       # 送信時点の会話内での位置（送信前のcurrent_messages件数+1）
        "message_datetime": datetime|None,  # レスポンスのcreated(unix time)をJSTに変換したもの
    }
    """
    driver.get(f"https://jp.mercari.com/transaction/{order_id}")
    time.sleep(4)

    current_messages = mercari_get_messages(driver, order_id)
    if len(current_messages) != expected_count:
        # 送信は中止するが、ここで既に取得できているmercari_get_messages()の結果
        # （通常scrapeと全く同じ形式・Mercari APIの正規データ）を呼び出し元へ渡す。
        # 呼び出し元(messages_blueprint.py)がこれを使ってtrx.vendor_messageへ保存し、
        # 画面を最新化する（わざわざ再度APIを呼び直したり通常scrapeを起動したりしない）。
        return {"ok": False, "error": "新しいメッセージを受信したため送信を中止しました",
                "reason": "new_message_detected", "new_messages": current_messages,
                "message_id": None, "message_no": None, "message_datetime": None}

    if expected_count > 0:
        # 既存メッセージがある場合のみ、従来のmessage_id一致確認を追加の安全確認として行う
        # （件数が一致していても、万一メッセージが入れ替わっているケースを検知するため）。
        if expected_last_message_id is None:
            return {"ok": False, "error": "既存メッセージがあるため、message_idによる追加確認が必要です",
                    "message_id": None, "message_no": None, "message_datetime": None}
        current_last_id = current_messages[-1]["message_id"]
        if current_last_id != expected_last_message_id:
            return {"ok": False, "error": "新しいメッセージを受信したため送信を中止しました",
                    "reason": "new_message_detected", "new_messages": current_messages,
                    "message_id": None, "message_no": None, "message_datetime": None}

    next_message_no = len(current_messages) + 1

    textarea_els = driver.find_elements(By.CSS_SELECTOR, CHAT_TEXTAREA_SELECTOR)
    if not textarea_els:
        return {"ok": False, "error": "本文入力欄が見つかりません",
                "message_id": None, "message_no": None, "message_datetime": None}
    textarea = textarea_els[0]

    import websocket as _ws_client  # ローカルimport: 送信結果の監視専用のため使用箇所を限定する

    ws_url = _get_ws_debugger_url(MERCARI_DEBUG_PORT, f"transaction/{order_id}")
    ws = _ws_client.create_connection(ws_url, timeout=MERCARI_SEND_RESPONSE_WAIT_SEC, suppress_origin=True)
    try:
        next_id = [0]

        def send(method, params=None):
            next_id[0] += 1
            ws.send(json.dumps({"id": next_id[0], "method": method, "params": params or {}}))
            return next_id[0]

        send("Network.enable")

        # --- 入力（実機検証済みの手順） ---
        driver.execute_cdp_cmd("Page.bringToFront", {})
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", textarea)
        time.sleep(0.2)
        driver.execute_script("arguments[0].focus();", textarea)
        time.sleep(0.2)

        driver.execute_cdp_cmd("Input.insertText", {"text": reply_text})
        time.sleep(0.6)

        actual_value = textarea.get_attribute("value")
        if actual_value != reply_text:
            return {"ok": False, "error": f"本文入力欄への入力を確認できませんでした（value={actual_value!r}）",
                    "message_id": None, "message_no": None, "message_datetime": None}

        send_button_els = driver.find_elements(By.CSS_SELECTOR, CHAT_SEND_BUTTON_SELECTOR)
        if not send_button_els:
            return {"ok": False, "error": "送信ボタンが見つかりません",
                    "message_id": None, "message_no": None, "message_datetime": None}
        send_button = send_button_els[0]

        if send_button.get_attribute("disabled") is not None:
            return {"ok": False, "error": "送信ボタンが無効化されたままでした（入力内容が反映されていない可能性があります）",
                    "message_id": None, "message_no": None, "message_datetime": None}

        # --- クリックは以降この1回のみ。失敗しても自動リトライしない（二重送信防止） ---
        driver.execute_script("arguments[0].click();", send_button)

        # --- 実際の送信レスポンスを捕捉する ---
        # 【2026-08-28 実機不具合を受けて全面改修】
        # 旧実装は2つの不具合を持っていた。
        #   (1) Network.responseReceivedのURL一致だけでrequest_idを採用していたため、
        #       同じURLに先行して発生するCORSプリフライト(OPTIONS)を本物のPOSTと
        #       誤認することがあった（Authorization/DPoPヘッダーを持つクロスオリジン
        #       リクエストは仕様上プリフライトが必須で、そのレスポンスは通常200/204・
        #       ボディ無しのため「status=200, body=None」という紛らわしい誤判定を生む）。
        #   (2) 特定requestIdのgetResponseBody応答を待つ内側のブロッキングwhileループが
        #       ws.recv()で受信した他の全イベントを読み捨てていたため、その間に届いた
        #       本物のPOSTのresponseReceived/loadingFinishedが失われ、二度と検知できなく
        #       なっていた（実際にはメルカリへの送信自体は成功していたにもかかわらず、
        #       このスクリプトだけが「送信できなかった」と誤判定する原因になった）。
        #
        # 対策:
        #   - Network.requestWillBeSent で method=="POST" のリクエストのみを対象の
        #     request_idとして採用する（OPTIONSは最初から候補にしない）。
        #   - 内側の別ループを廃止し、real_send_once.py で実際に動作確認済みの
        #     「1本の継続的な受信ループで全イベントを順番に処理する」方式に統一した
        #     （途中で他のイベントを読み捨てる経路を無くした）。
        target_request_id = None
        status = None
        get_body_id = None
        body = None

        deadline = time.time() + MERCARI_SEND_RESPONSE_WAIT_SEC
        while time.time() < deadline and body is None:
            ws.settimeout(max(0.1, deadline - time.time()))
            try:
                raw = ws.recv()
            except Exception:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            method = data.get("method")

            if method == "Network.requestWillBeSent" and target_request_id is None:
                req = data["params"].get("request", {})
                if req.get("method") == "POST" and MERCARI_SEND_MESSAGE_API_URL_SUBSTR in req.get("url", ""):
                    target_request_id = data["params"]["requestId"]

            elif method == "Network.responseReceived" and data["params"].get("requestId") == target_request_id:
                status = data["params"]["response"]["status"]

            elif (method == "Network.loadingFinished"
                  and data["params"].get("requestId") == target_request_id
                  and get_body_id is None):
                get_body_id = send("Network.getResponseBody", {"requestId": target_request_id})

            elif method is None and get_body_id is not None and data.get("id") == get_body_id:
                if "result" in data:
                    body = json.loads(data["result"]["body"])
                # "error"の場合は本物のPOST自体のボディ取得に失敗しているため、
                # 無理に別のイベントを本物とみなしたりせず、bodyはNoneのままタイムアウトさせる
                # （＝status!=200やbody=Noneの通常の失敗判定に委ねる。取りこぼしは発生しない）。
    finally:
        ws.close()

    if target_request_id is None:
        return {"ok": False, "error": "送信リクエスト（POST transaction_messages/post）の発生を確認できませんでした"
                                       "（クリックが反映されていない可能性があります）。再送信はせず、必ず状況を確認してください。",
                "message_id": None, "message_no": None, "message_datetime": None}

    if status != 200 or not body or body.get("result") != "OK" or not body.get("data", {}).get("id"):
        return {"ok": False, "error": f"メルカリ側の実レスポンスでresult==\"OK\"かつ本物のmessage_idを確認できませんでした"
                                       f"（status={status}, body={body}）",
                "message_id": None, "message_no": None, "message_datetime": None}

    data_obj = body.get("data", {})
    real_message_id = data_obj.get("id")
    created = data_obj.get("created")
    message_datetime = datetime.fromtimestamp(created, tz=JST).replace(tzinfo=None) if created else None

    return {"ok": True, "error": None,
            "message_id": real_message_id, "message_no": next_message_no,
            "message_datetime": message_datetime}


def determine_status(raw_status, messages):
    """発送前の場合のみ出品者メッセージの有無で購入済/連絡あり に分岐する"""
    if raw_status != "発送前":
        return raw_status

    if any(msg["is_from_seller"] for msg in messages):
        return "連絡あり"

    return "【購入済】"


MERCARI_TRACKING_NUMBER_RE = re.compile(r"\d{10,14}")

# 送り状番号の直下に表示される文言で配送会社を判定する（番号の桁数・書式からは判定しない）
MERCARI_TRACKING_CARRIER_MARKERS = (
    ("ヤマト運輸", "ヤマト"),
    ("日本郵便", "日本郵便"),
)

# 「あんしん鑑定」（第三者鑑定）対象商品は、通常のメルカリ便と異なり
# 出品者 → 鑑定事業者 → 購入者 の2段階配送になり、それぞれ別の送り状番号を持つ
# （実機確認済み。ページ上の表示例）:
#   出品者送り状番号 : 626846983346
#   事業者送り状番号 : まだ発行されていません
# または
#   出品者送り状番号 : 622993517543
#   事業者送り状番号 : 390902831594
# 「出品者送り状番号」は出品者→鑑定事業者間の番号で、購入者への配送状況とは無関係。
# 誤って購入者への配送追跡に使わないよう、あんしん鑑定対象と判定した場合は
# 「事業者送り状番号」（鑑定事業者→購入者間の番号）のみを送り状番号として扱う。
KANTEI_BUSINESS_LABEL = "事業者送り状番号"
KANTEI_BUSINESS_NOT_ISSUED = "まだ発行されていません"


def mercari_get_tracking_info(driver):
    """
    取引ページの「送り状番号」表示から番号と配送会社を取得する。
    未発送・あんしん鑑定で事業者送り状番号が未発行の場合などは (None, None) を返す（異常ではない）。
    """
    body_text = driver.find_element(By.TAG_NAME, "body").text

    business_idx = body_text.find(KANTEI_BUSINESS_LABEL)
    if business_idx != -1:
        # あんしん鑑定対象。「事業者送り状番号」（鑑定事業者→購入者）だけを見る。
        # 「出品者送り状番号」（出品者→鑑定事業者）は日常への保存にも到着判定にも使わない。
        window = body_text[business_idx: business_idx + 200]
        if KANTEI_BUSINESS_NOT_ISSUED in body_text[business_idx: business_idx + 40]:
            return None, None

        m = MERCARI_TRACKING_NUMBER_RE.search(window)
        if not m:
            return None, None
        tracking_number = m.group(0)

        for marker, carrier in MERCARI_TRACKING_CARRIER_MARKERS:
            if marker in window:
                return tracking_number, carrier
        return tracking_number, None

    idx = body_text.find("送り状番号")
    if idx == -1:
        return None, None

    window = body_text[idx: idx + 200]
    m = MERCARI_TRACKING_NUMBER_RE.search(window)
    if not m:
        return None, None
    tracking_number = m.group(0)

    for marker, carrier in MERCARI_TRACKING_CARRIER_MARKERS:
        if marker in window:
            return tracking_number, carrier

    return tracking_number, None


# ------------------------------------------------------------
# メイン
# ------------------------------------------------------------
def mercari_main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--item-ids", nargs="*", default=None, help="注文ID(取引URL末尾)で対象を絞り込む（テスト用）")
    args = parser.parse_args()
    wanted_ids = set(args.item_ids) if args.item_ids else None

    mercari_ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{MERCARI_DEBUG_PORT}"

    driver = webdriver.Chrome(options=options)
    conn = get_sql_server_connection()
    access_conn = get_access_connection()

    tab_id = None
    try:
        # 処理用タブをbackgroundで作成する（Chromeウィンドウを前面化しない。
        # 実機確認済み）。既存タブは一切操作しない。
        tab_id = _create_processing_tab(driver)

        driver.get(MERCARI_TARGET_URL)
        time.sleep(5)

        current_url = _get_current_url_with_retry(driver)
        page_title = driver.title
        print(f"URL: {current_url}")
        print(f"Title: {page_title}")

        if "login" in current_url or "sign_in" in current_url or "signin" in current_url:
            print("NG: ログインされていない可能性あり")
            return

        try:
            _ensure_in_transaction_checkbox_checked(driver)
        except Exception as e:
            print(f"NG: 「取引中の商品」チェックボックスの操作に失敗しました: {e}")
            print("NG: 取得対象が確定できないため、今回の処理を中断します。")
            return

        if wanted_ids is not None:
            # 指定IDは「取引中の商品」一覧に既に出てこない（評価済み等で外れた）ことがあるため、
            # 一覧経由ではなく取引URLを直接組み立てる（テスト用の絞り込み時のみ）。
            transaction_urls = [f"https://jp.mercari.com/transaction/{iid}" for iid in wanted_ids]
        else:
            transaction_urls = _collect_transaction_urls(driver)

        print(f"取引URL数: {len(transaction_urls)}")
        print()

        for url in transaction_urls:

            try:

                driver.get(url)
                _wait_for_transaction_page_ready(driver)

                vendor_item_id   = get_vendor_item_id(url)
                raw_status       = mercari_get_raw_status(driver)
                item_name        = get_item_name(driver)
                purchase_datetime, purchase_price = get_purchase_info(driver)
                messages         = mercari_get_messages(driver, vendor_item_id)
                status           = determine_status(raw_status, messages)
                tracking_number, carrier = mercari_get_tracking_info(driver)

                with conn.cursor() as cur:
                    cur.execute(
                        MERCARI_SQL_UPSERT_VENDOR_PURCHASE,
                        (MERCARI_VENDOR_NAME, vendor_item_id, purchase_datetime, purchase_price, status, item_name)
                    )
                    for msg in messages:
                        cur.execute(
                            MERCARI_SQL_UPSERT_VENDOR_MESSAGE_BY_ID,
                            (
                                MERCARI_VENDOR_NAME,
                                vendor_item_id,
                                msg["message_id"],
                                msg["message_no"],
                                msg["sender_name"],
                                "出品者" if msg["is_from_seller"] else "購入者",
                                msg["message_datetime"],
                                msg["message_body"],
                            )
                        )
                conn.commit()

                # 日常に注文IDのレコードが無い場合（仕入入力忘れ／私用購入で未入力）は新規追加する。
                # 既存レコードがある場合は何もしない（以降の更新処理がそのまま担当する）。
                created = ensure_daily_record(
                    access_conn, MERCARI_VENDOR_NAME, vendor_item_id,
                    item_name, purchase_datetime.date(), purchase_price
                )

                # 送り状番号・配送会社は日常テーブルへ直接保存する（trx.vendor_purchaseは中間テーブルのため経由しない）。
                update_daily_tracking_info(access_conn, vendor_item_id, tracking_number, carrier)

                print(url)
                print(f"status={status}  price={purchase_price}  messages={len(messages)}  item={item_name[:30]}")
                if created:
                    print(f"日常: 新規レコード追加（注文ID={vendor_item_id}）")
                print()

            except Exception as e:
                print(url)
                print(f"ERROR: {e}")
                print()

        sync_carrier_tracking_to_daily(access_conn)
        sync_arrival_status_to_access(conn)
        sync_unregistered_daily_items(conn)

    finally:
        access_conn.close()
        conn.close()
        # 処理用タブは成功・失敗にかかわらず必ず閉じる（ユーザーの既存タブには触れない）。
        _close_processing_tab(driver, tab_id)
        # options.debugger_addressで常駐Chromeへ外部接続しているだけなので、
        # driver.quit()しても常駐Chrome本体・既存タブは終了しない（実機確認済み）。
        # 今回のSeleniumセッション（chromedriver.exeプロセス）だけを終了し、
        # 毎回実行するたびにchromedriver.exeが残留し続けるのを防ぐ。
        driver.quit()


# ============================================================================
# ============================================================================
# PayPayフリマ固有（旧 apps/etc/yahoo_furima_purchase.py）
# 同名衝突があった識別子は paypay_ / PAYPAY_ 接頭辞で区別している。
# ============================================================================
# ============================================================================
PAYPAY_VENDOR_NAME = "ＰａｙＰａｙフリマ"

PAYPAY_CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PAYPAY_PROFILE_DIR = r"D:\apps_nostock\selenium_profile"
PAYPAY_DEBUG_PORT = 9223
PAYPAY_LAUNCH_TIMEOUT_SEC = 30

PURCHASE_LIST_URL = "https://paypayfleamarket.yahoo.co.jp/my/purchase"

PAYPAY_ORDER_ID_RE = re.compile(r"/item/([A-Za-z0-9]+)/trade/buyer")

# 一覧に表示されるステータス文言（実機確認済み）
STATUS_COMPLETED = "取引完了"
STATUS_BEFORE_SHIP = "発送待ち"

# 個別取引ページ（詳細）で判定する。一覧の文言は「商品が到着したら評価をしてください」
# 「未評価の場合は評価してください」など複数のバリエーションがあり一覧文言だけでは
# 判定しきれないことを実機確認したため、詳細ページの共通見出しで判定する。
DETAIL_ARRIVED_MARKER = "受取評価をして取引を完了してください"
DETAIL_BEFORE_SHIP_MARKER = "出品者の発送をお待ちください"

CARRIER_BY_HOST = (
    ("kuronekoyamato.co.jp", "ヤマト"),
    ("post.japanpost.jp", "日本郵便"),
)

# 相対日時表記（例: 「12時間前」「2日前」）を見つけるための正規表現
# 「○秒前」は送信直後に実機で確認済み（送信直後は「35秒前」のように表示され、
# 分前に切り替わるまでの間、この形式に非対応だとpaypay_get_messages()が検出できなかった）。
RELATIVE_TIME_RE = re.compile(r"^(たった今|\d+秒前|\d+分前|\d+時間前|\d+日前|\d+週間前|\d+ヶ月前|\d+年前)$")


# ------------------------------------------------------------
# Chrome起動・タブ管理（mercari_ensure_chrome_debugger()と同じ仕組み）
# ------------------------------------------------------------
def _paypay_debugger_alive(port: int) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


def paypay_ensure_chrome_debugger(port: int = PAYPAY_DEBUG_PORT, profile_dir: str = PAYPAY_PROFILE_DIR,
                                   timeout: int = PAYPAY_LAUNCH_TIMEOUT_SEC) -> None:
    if _paypay_debugger_alive(port):
        print(f"OK: 起動済みのChrome(ポート{port})を利用します")
        return

    print(f"Chromeをリモートデバッグモードで起動します（ポート{port}）...")
    subprocess.Popen([
        PAYPAY_CHROME_EXE,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
    ])

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _paypay_debugger_alive(port):
            print("OK: Chrome起動完了")
            return
        time.sleep(0.5)

    raise RuntimeError(f"Chromeの起動確認がタイムアウトしました（{timeout}秒）")


# ------------------------------------------------------------
# 購入一覧
# ------------------------------------------------------------
def collect_active_transactions(driver, retries: int = 6, interval: float = 2.0):
    """
    最初に表示されている範囲（「もっと見る」は押さない）から、
    ステータスが「取引完了」以外の取引URL・生ステータス文言の一覧を返す。
    戻り値: [(url, list_status_text), ...]

    【2026-08-31 メルカリの実機不具合を受けて共通の考え方を適用】retries回待っても
    フィルタ前の生リンクが1件も見つからない場合は、0件と決めつけず例外を送出する
    （一覧ページの描画・読み込みに失敗している可能性があるため）。「取引完了」の
    フィルタで結果的に0件になるのは正常な状態のため区別する（その場合は生リンクは
    見つかっている）。
    """
    driver.get(PURCHASE_LIST_URL)

    for attempt in range(1, retries + 1):
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/trade/buyer']")
        if links or attempt == retries:
            break
        time.sleep(interval)

    if not links:
        raise RuntimeError(
            f"購入した商品の一覧が読み込めませんでした（{retries * interval:.0f}秒待っても"
            "取引リンクが1件も見つかりません）。一覧ページの描画に失敗している可能性が"
            "あるため、0件と決めつけず処理を中断します。"
        )

    results = []
    seen = set()
    for link in links:
        href = link.get_attribute("href")
        text = link.text or ""
        if not href or href in seen:
            continue
        if STATUS_COMPLETED in text:
            continue  # 取引完了は対象外
        seen.add(href)
        results.append((href, text.strip()))

    return results


def paypay_get_order_id(url: str):
    m = PAYPAY_ORDER_ID_RE.search(url)
    return m.group(1) if m else None


def paypay_get_raw_status(driver) -> str:
    """
    個別取引ページの文言から、共通の raw_status 語彙
    （"発送前" / "発送済み" / "☆出荷可能"）へ変換する。
    一覧の文言だけでは「商品が到着したら評価をしてください」
    「未評価の場合は評価してください」のように表記が複数あり判定しきれないため、
    詳細ページの共通見出しで判定する（実機確認済み）。

    「発送済みであることを確認できた場合だけ発送済みとする」を原則とし、
    既知のいずれの文言にも一致しない場合は自動返信を誤らせないよう例外を送出する
    （mercari_get_raw_status()と同じ方針）。
    【2026-08-29修正】旧実装はいずれにも一致しない場合に無条件で"発送済み"を返して
    おり、ラクマで同種のパターンが実際に誤判定を起こしたことを受けて廃止した。
    「発送済み・配送中」の実機未確認の文言を新たに推測して追加することはしない。
    """
    body_text = driver.find_element(By.TAG_NAME, "body").text
    if DETAIL_ARRIVED_MARKER in body_text:
        return "☆出荷可能"
    if DETAIL_BEFORE_SHIP_MARKER in body_text:
        return "発送前"

    raise RuntimeError(
        "配送状況を判定できませんでした。未確認の状態のため、"
        "誤って発送済み扱いにしないよう処理を中断します。"
    )


# ------------------------------------------------------------
# 個別取引ページ
# ------------------------------------------------------------
def paypay_get_tracking_info(driver):
    """
    送り状番号のリンク(<a href>)のホスト名から配送会社を判定する
    （文言推測ではなくリンク先で判定。実機確認済み）。
    見つからない場合は (None, None)。
    """
    for host, carrier in CARRIER_BY_HOST:
        els = driver.find_elements(By.CSS_SELECTOR, f"a[href*='{host}']")
        if els:
            tracking_number = els[0].text.strip()
            if tracking_number:
                return tracking_number, carrier
    return None, None


def paypay_get_seller_name(driver):
    """出品者名は安定したクラス名 .UserInfo__Name で取得できる（実機確認済み）。"""
    els = driver.find_elements(By.CSS_SELECTOR, ".UserInfo__Name")
    return els[0].text.strip() if els else None


def paypay_get_item_name(driver):
    """
    商品情報カードへのリンク(a[href*='paypayfleamarket.yahoo.co.jp/item/'])内、
    先頭の<p>が商品名（実機確認済み。class名はハッシュ化されているためhref側で判定）。
    """
    els = driver.find_elements(By.CSS_SELECTOR, "a[href*='paypayfleamarket.yahoo.co.jp/item/'] p")
    if not els:
        raise RuntimeError("商品名要素が見つかりません (a[href*='paypayfleamarket.yahoo.co.jp/item/'] p)")
    return els[0].text.strip()


def paypay_get_purchase_price(driver):
    """商品情報カード内の<p>のうち「円」を含むものが購入金額（実機確認済み）。"""
    els = driver.find_elements(By.CSS_SELECTOR, "a[href*='paypayfleamarket.yahoo.co.jp/item/'] p")
    for el in els:
        text = el.text.strip()
        if "円" in text:
            digits = re.sub(r"[^\d]", "", text)
            if digits.isdigit():
                return int(digits)
    raise RuntimeError(
        "購入金額要素が見つかりません (a[href*='paypayfleamarket.yahoo.co.jp/item/'] 内に「円」を含む<p>なし)"
    )


def paypay_get_purchase_date(driver):
    """
    「購入日時」ラベル<span>の祖先<li>内の<p>が値（実機確認済み）。
    Mercariと同じparse_japanese_datetime()で解析する（表記形式が共通のため）。
    """
    els = driver.find_elements(By.XPATH, "//span[text()='購入日時']/ancestor::li[1]//p")
    if not els:
        raise RuntimeError("購入日時要素が見つかりません (span[text()='購入日時']/ancestor::li[1]//p)")
    return parse_japanese_datetime(els[0].text.strip())


def paypay_get_messages(driver, seller_name: str):
    """
    取引メッセージを発言順に取得する。日時は「12時間前」等の相対表記のみのため、
    無理に絶対日時へ変換せず、表示されている文字列をそのまま保存する。
    sender_nameが出品者名と一致するものを「出品者」、それ以外を「自分」として扱う。
    """
    time_els = driver.find_elements(By.XPATH, "//span[contains(text(), '前')]")

    messages = []
    no = 0
    for el in time_els:
        text = el.text.strip()
        if not RELATIVE_TIME_RE.match(text):
            continue

        container = driver.execute_script(
            "return arguments[0].closest('li');", el
        )
        if container is None:
            continue
        full_text = container.text.strip()
        lines = [l for l in full_text.split("\n") if l.strip()]
        if len(lines) < 3:
            continue

        sender_name = lines[0].strip()
        message_datetime_text = lines[-1].strip()
        message_body = "\n".join(lines[1:-1]).strip()

        no += 1
        is_seller = (sender_name == seller_name)
        messages.append({
            "message_no": no,
            "sender_name": sender_name if is_seller else "自分",
            "sender_type": "出品者" if is_seller else "購入者",
            "message_datetime_text": message_datetime_text,
            "message_body": message_body,
        })

    return messages


# ------------------------------------------------------------
# メッセージ送信（/messages画面からの実送信）
# ------------------------------------------------------------
# 本文入力欄・送信ボタンは実機DOM調査で特定済み（推測ではない）。
#   本文入力欄: <textarea placeholder="メッセージを入力">（React管理下、name/id無し）
#   送信ボタン: 文言「取引メッセージを送る」の<button type="button">（本文が空だとdisabled）
# フォーム(<form>)は存在せず、送信ボタンクリック位置に対するdocument.elementFromPoint()も
# メルカリ同様Noneになる（座標系のズレが同じく発生する）ため、Selenium座標クリックではなく
# DOM直接clickを使う。
PAYPAY_CHAT_SEND_BUTTON_TEXT = "取引メッセージを送る"

PAYPAY_SEND_RESPONSE_WAIT_SEC = 15.0


def paypay_send_chat_message(driver, order_id: str, expected_count: int, reply_text: str) -> dict:
    """
    PayPayフリマの取引ページへ実際にメッセージを送信する（誤送信防止のため必ずこの手順で行う）。

    【2026-08-30 3サイト共通のexpected_count方式に統一】新着確認基準の名称を
    expected_last_message_no から expected_count（0以上の整数）へ変更した。
    比較ロジック自体（件数比較）は変更していない。会話履歴が0件の場合も
    expected_count=0として正常に送信できる（メルカリ・ラクマと共通の考え方）。

    実機調査済み（2026-08-29、実送信1回で確認）:
      送信通信: POST https://paypayfleamarket-sec.yahoo.co.jp/api/v2/items/{order_id}/message
      同一オリジンのためCORSプリフライトは発生しない（メルカリで問題になったOPTIONS誤認識の
      心配は無いが、念のためメルカリと同じ「1本の継続的な受信ループ」構造で捕捉する）。
      レスポンスにメッセージ固有の一意ID（message_id相当）は含まれず、
      {"thread": [{"text":..., "date":..., "userId":...}, ...]} という会話全文のみが返る。
      そのためtrx.vendor_messageへの保存はmessage_id方式ではなく、既存のsave_vendor_messages()
      （(vendor_name, vendor_item_id, message_no)キーのMERGE）をそのまま使う。送信成功後に
      paypay_get_messages()を再実行し、送信分を含む最新の全件をそのまま渡せば、通常scrapeと
      全く同じ経路で重複なく保存できる。

    手順:
      1. 取引ページを開き、現在のメッセージ件数をpaypay_get_messages()で取得する
      2. /messages画面表示時点の件数(expected_count)と比較し、新しいメッセージが
         増えていないか確認する（増えていれば送信せず中止し、取得済みのpaypay_get_messages()
         結果をそのまま呼び出し元へ渡す＝呼び出し元がDB保存・画面更新・返信判定の再実行に使う）
      3. Page.bringToFront → JS focus() → CDP Input.insertText で本文を入力し、valueの
         読み返しと送信ボタンのdisabled解除を確認する
      4. 送信ボタンをDOM直接clickで「1回だけ」クリックする（このスクリプト内で再クリックは
         一切行わない。失敗時も自動リトライしない＝二重送信防止を最優先する）
      5. 実際のPOSTレスポンス（HTTPステータス・本文）をCDP経由で捕捉し、status==200 かつ
         本文に thread 配列が含まれることをもって初めて成功と判定する
         （クリックの成否・HTTP 200単体では判定しない）
      6. 成功していれば、paypay_get_messages()を再度呼び直し、送信したメッセージを含む
         最新の全件を戻り値として返す

    戻り値: {
        "ok": bool, "error": str|None,
        "reason": "new_message_detected"|None,
        "new_messages": list|None,  # 成功時・新着検出時とも、paypay_get_messages()の戻り値そのもの
    }
    """
    url = f"https://paypayfleamarket-sec.yahoo.co.jp/item/{order_id}/trade/buyer"
    driver.get(url)
    time.sleep(4)

    seller_name = paypay_get_seller_name(driver)
    current_messages = paypay_get_messages(driver, seller_name)
    if len(current_messages) != expected_count:
        return {"ok": False, "error": "新しいメッセージを受信したため送信を中止しました",
                "reason": "new_message_detected", "new_messages": current_messages}

    # 取引によっては評価コメント欄（placeholder="（必須）コメントを入力してください」等）が
    # 取引メッセージ欄より先にDOM上へ現れることが実機で確認された（例: z669802644）。
    # 先頭のtextareaを無条件に使うと評価コメント欄を誤って選んでしまうため、
    # 取引メッセージ欄のplaceholderで明示的に絞り込む。
    textarea_els = driver.find_elements(By.CSS_SELECTOR, "textarea[placeholder='メッセージを入力']")
    if not textarea_els:
        return {"ok": False, "error": "本文入力欄が見つかりません", "reason": None, "new_messages": None}
    textarea = textarea_els[0]

    send_button = None
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        if btn.text.strip() == PAYPAY_CHAT_SEND_BUTTON_TEXT:
            send_button = btn
            break
    if send_button is None:
        return {"ok": False, "error": "送信ボタンが見つかりません", "reason": None, "new_messages": None}

    import json as _json
    import websocket as _ws_client  # ローカルimport: 送信結果の監視専用のため使用箇所を限定する

    with urllib.request.urlopen(f"http://127.0.0.1:{PAYPAY_DEBUG_PORT}/json/list") as resp:
        targets = _json.loads(resp.read().decode("utf-8"))
    target = next((t for t in targets if order_id in t.get("url", "")), None)
    if target is None:
        return {"ok": False, "error": "CDPターゲットが見つかりません", "reason": None, "new_messages": None}

    ws = _ws_client.create_connection(target["webSocketDebuggerUrl"], timeout=PAYPAY_SEND_RESPONSE_WAIT_SEC, suppress_origin=True)
    try:
        next_id = [0]

        def send_cdp(method, params=None):
            next_id[0] += 1
            ws.send(_json.dumps({"id": next_id[0], "method": method, "params": params or {}}))
            return next_id[0]

        send_cdp("Network.enable")

        driver.execute_cdp_cmd("Page.bringToFront", {})
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", textarea)
        time.sleep(0.2)
        driver.execute_script("arguments[0].focus();", textarea)
        time.sleep(0.2)

        driver.execute_cdp_cmd("Input.insertText", {"text": reply_text})
        time.sleep(0.6)

        actual_value = textarea.get_attribute("value")
        if actual_value != reply_text:
            return {"ok": False, "error": f"本文入力欄への入力を確認できませんでした（value={actual_value!r}）",
                    "reason": None, "new_messages": None}

        if send_button.get_attribute("disabled") is not None:
            return {"ok": False, "error": "送信ボタンが無効化されたままでした（入力内容が反映されていない可能性があります）",
                    "reason": None, "new_messages": None}

        # --- クリックは以降この1回のみ。失敗しても自動リトライしない（二重送信防止） ---
        driver.execute_script("arguments[0].click();", send_button)

        send_api_substr = f"/api/v2/items/{order_id}/message"
        target_request_id = None
        status = None
        get_body_id = None
        body = None

        deadline = time.time() + PAYPAY_SEND_RESPONSE_WAIT_SEC
        while time.time() < deadline and body is None:
            ws.settimeout(max(0.1, deadline - time.time()))
            try:
                raw = ws.recv()
            except Exception:
                continue
            try:
                data = _json.loads(raw)
            except Exception:
                continue

            method = data.get("method")

            if method == "Network.requestWillBeSent" and target_request_id is None:
                req = data["params"].get("request", {})
                if req.get("method") == "POST" and send_api_substr in req.get("url", ""):
                    target_request_id = data["params"]["requestId"]

            elif method == "Network.responseReceived" and data["params"].get("requestId") == target_request_id:
                status = data["params"]["response"]["status"]

            elif (method == "Network.loadingFinished"
                  and data["params"].get("requestId") == target_request_id
                  and get_body_id is None):
                get_body_id = send_cdp("Network.getResponseBody", {"requestId": target_request_id})

            elif method is None and get_body_id is not None and data.get("id") == get_body_id:
                if "result" in data:
                    body = _json.loads(data["result"]["body"])
    finally:
        ws.close()

    if target_request_id is None:
        return {"ok": False, "error": "送信リクエスト（POST .../message）の発生を確認できませんでした"
                                       "（クリックが反映されていない可能性があります）。再送信はせず、必ず状況を確認してください。",
                "reason": None, "new_messages": None}

    if status != 200 or not body or "thread" not in body:
        return {"ok": False, "error": f"PayPayフリマ側の実レスポンスで送信成功を確認できませんでした"
                                       f"（status={status}, body={body}）",
                "reason": None, "new_messages": None}

    # 実送信成功。paypay_get_messages()を再実行し、送信分を含む最新の全件を返す
    # （通常scrapeと同じsave_vendor_messages()経路でDB保存できるようにするため）。
    time.sleep(1.0)
    fresh_messages = paypay_get_messages(driver, seller_name)

    return {"ok": True, "error": None, "reason": None, "new_messages": fresh_messages}


# ------------------------------------------------------------
# メイン
# ------------------------------------------------------------
def paypay_main():
    paypay_ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{PAYPAY_DEBUG_PORT}"

    driver = webdriver.Chrome(options=options)
    sql_conn = get_sql_server_connection()
    access_conn = get_access_connection()

    tab_id = None
    try:
        # 処理用タブをbackgroundで作成する（Chromeウィンドウを前面化しない。
        # 実機確認済み）。既存タブは一切操作しない。
        tab_id = _create_processing_tab(driver)

        transactions = collect_active_transactions(driver)
        print(f"取引URL数(取引完了を除く): {len(transactions)}")
        print()

        for url, list_status_text in transactions:
            try:
                driver.get(url)
                time.sleep(4)

                order_id = paypay_get_order_id(url)
                if not order_id:
                    print(url)
                    print("ERROR: 注文IDを取得できませんでした")
                    print()
                    continue

                raw_status = paypay_get_raw_status(driver)
                tracking_number, carrier = paypay_get_tracking_info(driver)
                seller_name = paypay_get_seller_name(driver)
                messages = paypay_get_messages(driver, seller_name)
                has_seller_message = any(m["sender_type"] == "出品者" for m in messages)

                item_name = paypay_get_item_name(driver)
                purchase_datetime = paypay_get_purchase_date(driver)
                purchase_price = paypay_get_purchase_price(driver)

                # 日常に注文IDのレコードが無い場合（仕入入力忘れ等）は新規追加する。
                # Mercariと同じensure_daily_record()を使用する。既存レコードがある場合は何もしない。
                created = ensure_daily_record(
                    access_conn, PAYPAY_VENDOR_NAME, order_id,
                    item_name, purchase_datetime.date(), purchase_price
                )

                daily_updated = update_daily_purchase_status(access_conn, order_id, raw_status, has_seller_message)
                update_daily_tracking_info(access_conn, order_id, tracking_number, carrier)
                save_vendor_messages(sql_conn, PAYPAY_VENDOR_NAME, order_id, messages)

                print(url)
                print(f"order_id={order_id}  list_status={list_status_text!r}  raw_status={raw_status}  "
                      f"item={item_name[:30]}  price={purchase_price}  "
                      f"tracking={tracking_number}  carrier={carrier}  seller={seller_name}  "
                      f"messages={len(messages)}  日常更新={'OK' if daily_updated else '対象行なし'}")
                if created:
                    print(f"日常: 新規レコード追加（注文ID={order_id}）")
                print()

            except Exception as e:
                print(url)
                print(f"ERROR: {e}")
                print()

        sync_carrier_tracking_to_daily(access_conn)

    finally:
        access_conn.close()
        sql_conn.close()
        # 処理用タブは成功・失敗にかかわらず必ず閉じる（ユーザーの既存タブには触れない）。
        _close_processing_tab(driver, tab_id)
        # メルカリと同様、chromedriver.exeセッションを終了する（常駐Chrome本体・
        # 既存タブには影響しない）。旧実装はここが無く、実行するたびに
        # chromedriver.exeが残留し続けていた。
        driver.quit()


# ============================================================================
# ============================================================================
# ラクマ固有（旧 apps/etc/rakuma_purchase.py）
# 同名衝突があった識別子は rakuma_ / RAKUMA_ 接頭辞で区別している。
# ============================================================================
# ============================================================================
RAKUMA_VENDOR_NAME = "ラクマ"

RAKUMA_CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
RAKUMA_PROFILE_DIR = r"D:\apps_nostock\selenium_profile"
RAKUMA_DEBUG_PORT = 9223
RAKUMA_LAUNCH_TIMEOUT_SEC = 30

BUY_LIST_URL = "https://fril.jp/buy"

RAKUMA_TRACKING_NUMBER_RE = re.compile(r"\d{10,14}")
RAKUMA_ORDER_ID_RE = re.compile(r"item_id=([0-9a-f]{32})")

# 送り状番号の直後に表示される文言で配送会社を判定する（番号の書式からは判定しない）
RAKUMA_TRACKING_CARRIER_MARKERS = (
    ("ヤマト運輸のサイトへ移動します", "ヤマト"),
    ("日本郵便のサイトへ移動します", "日本郵便"),
)

# 実機確認済みの文言のみで判定する。未確認の文言は追加しない。
ARRIVED_MARKER = "配達が完了しました"
# 発送前ステータスは、ステータス表示専用の要素(.status-title、h5)の文言で判定する
# （2026-08-29実機確認済み）。旧文言「出品者の発送をお待ちください」は実際のページと
# 一致せず、未発送の取引を発送済みと誤判定する不具合の原因になっていたため修正。
BEFORE_SHIP_MARKER = "商品発送までしばらくお待ちください"


# ------------------------------------------------------------
# Chrome起動・タブ管理（mercari_ensure_chrome_debugger()と同じ仕組み）
# ------------------------------------------------------------
def _rakuma_debugger_alive(port: int) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


def rakuma_ensure_chrome_debugger(port: int = RAKUMA_DEBUG_PORT, profile_dir: str = RAKUMA_PROFILE_DIR,
                                   timeout: int = RAKUMA_LAUNCH_TIMEOUT_SEC) -> None:
    if _rakuma_debugger_alive(port):
        print(f"OK: 起動済みのChrome(ポート{port})を利用します")
        return

    print(f"Chromeをリモートデバッグモードで起動します（ポート{port}）...")
    subprocess.Popen([
        RAKUMA_CHROME_EXE,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
    ])

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _rakuma_debugger_alive(port):
            print("OK: Chrome起動完了")
            return
        time.sleep(0.5)

    raise RuntimeError(f"Chromeの起動確認がタイムアウトしました（{timeout}秒）")


# ------------------------------------------------------------
# 取引一覧
# ------------------------------------------------------------
def collect_in_progress_transaction_urls(driver, retries: int = 6, interval: float = 2.0):
    """
    「取引中」タブに実際に表示されている取引URLだけを返す。
    購入済み(完了)分もDOM上には存在するため、is_displayed()で可視要素のみに絞る。

    【2026-08-31 メルカリの実機不具合を受けて共通の考え方を適用】retries回待っても
    フィルタ前の生リンク（is_displayed()適用前）が1件も見つからない場合は、
    0件と決めつけず例外を送出する（一覧ページの描画・読み込みに失敗している
    可能性があるため）。「取引中」が可視要素の中に無いだけで結果的に0件になるのは
    正常な状態のため区別する（その場合は生リンクは見つかっている）。
    """
    driver.get(BUY_LIST_URL)

    ever_had_raw_links = False
    visible_urls = []
    for attempt in range(1, retries + 1):
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='transaction?item_id=']")
        if links:
            ever_had_raw_links = True

        visible_urls = []
        seen = set()
        for link in links:
            if not link.is_displayed():
                continue
            href = link.get_attribute("href")
            if href and href not in seen:
                seen.add(href)
                visible_urls.append(href)

        if visible_urls or attempt == retries:
            break
        time.sleep(interval)

    if not ever_had_raw_links:
        raise RuntimeError(
            f"購入した商品の一覧が読み込めませんでした（{retries * interval:.0f}秒待っても"
            "取引リンクが1件も見つかりません）。一覧ページの描画に失敗している可能性が"
            "あるため、0件と決めつけず処理を中断します。"
        )

    return visible_urls


def rakuma_get_order_id(url: str):
    m = RAKUMA_ORDER_ID_RE.search(url)
    return m.group(1) if m else None


# ------------------------------------------------------------
# 個別取引ページ
# ------------------------------------------------------------
def rakuma_get_raw_status(driver) -> str:
    """
    配送状況を取得する。「発送済みであることを確認できた場合だけ発送済みとする」を
    原則とし、既知のいずれの文言にも一致しない場合は自動返信を誤らせないよう
    例外を送出する（mercari_get_raw_status()と同じ方針）。

    発送前判定は、ステータス表示専用の要素(.status-title)の文言で行う
    （body全文を検索すると、無関係な箇所の文言に誤って一致するリスクがあるため）。

    【2026-08-29 実機不具合を受けて修正】旧実装は既知の2文言（配達完了／発送前）の
    いずれにも一致しない場合に無条件で"発送済み"を返しており、ページ文言が実際の
    表示と一致しなくなった際に、未発送の取引を発送済みと誤判定して自動返信の
    定型文（発送お礼）を誤送信する原因になった（実例: ddf616d4302a023f63171d864533675a）。

    「発送済み・配送中」状態の.status-title文言は実機で未確認のため、
    確認できるまでは実装しない（該当する取引は判定不能として例外になる）。
    """
    status_title_els = driver.find_elements(By.CSS_SELECTOR, ".status-title")
    status_title_text = status_title_els[0].text.strip() if status_title_els else ""

    if BEFORE_SHIP_MARKER in status_title_text:
        return "発送前"

    body_text = driver.find_element(By.TAG_NAME, "body").text
    if ARRIVED_MARKER in body_text:
        return "☆出荷可能"

    raise RuntimeError(
        f"配送状況を判定できませんでした（.status-title={status_title_text!r}）。"
        "未確認の状態のため、誤って発送済み扱いにしないよう処理を中断します。"
    )


def rakuma_get_tracking_info(driver):
    """
    「お問い合わせ伝票番号」の値と、続く「※◯◯のサイトへ移動します」の文言から
    送り状番号・配送会社を取得する。見つからない場合は (None, None)。
    """
    body_text = driver.find_element(By.TAG_NAME, "body").text
    idx = body_text.find("伝票番号")
    if idx == -1:
        return None, None

    window = body_text[idx: idx + 200]
    m = RAKUMA_TRACKING_NUMBER_RE.search(window)
    if not m:
        return None, None
    tracking_number = m.group(0)

    for marker, carrier in RAKUMA_TRACKING_CARRIER_MARKERS:
        if marker in window:
            return tracking_number, carrier

    return tracking_number, None


def rakuma_get_seller_name(driver):
    """
    「出品者情報」見出しの直後の.row（プロフィールリンク・ショップ名）から取得する。
    実機確認済み: 見出しと氏名は別々の.row（兄弟要素）に分かれている。
    """
    els = driver.find_elements(By.XPATH, "//*[contains(text(), '出品者情報')]")
    if not els:
        return None
    heading_row = driver.execute_script("return arguments[0].closest('.row');", els[0])
    if heading_row is None:
        return None
    sibling = driver.execute_script("return arguments[0].nextElementSibling;", heading_row)
    if sibling is None:
        return None
    name_els = sibling.find_elements(By.CSS_SELECTOR, "a.bridge-user.primary-text")
    return name_els[0].text.strip() if name_els else None


def rakuma_get_messages(driver, self_name: str):
    """
    取引メッセージを発言順に取得する。sender_nameが self_name と一致するものを
    「購入者(自分)」、それ以外を「出品者」として扱う。
    """
    name_els = driver.find_elements(By.CSS_SELECTOR, ".user-name")

    messages = []
    for i, name_el in enumerate(name_els, start=1):
        sender_name = name_el.text.strip()
        container = driver.execute_script(
            "return arguments[0].closest('li') || arguments[0].parentElement.parentElement;",
            name_el
        )
        body_text = container.text.strip() if container else ""
        lines = [l for l in body_text.split("\n") if l.strip()]

        # 1行目=送信者名、2行目=日時、3行目以降=本文 という並びを想定（実機確認済みの表示順）
        message_datetime_text = lines[1] if len(lines) > 1 else ""
        message_body = "\n".join(lines[2:]) if len(lines) > 2 else ""

        is_self = (sender_name == self_name)
        messages.append({
            "message_no": i,
            "sender_name": "自分" if is_self else sender_name,
            "sender_type": "購入者" if is_self else "出品者",
            "message_datetime_text": message_datetime_text,
            "message_body": message_body,
        })

    return messages


def get_self_name(driver):
    """ログイン中の自分の表示名。ヘッダー等に出る名前ではなく、実装簡素化のため
    購入者側メッセージの送信者名から推定するのではなく、マイページの導線に頼らず
    ページ内スクリプトのbugsnagユーザー情報から取得する（実機確認済み）。"""
    import json
    scripts = driver.find_elements(By.CSS_SELECTOR, "script[data-bugsnag-user]")
    for s in scripts:
        try:
            data = json.loads(driver.execute_script("return arguments[0].textContent;", s))
            if "name" in data:
                return data["name"]
        except Exception:
            continue
    return None


# ------------------------------------------------------------
# メッセージ送信（/messages画面からの実送信）
# ------------------------------------------------------------
# 本文入力欄・送信ボタンは実機DOM調査で特定済み（推測ではない）。
#   本文入力欄: <textarea name="comment" id="order-comment" class="message-textarea">
#   送信ボタン: 文言「取引メッセージを送る」の<button type="button">
# 送信ボタンクリック位置に対するdocument.elementFromPoint()はメルカリ・PayPayフリマ同様
# Noneになる（座標系のズレが同じく発生する）ため、Selenium座標クリックではなくDOM直接clickを使う。
#
# fril.jp/transaction?item_id={32桁hexの既存vendor_item_id} は
# web.fril.jp/v2/purchase/receipt/item?is_web=1&item_id={別体系の数値ID} へ自動リダイレクトされる
# （実機確認済み。ラクマがサイトのURL体系を新しいものへ移行済みのため）。
# 既存のvendor_item_id（trx.vendor_messageに保存済みの32桁hex）との整合性を保つため、
# 送信もこのリダイレクト前提の既存URL形式（item_idに32桁hexを渡す）で行う。
RAKUMA_CHAT_SEND_BUTTON_TEXT = "取引メッセージを送る"
RAKUMA_SEND_MESSAGE_API_URL_SUBSTR = "/api/order/comment/add"

RAKUMA_SEND_RESPONSE_WAIT_SEC = 15.0


def rakuma_send_chat_message(driver, order_id: str, expected_count: int, reply_text: str) -> dict:
    """
    ラクマの取引ページへ実際にメッセージを送信する（誤送信防止のため必ずこの手順で行う）。

    【2026-08-30 3サイト共通のexpected_count方式に統一】新着確認基準の名称を
    expected_last_message_no から expected_count（0以上の整数）へ変更した。
    比較ロジック自体（件数比較）は変更していない。

    実機調査済み（2026-08-29、実送信1回で確認）:
      送信通信: POST https://api.fril.jp/api/order/comment/add
      （web.fril.jp → api.fril.jp のクロスオリジンリクエストで、Authorizationヘッダーを
      持つためCORSプリフライト(OPTIONS)が発生する。メルカリで対策済みの
      「Network.requestWillBeSentでmethod=="POST"のリクエストのみ追跡する、1本の
      継続的な受信ループ」構造をそのまま使い、OPTIONSを誤認識しない）。
      レスポンス例: {"result": true, "comments": [{"id":..., "order_id":..., "comment":...,
      "created_at":..., "screen_name":..., ...}], "current_user_id": ...}
      各コメントには"id"（コメント固有の数値ID）が付与されるが、ラクマの既存保存経路
      （save_vendor_messages()、(vendor_name, vendor_item_id, message_no)キーのMERGE）は
      そもそもmessage_id列を使わない設計のため、ここでもmessage_id方式は導入しない。
      送信成功後にrakuma_get_messages()を再実行し、送信分を含む最新の全件をそのまま
      save_vendor_messages()へ渡せば、通常scrapeと全く同じ経路で重複なく保存できる。

    手順:
      1. 取引ページを開き、現在のメッセージ件数をrakuma_get_messages()で取得する
      2. /messages画面表示時点の件数(expected_count)と比較し、新しいメッセージが
         増えていないか確認する（増えていれば送信せず中止し、取得済みのrakuma_get_messages()
         結果をそのまま呼び出し元へ渡す）
      3. Page.bringToFront → JS focus() → CDP Input.insertText で本文を入力し、valueの
         読み返しと送信ボタンのdisabled解除を確認する
      4. 送信ボタンをDOM直接clickで「1回だけ」クリックする（このスクリプト内で再クリックは
         一切行わない。失敗時も自動リトライしない＝二重送信防止を最優先する）
      5. 実際のPOSTレスポンス（HTTPステータス・本文）をCDP経由で捕捉し、status==200 かつ
         本文の result が真であることをもって初めて成功と判定する
         （クリックの成否・HTTP 200単体では判定しない）
      6. 成功していれば、rakuma_get_messages()を再度呼び直し、送信したメッセージを含む
         最新の全件を戻り値として返す

    戻り値: {
        "ok": bool, "error": str|None,
        "reason": "new_message_detected"|None,
        "new_messages": list|None,  # 成功時・新着検出時とも、rakuma_get_messages()の戻り値そのもの
    }
    """
    url = f"https://fril.jp/transaction?item_id={order_id}"
    driver.get(url)
    time.sleep(4)

    self_name = get_self_name(driver)
    current_messages = rakuma_get_messages(driver, self_name)
    if len(current_messages) != expected_count:
        return {"ok": False, "error": "新しいメッセージを受信したため送信を中止しました",
                "reason": "new_message_detected", "new_messages": current_messages}

    textarea_els = driver.find_elements(By.CSS_SELECTOR, "textarea#order-comment")
    if not textarea_els:
        return {"ok": False, "error": "本文入力欄が見つかりません", "reason": None, "new_messages": None}
    textarea = textarea_els[0]

    send_button = None
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        if btn.text.strip() == RAKUMA_CHAT_SEND_BUTTON_TEXT:
            send_button = btn
            break
    if send_button is None:
        return {"ok": False, "error": "送信ボタンが見つかりません", "reason": None, "new_messages": None}

    import json as _json
    import websocket as _ws_client  # ローカルimport: 送信結果の監視専用のため使用箇所を限定する

    # driver.get(url)は32桁hexのorder_idを渡したURLだが、実際には新URL体系
    # （数値item_id）へ自動リダイレクトされているため、CDPターゲットは
    # order_idの部分文字列ではなく、リダイレクト後の実際のURL（driver.current_url）
    # で照合する。
    with urllib.request.urlopen(f"http://127.0.0.1:{RAKUMA_DEBUG_PORT}/json/list") as resp:
        targets = _json.loads(resp.read().decode("utf-8"))
    current_url_now = driver.current_url
    target = next((t for t in targets if t.get("url") == current_url_now), None)
    if target is None:
        return {"ok": False, "error": "CDPターゲットが見つかりません", "reason": None, "new_messages": None}

    ws = _ws_client.create_connection(target["webSocketDebuggerUrl"], timeout=RAKUMA_SEND_RESPONSE_WAIT_SEC, suppress_origin=True)
    try:
        next_id = [0]

        def send_cdp(method, params=None):
            next_id[0] += 1
            ws.send(_json.dumps({"id": next_id[0], "method": method, "params": params or {}}))
            return next_id[0]

        send_cdp("Network.enable")

        driver.execute_cdp_cmd("Page.bringToFront", {})
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", textarea)
        time.sleep(0.2)
        driver.execute_script("arguments[0].focus();", textarea)
        time.sleep(0.2)

        driver.execute_cdp_cmd("Input.insertText", {"text": reply_text})
        time.sleep(0.6)

        actual_value = textarea.get_attribute("value")
        if actual_value != reply_text:
            return {"ok": False, "error": f"本文入力欄への入力を確認できませんでした（value={actual_value!r}）",
                    "reason": None, "new_messages": None}

        if send_button.get_attribute("disabled") is not None:
            return {"ok": False, "error": "送信ボタンが無効化されたままでした（入力内容が反映されていない可能性があります）",
                    "reason": None, "new_messages": None}

        # --- クリックは以降この1回のみ。失敗しても自動リトライしない（二重送信防止） ---
        driver.execute_script("arguments[0].click();", send_button)

        target_request_id = None
        status = None
        get_body_id = None
        body = None

        deadline = time.time() + RAKUMA_SEND_RESPONSE_WAIT_SEC
        while time.time() < deadline and body is None:
            ws.settimeout(max(0.1, deadline - time.time()))
            try:
                raw = ws.recv()
            except Exception:
                continue
            try:
                data = _json.loads(raw)
            except Exception:
                continue

            method = data.get("method")

            if method == "Network.requestWillBeSent" and target_request_id is None:
                req = data["params"].get("request", {})
                if req.get("method") == "POST" and RAKUMA_SEND_MESSAGE_API_URL_SUBSTR in req.get("url", ""):
                    target_request_id = data["params"]["requestId"]

            elif method == "Network.responseReceived" and data["params"].get("requestId") == target_request_id:
                status = data["params"]["response"]["status"]

            elif (method == "Network.loadingFinished"
                  and data["params"].get("requestId") == target_request_id
                  and get_body_id is None):
                get_body_id = send_cdp("Network.getResponseBody", {"requestId": target_request_id})

            elif method is None and get_body_id is not None and data.get("id") == get_body_id:
                if "result" in data:
                    raw_body = data["result"]["body"]
                    try:
                        body = _json.loads(raw_body)
                    except Exception:
                        body = raw_body
    finally:
        ws.close()

    if target_request_id is None:
        return {"ok": False, "error": "送信リクエスト（POST .../comment/add）の発生を確認できませんでした"
                                       "（クリックが反映されていない可能性があります）。再送信はせず、必ず状況を確認してください。",
                "reason": None, "new_messages": None}

    if status != 200 or not isinstance(body, dict) or not body.get("result"):
        return {"ok": False, "error": f"ラクマ側の実レスポンスで送信成功を確認できませんでした"
                                       f"（status={status}, body={body}）",
                "reason": None, "new_messages": None}

    # 実送信成功。rakuma_get_messages()を再実行し、送信分を含む最新の全件を返す
    # （通常scrapeと同じsave_vendor_messages()経路でDB保存できるようにするため）。
    time.sleep(1.0)
    fresh_messages = rakuma_get_messages(driver, self_name)

    return {"ok": True, "error": None, "reason": None, "new_messages": fresh_messages}


# ------------------------------------------------------------
# メイン
# ------------------------------------------------------------
def rakuma_main():
    rakuma_ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{RAKUMA_DEBUG_PORT}"

    driver = webdriver.Chrome(options=options)
    sql_conn = get_sql_server_connection()
    access_conn = get_access_connection()

    tab_id = None
    try:
        # 処理用タブをbackgroundで作成する（Chromeウィンドウを前面化しない。
        # 実機確認済み）。既存タブは一切操作しない。
        tab_id = _create_processing_tab(driver)

        transaction_urls = collect_in_progress_transaction_urls(driver)
        print(f"取引URL数(取引中のみ): {len(transaction_urls)}")
        print()

        for url in transaction_urls:
            try:
                driver.get(url)
                time.sleep(4)

                order_id = rakuma_get_order_id(url)
                if not order_id:
                    print(url)
                    print("ERROR: 注文ID(item_id)を取得できませんでした")
                    print()
                    continue

                self_name = get_self_name(driver)
                raw_status = rakuma_get_raw_status(driver)
                tracking_number, carrier = rakuma_get_tracking_info(driver)
                seller_name = rakuma_get_seller_name(driver)
                messages = rakuma_get_messages(driver, self_name)
                has_seller_message = any(m["sender_type"] == "出品者" for m in messages)

                daily_updated = update_daily_purchase_status(access_conn, order_id, raw_status, has_seller_message)
                update_daily_tracking_info(access_conn, order_id, tracking_number, carrier)
                save_vendor_messages(sql_conn, RAKUMA_VENDOR_NAME, order_id, messages)

                print(url)
                print(f"order_id={order_id}  raw_status={raw_status}  tracking={tracking_number}  "
                      f"carrier={carrier}  seller={seller_name}  messages={len(messages)}  "
                      f"日常更新={'OK' if daily_updated else '対象行なし'}")
                print()

            except Exception as e:
                print(url)
                print(f"ERROR: {e}")
                print()

        sync_carrier_tracking_to_daily(access_conn)

    finally:
        access_conn.close()
        sql_conn.close()
        # 処理用タブは成功・失敗にかかわらず必ず閉じる（ユーザーの既存タブには触れない）。
        _close_processing_tab(driver, tab_id)
        # メルカリと同様、chromedriver.exeセッションを終了する（常駐Chrome本体・
        # 既存タブには影響しない）。旧実装はここが無く、実行するたびに
        # chromedriver.exeが残留し続けていた。
        driver.quit()


# ============================================================================
# ============================================================================
# 返信送信ラッパー
# （旧 apps/common/mercari_send_reply.py / paypay_send_reply.py / rakuma_send_reply.py）
#
# /messages画面の「送信」ボタンから、各サイトの取引ページへ実際に返信を送信する処理。
# Webアプリ本体（D:\apps_resale\furima\webapp\messages_blueprint.py）から呼び出される。
# DOM操作（本文入力欄・送信ボタンの特定、送信前の再確認、実送信結果の確認）は上記の
# 各サイト固有の *_send_chat_message() に実装済みのものをそのまま使う
# （本節では新たにセレクタ・判定ロジックを実装しない）。
# 各サイトが使っているChromeデバッグセッション（ポート9223・永続プロファイル）を
# そのまま利用する。ログイン状態を壊さないよう、送信は新しいタブを開いて行い、
# 完了後にそのタブだけを閉じる。
# ============================================================================
# ============================================================================
def send_mercari_reply(vendor_item_id: str, expected_count: int, reply_text: str,
                        expected_last_message_id=None) -> dict:
    """
    expected_count: /messages画面表示時点のメッセージ件数（0以上の整数。メッセージが
    無い取引に初めて送る場合は0）。3サイト共通の新着確認基準。
    expected_last_message_id: expected_count>0の場合に渡す、画面表示時点の最新メッセージの
    message_id（メルカリ内部の安定した一意ID）。件数一致に加えた追加の安全確認に使う
    （expected_count>0なのに省略した場合は安全のため送信しない）。
    戻り値: {"ok": bool, "error": str|None}
    """
    mercari_ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{MERCARI_DEBUG_PORT}"
    driver = webdriver.Chrome(options=options)

    tab_id = None
    try:
        # 処理用タブをbackgroundで作成する（Chromeウィンドウを前面化しない）。
        # 本文入力の直前にmercari_send_chat_message()内でPage.bringToFrontにより
        # 一時的に前面化する（これは引き続き必要なため維持する）。
        tab_id = _create_processing_tab(driver)
        result = mercari_send_chat_message(driver, vendor_item_id, expected_count, reply_text,
                                            expected_last_message_id=expected_last_message_id)
    finally:
        _close_processing_tab(driver, tab_id)
        driver.quit()

    return result


def send_paypay_reply(vendor_item_id: str, expected_count: int, reply_text: str) -> dict:
    """
    expected_count: /messages画面表示時点のメッセージ件数（0以上の整数。メッセージが
    無い取引に初めて送る場合は0）。PayPayフリマにはメルカリのような安定した一意message_idが
    無いため、この件数比較のみで新着有無を判定する。
    戻り値: {"ok": bool, "error": str|None, "reason": str|None, "new_messages": list|None}
    """
    paypay_ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{PAYPAY_DEBUG_PORT}"
    driver = webdriver.Chrome(options=options)

    tab_id = None
    try:
        # 処理用タブをbackgroundで作成する（Chromeウィンドウを前面化しない）。
        # 本文入力の直前にpaypay_send_chat_message()内でPage.bringToFrontにより
        # 一時的に前面化する（これは引き続き必要なため維持する）。
        tab_id = _create_processing_tab(driver)
        result = paypay_send_chat_message(driver, vendor_item_id, expected_count, reply_text)
    finally:
        _close_processing_tab(driver, tab_id)
        driver.quit()

    return result


def send_rakuma_reply(vendor_item_id: str, expected_count: int, reply_text: str) -> dict:
    """
    expected_count: /messages画面表示時点のメッセージ件数（0以上の整数。メッセージが
    無い取引に初めて送る場合は0）。ラクマにはメルカリのような安定した一意message_idが
    無いため、この件数比較のみで新着有無を判定する。
    戻り値: {"ok": bool, "error": str|None, "reason": str|None, "new_messages": list|None}
    """
    rakuma_ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{RAKUMA_DEBUG_PORT}"
    driver = webdriver.Chrome(options=options)

    tab_id = None
    try:
        # 処理用タブをbackgroundで作成する（Chromeウィンドウを前面化しない）。
        # 本文入力の直前にrakuma_send_chat_message()内でPage.bringToFrontにより
        # 一時的に前面化する（これは引き続き必要なため維持する）。
        tab_id = _create_processing_tab(driver)
        result = rakuma_send_chat_message(driver, vendor_item_id, expected_count, reply_text)
    finally:
        _close_processing_tab(driver, tab_id)
        driver.quit()

    return result


# ============================================================================
# ============================================================================
# 実行オーケストレーション（旧 apps/etc/furima_purchase_runner.py）
#
# Access「到着日入力」フォームの「フリマ情報取得」ボタンから起動する薄いラッパー。
# メルカリ・PayPayフリマ・ラクマの各 *_main()（本ファイル内の各サイト固有節に定義済み）を
# 順番に呼び出すだけで、各サイトの取得ロジック（ステータス判定・送り状番号・到着日・
# メッセージ取得・DB保存等）は一切変更・複製しない。
#
# 1サイトの失敗で残りのサイトの実行を止めない（サイト単位でtry/exceptする）。
#
# 二重起動防止:
#   ロックファイル(furima_purchase_runner.lock)に自分のPIDを書き込む。
#   既存のロックファイルがある場合、そのPIDが実際に生きているプロセスかどうかを
#   psutilで確認し、生きていれば「実行中」として二重起動を拒否する。
#   PC再起動・強制終了等でプロセスが既に無いのにロックファイルだけ残っている
#   （stale lock）場合は無視して実行する。
#   【2026-08-29統合】ロック・状態ファイルは旧 furima_purchase_runner.py と同じ
#   ファイル名を引き続き使う。統合当初は旧ファイルも現役という想定だったが、
#   【2026-09-01確認】Access「ヤフオク.accdb」Form_到着日入力のVBAを実機で確認した
#   ところ、Shell()は既に `python.exe furima_purchase.py` を直接起動しており、
#   旧 furima_purchase_runner.py（→旧mercari_purchase.py等）は現在呼ばれていない。
#   ファイル名共有は不要になったが、実害はないため据え置いている。
#
# 状態ファイル(furima_purchase_runner_status.txt)に running/done/error と
# サイトごとの結果(success/error)を書き込む。Access側のフォームタイマーが
# これをポーリングして表示更新・Requeryに使う。VBA側にJSONパーサーを新設
# せずに読めるよう、あえてJSONではなく1行1個の"key=value"形式にしている。
# ============================================================================
# ============================================================================
LOCK_FILE = Path(__file__).with_name("furima_purchase_runner.lock")
STATUS_FILE = Path(__file__).with_name("furima_purchase_runner_status.txt")

# (状態ファイル上の表示名, 呼び出す各サイトのmain())
SITES = [
    ("Mercari", mercari_main),
    ("YahooFurima", paypay_main),
    ("Rakuma", rakuma_main),
]


def _write_status(state: str, results: dict, started_at: str, finished_at: str = "") -> None:
    lines = [
        f"state={state}",
        f"started_at={started_at}",
        f"finished_at={finished_at}",
    ]
    lines += [f"{name}={value}" for name, value in results.items()]
    STATUS_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _is_locked_by_live_process() -> bool:
    """既存ロックファイルがあり、かつそのPIDが実際に生きているプロセスならTrue。
    ファイルはあるが対応プロセスが存在しない場合（stale lock）はFalseを返す。"""
    if not LOCK_FILE.exists():
        return False
    try:
        pid = int(LOCK_FILE.read_text(encoding="utf-8").strip())
    except (ValueError, OSError):
        return False
    return psutil.pid_exists(pid)


def main() -> None:
    if _is_locked_by_live_process():
        print("[INFO] 既に実行中のプロセスが存在するため、今回は起動しません。")
        return

    LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
    started_at = datetime.now().isoformat()
    results = {name: "pending" for name, _ in SITES}
    _write_status("running", results, started_at)

    try:
        for name, run_func in SITES:
            print(f"\n{'=' * 20} {name} {'=' * 20}", flush=True)
            try:
                run_func()
                results[name] = "success"
            except Exception as e:
                print(f"[ERROR] {name} の実行中にエラーが発生しました: {e}", flush=True)
                results[name] = "error"
            # 1サイト終わるたびに書き込み、Access側が進捗を見られるようにする。
            _write_status("running", results, started_at)

        overall_state = "done" if all(v == "success" for v in results.values()) else "error"
        _write_status(overall_state, results, started_at, finished_at=datetime.now().isoformat())

    finally:
        try:
            LOCK_FILE.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    main()
