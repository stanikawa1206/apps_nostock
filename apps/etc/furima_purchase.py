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
# pythonw.exe（コンソール無し）ではsys.stdout/sys.stderrがNoneになるため、
# タスクスケジューラをpython.exeからpythonw.exeへ切り替えても起動時に
# AttributeErrorで落ちないようNoneチェックを行う。
if sys.stdout is not None:
    sys.stdout.reconfigure(encoding="utf-8")
if sys.stderr is not None:
    sys.stderr.reconfigure(encoding="utf-8")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import json
import logging
import os
import re
import socket
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

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

# 【2026-09-10 trx.vendor_purchase廃止に伴い追加】このプログラムの管理外・後工程の
# 値。write_ebay_status_if_advancing()はランク比較すら行わず、常に上書きしない
# （例: GA鑑定待ち・◎有在庫はメルカリ到着後の別ワークフロー、出荷済みはeBay側の
# 発送完了で、このプログラムが管理する「購入〜到着」より後の工程のため）。
# 「出荷済み」はランク上も最上位(4)のため従来から実質保護されていたが、
# GA鑑定待ち・◎有在庫はランク表に無く既定で最下位(-1)扱いとなり上書きされて
# しまっていたため、明示的に保護対象とする。
PROTECTED_EBAY_STATUSES = ("GA鑑定待ち", "出荷済み", "◎有在庫")


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
    現在値がPROTECTED_EBAY_STATUSES（このプログラムの管理外・後工程の値）の場合は、
    ランク比較すら行わず一切上書きしない。それ以外でランク不明の現在値（人手入力など）は
    保護対象外とし、これまで通り上書きする。
    戻り値: 該当行が存在し実際に更新できたか。
    """
    row = access_cur.execute(
        f"SELECT eBayステータス FROM {ACCESS_TABLE} WHERE 注文ID = ?", order_id
    ).fetchone()
    if row is None:
        return False

    current_status = row[0]
    if current_status in PROTECTED_EBAY_STATUSES:
        return False

    new_rank = _STATUS_RANK.get(new_status, 999)
    current_rank = _STATUS_RANK.get(current_status, -1)
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


def sync_flema_active_orders(access_conn, vendor_name: str, active_order_ids) -> dict:
    """
    【2026-09-10 trx.vendor_purchase廃止に伴い新設】各サイトの「取引中」一覧を
    最後まで正常取得できた直後に、店舗単位で日常.フリマ取引中を一括更新する。
    呼び出し元は、一覧取得が完全に成功した場合のみこの関数を呼ぶこと
    （取得失敗・途中中断時はフラグを一切変更しないため、この関数自体を呼ばない）。

    手順（すべて1つのトランザクションとして実行する。【2026-09-10改善】途中で
    例外が発生した場合はaccess_conn.rollback()して呼び出し元へ再送出し、
    前回のフラグ状態を維持する。呼び出し元は、一覧取得が完全に成功した場合のみ
    この関数を呼ぶため、この関数自体の失敗＝そのサイトの処理全体をエラー扱いに
    してよい）:
      1. その店舗の「現在ONになっている」行だけをOFFにする
         （【2026-09-10改善】以前は店舗の全行を無条件UPDATEしており、対象外の
         行数まで巨大なUPDATE件数として報告されていた。WHERE句にフリマ取引中=True
         を加え、実際にOFFへ変化させる行だけに限定する）
      2. active_order_ids に含まれる注文（既存行）はONにする
         （店舗＋注文IDが日常上で複数行になっている場合は全行を更新する。
         1回の購入を複数ASIN行に分けて記帳しているケースが実際にあるため）
      3. 日常に存在しない注文IDは新規追加してONにする。新規追加時の仕入日は
         スクレイプ実行日（実際の購入日時はこの時点では未取得のため）。
         品目text・仕入（金額）は空のまま（後続の個別ページ処理・ensure_daily_record()
         が正確な値を持っていれば追って補うが、この関数自体はここへ書き込まない）。
         固定値（仕入元="電脳"・区分="その他"・eBayステータス="未入力"）は
         ensure_daily_record()と同じものを使う（私物購入等も区別せず同じ扱いにする＝
         除外しない）。

    戻り値: {"reset": OFFへ変化させた行数, "updated": ONにした既存行数, "created": 新規追加件数}
    """
    access_cur = access_conn.cursor()
    try:
        access_cur.execute(
            f"UPDATE {ACCESS_TABLE} SET フリマ取引中 = ? WHERE 店舗 = ? AND フリマ取引中 = ?",
            False, vendor_name, True
        )
        reset = access_cur.rowcount

        updated = 0
        created = 0
        for order_id in active_order_ids:
            if not order_id:
                continue
            access_cur.execute(
                f"UPDATE {ACCESS_TABLE} SET フリマ取引中 = ? WHERE 店舗 = ? AND 注文ID = ?",
                True, vendor_name, order_id
            )
            if access_cur.rowcount > 0:
                updated += access_cur.rowcount
            else:
                access_cur.execute(
                    f"""INSERT INTO {ACCESS_TABLE}
                        ([注文ID], [仕入元], [店舗], [仕入日], [eBayステータス], [区分], [フリマ取引中])
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    order_id, FIXED_SOURCE, vendor_name, date.today(), UNENTERED_STATUS, FIXED_CATEGORY, True
                )
                created += 1
    except Exception:
        access_conn.rollback()
        raise
    else:
        access_conn.commit()
    finally:
        access_cur.close()

    return {"reset": reset, "updated": updated, "created": created}


def mark_flema_inactive(access_conn, vendor_name: str, order_id: str) -> int:
    """
    【2026-09-10 追加】指定の店舗＋注文IDについて、日常.フリマ取引中だけをFalseにする
    （eBayステータス・到着日・メッセージ履歴など他の列は一切変更しない）。
    購入者側の対応が完了した（例: メルカリで「受取評価をしました」を検出した）取引を、
    出品者側がまだ「取引中の商品」一覧に残していても対象外にするために使う
    （sync_flema_active_orders()の一括ON設定より後にこの関数を呼ぶことで、
    最終的にOFFの状態を保つ）。
    店舗＋注文IDが日常上で複数行になっている場合は全行を更新する。
    戻り値: 更新した行数。
    """
    access_cur = access_conn.cursor()
    try:
        access_cur.execute(
            f"UPDATE {ACCESS_TABLE} SET フリマ取引中 = ? WHERE 店舗 = ? AND 注文ID = ?",
            False, vendor_name, order_id
        )
        updated = access_cur.rowcount
        access_conn.commit()
    finally:
        access_cur.close()
    return updated


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
      - 配達完了を確認できた場合 → 到着日に実際の配達完了日を書き込み、従来通り
        販売(eBay/amazon)に応じた到着済みステータス（☆出荷可能／☆到着済）へ進める。
      - 配達完了はまだだが配達予定日を取得できた場合 → 到着日に配達予定日を書き込み、
        ステータスをESTIMATED_ARRIVAL_STATUS(到着予定)へ進める。【2026-09-10再修正】
        一度「配達予定日は到着日に書き込まない」方針に変更したが、ヤマト・日本郵便が
        提示する到着予定日自体は信頼できる情報として引き続き到着日へ保存する方針に
        戻した（フリマ画面側で「配達済み」を確認しただけの日付を到着日として代用する
        （＝旧sync_arrival_status_to_access()のフォールバック）のとは別物）。
        配達完了確認後は、下のUPDATEが実際の配達完了日で上書きするため、
        到着予定日が古いまま残ることはない。実配達確認済みという意味ではないため、
        この状態のままでは対象から外れず、次回以降も追跡を継続する
        （受取評価忘れアラート側は、eBayステータス=到着予定の間は対象外にする。
        fetch_pending_seller_messages側の_apply_arrival_reminders()参照）。
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

        # 到着日は、確定配達日・配達予定日のいずれの場合も書き込む（配達完了確認後に
        # 実行された場合は、ここで実際の配達完了日が予定日を上書きする）。
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


# 【2026-09-10 trx.vendor_purchase廃止に伴い変更】従来は「到着日入力」フォームと
# 同じ条件（仕入日・区分・発送日IS NULL等）で母集団を決めていたが、この条件は
# 「こちらの作業（eBayへの再出品等）がまだ進んでいない」ことを基準にしており、
# 各サイトの実際の「現在取引中」状態とは無関係だった（そのため、実サイトでは
# 既に取引完了しているのに日常側の条件だけで一覧に残り続ける不具合があった。
# 実例: m17393213750）。
#
# 母集団は、各サイトのメイン取得処理（mercari_main/paypay_main/rakuma_main）が
# 取引中一覧を最後まで正常取得できるたびに sync_flema_active_orders() で更新する
# 日常.フリマ取引中フラグだけを基準にする。Access日常の他の列（発送日等）は
# 一切参照しない（Accessの過去データを起点に検索しない）。
FETCH_ACTIVE_ORDERS_SQL = """
    SELECT 日常.注文ID,
           IIf(IsNull(ASIN.品目), 日常.品目text, ASIN.品目) AS 商品名,
           日常.eBayステータス,
           日常.店舗,
           日常.到着日
    FROM 日常 LEFT JOIN ASIN ON 日常.ASIN = ASIN.ASIN
    WHERE 日常.フリマ取引中 = True
"""


def fetch_active_orders(access_conn) -> dict:
    """
    Access「日常」から、フリマ取引中=True（各サイトの直近の巡回で「取引中」と
    確認できた注文）の一覧を取得する（注文ID・商品名・eBayステータス・店舗・到着日）。

    店舗列は、trx.vendor_messageに保存されている(vendor_name, vendor_item_id)全129件と
    突き合わせて実機検証済み（一致126件・不一致0件・NULL0件。残り3件は日常に
    レコード自体が無いだけで矛盾ではない）。trx.vendor_messageに一度も履歴の無い
    「無言発送」の取引でも、この店舗列だけでvendor_nameを特定できる
    （trx.vendor_messageへダミー行を作らず、新規テーブルも増やさずに済む）。

    注文IDが日常テーブル上で複数行になっている場合（実データで実例あり。1回の
    購入で複数ASINを別行として記録している等）、それらをまとめて同一取引として扱う。
    eBayステータス・店舗・到着日は注文単位の情報で、既存の更新処理
    （write_ebay_status_if_advancing等）が常に注文ID一致の全行へUPDATEするため、
    対象行間で値が揃っている前提で先に見つかった行の値を採用する。商品名だけは
    行ごとに異なりうるため、重複を除いて出現順に全件保持する（1件も取りこぼさない）。

    戻り値: {注文ID: {"product_names": [str, ...], "ebay_status": str, "vendor_name": str,
                       "arrival_date": date|None}, ...}
    """
    result = {}
    with access_conn.cursor() as cur:
        cur.execute(FETCH_ACTIVE_ORDERS_SQL)
        for order_id, product_name, ebay_status, vendor_name, arrival_date in cur.fetchall():
            if not order_id:
                continue
            entry = result.setdefault(order_id, {
                "product_names": [],
                "ebay_status": ebay_status,
                "vendor_name": vendor_name,
                "arrival_date": arrival_date.date() if hasattr(arrival_date, "date") else arrival_date,
            })
            if product_name and product_name not in entry["product_names"]:
                entry["product_names"].append(product_name)
    return result


# 到着日からの経過日数（日付単位。時刻は見ない）がこの値以上で一覧へ強制表示し、
# 行を黄色にする。+1日（＝ARRIVAL_REMINDER_THRESHOLD_DAYS+1日以上）で赤色にする
# （具体的な色分けはフロント側のJSで行う。ここは「強制表示するかどうか」の閾値）。
ARRIVAL_REMINDER_THRESHOLD_DAYS = 3

# 【2026-09-14追加】各サイトの取引ごとの収集処理（個別取引ページの取得〜DB保存）が
# 例外で失敗した場合に、その取引だけ最大でこの回数まで試行する（1回目の失敗で
# 即座に諦めず、ページ読み込みの一時的な失敗等を自動的にリトライする）。
# 3サイト共通（mercari_main/paypay_main/rakuma_main）で使う。取引URL一覧の取得
# 自体（一覧ページ）の失敗はこの対象外（従来通り、サイト全体のエラーとして扱う）。
ITEM_COLLECTION_MAX_ATTEMPTS = 2
ITEM_COLLECTION_RETRY_WAIT_SEC = 3.0


def _apply_arrival_reminders(sql_conn, active_orders, items) -> None:
    """
    配達後の受取評価忘れアラート。active_orders（fetch_active_orders()の戻り値。
    日常.フリマ取引中=Trueの注文だけに絞られている＝各サイトの直近の巡回で
    「現在取引中」と確認できたものだけ）のうち、到着日からARRIVAL_REMINDER_
    THRESHOLD_DAYS日以上経過しているものを、未返信メッセージの有無や最新送信者に
    関係なく強制的にitemsへ追加する（既にitemsに含まれている注文は追加せず、
    到着日情報だけ付与する）。

    【2026-09-10 trx.vendor_purchase廃止に伴い変更】母集団自体が既にフリマ取引中=True
    （＝直近の巡回で現在取引中と確認できたもの）に限定されているため、この関数側で
    メルカリだけ別途「取引完了」を除外する必要が無くなった（取引完了・キャンセル等で
    サイトの取引中一覧から外れた注文は、次回のsync_flema_active_orders()でフラグが
    OFFになり、active_orders自体に含まれなくなるため自動的に一覧から消える）。
    is_shippedもメルカリ含め全サイト共通でAccess日常.eBayステータスから判定する
    （trx.vendor_purchase.statusは参照しない）。

    【2026-09-10 再修正】到着日には配達予定日（未確定）も書き込まれるようになった
    （sync_carrier_tracking_to_daily()参照）ため、eBayステータスが到着予定
    （ESTIMATED_ARRIVAL_STATUS）の間はこのアラートの対象にしない。配達予定日は
    まだ配達完了が確認できていない見込み値であり、これを根拠に受取評価を催促するのは
    不適切なため（配達完了が確認できると、eBayステータスは☆出荷可能／☆到着済へ
    進み、到着日も実際の配達完了日に上書きされるため、その時点から対象になる）。

    【2026-09-10 返信不要ボタンとの連動】返品・キャンセル等のトラブル対応中で
    メッセージのやり取りがある取引は、条件Aによって既にitemsに含まれているのが
    通常だが、ユーザーが「返信不要」ボタンを押すと最新メッセージのreply_skippedが
    Trueになり、条件Aから外れる。この関数はitemsに含まれていない注文を到着日基準で
    無条件に強制表示するため、何もしなければ返信不要が効かず毎回再表示されてしまう。
    そのためここでも、対象注文の最新メッセージのreply_skippedがTrueの場合は強制表示
    しない。メッセージが1件も無い「無言発送」のケース（latestがNone）は従来どおり
    対象にする。新しい出品者メッセージが届くと、その行はreply_skipped=Falseの新しい
    行になるため（既存のreply_skipped機構と同じ仕組み）、自動的に再び対象へ戻る。
    フリマ取引中自体は変更しない（返信不要ボタンはreply_skippedのみを更新する既存の
    /api/messages/skipの仕組みをそのまま使う）。

    itemsはこの関数の呼び出し元が持つリストをin-placeに変更する（新規追加・既存要素への
    arrival_date_text/days_since_arrival付与の両方）。戻り値なし。
    """
    candidates = {
        oid: info for oid, info in active_orders.items()
        if info["vendor_name"] in TARGET_VENDOR_NAMES
        and info.get("arrival_date")
        and info.get("ebay_status") != ESTIMATED_ARRIVAL_STATUS
    }
    if not candidates:
        return

    today = date.today()
    items_by_key = {(it["vendor_name"], it["vendor_item_id"]): it for it in items}

    # 新規に一覧へ追加する必要がある注文（まだitemsに含まれておらず、かつ強制表示の
    # 閾値に達しているもの）だけ、メッセージ履歴を追加取得する。
    threshold_new_order_ids = [
        oid for oid, info in candidates.items()
        if (info["vendor_name"], oid) not in items_by_key
        and (today - info["arrival_date"]).days >= ARRIVAL_REMINDER_THRESHOLD_DAYS
    ]

    if threshold_new_order_ids:
        history_by_key = _fetch_histories_for_orders(sql_conn, threshold_new_order_ids)

        for oid in threshold_new_order_ids:
            info = candidates[oid]
            vendor_name = info["vendor_name"]

            history = history_by_key.get((vendor_name, oid), [])
            latest = history[-1] if history else None

            # 「返信不要」ボタンで最新メッセージがreply_skipped=Trueにされている
            # 取引は、到着日からの経過日数に関わらず強制表示しない。
            if latest and latest["reply_skipped"]:
                continue

            is_shipped = is_shipped_status(info["ebay_status"])
            suggested_reply = determine_suggested_reply(history, is_shipped)

            new_item = _build_message_item(
                vendor_name, oid, info["product_names"], is_shipped, history, suggested_reply
            )
            items.append(new_item)
            items_by_key[(vendor_name, oid)] = new_item

    # 到着日情報は、新規追加分・既にitemsにあった分の両方へ付与する
    # （強制表示の閾値未満でも、参考情報として表示できるようにする）。
    for oid, info in candidates.items():
        it = items_by_key.get((info["vendor_name"], oid))
        if it is None:
            continue
        it["arrival_date_text"] = info["arrival_date"].strftime("%Y/%m/%d")
        it["days_since_arrival"] = (today - info["arrival_date"]).days


# ------------------------------------------------------------
# 対象抽出
# ------------------------------------------------------------
def fetch_pending_seller_messages(sql_conn, access_frontend_conn):
    """
    対象抽出は次の順序で絞り込む:
      ① Access「日常」でフリマ取引中=True（各サイトの直近の巡回で現在取引中と
         確認できた注文。sync_flema_active_orders()が更新する）の注文IDと、
         そのeBayステータス・商品名・店舗（=vendor_name）・到着日を取得
         【2026-09-10 trx.vendor_purchase廃止に伴い変更】従来はAccess「到着日入力」と
         同じ条件（発送日IS NULL等、こちらの再出品作業の進捗基準）で母集団を決めて
         いたが、実サイトの「現在取引中」とは無関係で、既に取引完了した注文が
         いつまでも一覧に残る不具合があった（実例: m17393213750）。母集団は
         各サイトのメイン取得処理が実際に確認した「現在取引中」一覧だけに限定する。
      ② 店舗がTARGET_VENDOR_NAMESの注文IDについて、対応する trx.vendor_message の
         メッセージ履歴を取得する（履歴が1件も無い＝一度もメッセージが交換されて
         いない「無言発送」の注文IDも、店舗からvendor_nameが分かるため対象に含める）
      ③ 次のいずれかに該当する取引だけを対象にする（人が「返信不要」にした対象は除く）
           A. 最新メッセージが出品者（sender_type='出品者'）
           B. 発送済み（is_shipped）で、まだ返信2相当を送っていない
              （無言発送も対象に含まれる。最新メッセージが誰からでも良い）
      ④ classify_post_shipping_seller_message()が「発送後の単純なお礼・了承・挨拶」
         (NO_REPLY_CANDIDATE)と判定した場合でも、現段階（1st step）では対象から
         除外しない（suggested_reply["ai_no_reply_candidate"]を画面上のバッジ表示に
         のみ使う）。除外はユーザーが「返信不要」ボタンを押した場合（reply_skipped）
         のみで行う。
      ⑤ 配達後の受取評価忘れアラート:上記①〜④とは別に、_apply_arrival_reminders()が、
         ①で既にフリマ取引中=Trueに絞られた注文のうち、Access「日常」の到着日から
         ARRIVAL_REMINDER_THRESHOLD_DAYS日（既定3日、日付単位）以上経過している
         ものを、未返信メッセージの有無や最新送信者に関係なく強制的に追加する
         （取引完了になればフリマ取引中=Trueから外れるため、母集団①の時点で
         自動的に対象から消える。上限日数は設けない）。ただし最新メッセージの
         reply_skippedがTrueの場合（返品・キャンセル等のトラブル対応中の案件を
         ユーザーが「返信不要」ボタンで一時的に除外した場合）は、フリマ取引中を
         変更せずとも強制表示しない。新しい出品者メッセージが届けば自動的に
         再び対象になる（メッセージの無い「無言発送」のケースは従来どおり対象）。

    is_shippedの根拠（発送済みの肯定的証拠）は全サイト共通でAccess日常.eBayステータス
    （is_shipped_status()）。メルカリも含め、trx.vendor_purchase.statusは参照しない
    （trx.vendor_purchase廃止に伴い統一）。

    戻り値: [
        {
            "vendor_name": str,
            "vendor_item_id": str,
            "product_names": [str, ...],     # 同一注文IDに複数商品がある場合は全件
            "transaction_url": str|None,
            "seller_name": str,             # 履歴中の出品者メッセージの sender_name（無ければNone）
            "latest_message": {同形式の辞書}|None,
            "history": [同形式の辞書, ...],  # message_no昇順
            "suggested_reply": {"text": str, "source": str|None, "template_key": str|None,
                                 "ai_no_reply_candidate": bool},  # AIが「発送後の単純なお礼・了承・挨拶」と判定した印（1st step。一覧からの除外には使わず、画面上のバッジ表示にのみ使う）
            "can_skip": bool,               # 「返信不要」ボタンを表示してよいか
            "arrival_date_text": str|None,  # "YYYY/MM/DD"（Access日常.到着日が分かる場合のみ）
            "days_since_arrival": int|None, # 到着日からの経過日数（日付単位）。3以上で受取評価忘れアラート対象
        },
        ...
    ]
    最新メッセージが新しい順（updated_atが無いものは末尾）で返す。
    """
    active_orders = fetch_active_orders(access_frontend_conn)

    target_order_ids = [
        oid for oid, info in active_orders.items()
        if info["vendor_name"] in TARGET_VENDOR_NAMES
    ]

    items = []

    if target_order_ids:
        items = _build_pending_seller_message_items(sql_conn, active_orders, target_order_ids)

    _apply_arrival_reminders(sql_conn, active_orders, items)

    items.sort(key=lambda it: it["latest_message"]["message_no"] if it["latest_message"] else -1, reverse=True)
    return items


def _build_pending_seller_message_items(sql_conn, active_orders, target_order_ids):
    """
    fetch_pending_seller_messages()の従来ロジック（対象抽出条件①〜⑤）本体。

    【2026-09-10 trx.vendor_purchase廃止に伴い変更】target_order_ids自体が既に
    「日常.フリマ取引中=True（各サイトの直近の巡回で現在取引中と確認できたもの）」に
    限定されているため、ここでメルカリだけ別途「取引完了」を除外する必要が無くなった
    （取引完了・キャンセル等でサイトの取引中一覧から外れた注文は、次回の
    sync_flema_active_orders()でフラグがOFFになり、そもそもtarget_order_idsに
    含まれなくなる）。is_shippedもメルカリ含め全サイト共通でAccess日常.eBayステータス
    から判定する（trx.vendor_purchase.statusは参照しない）。
    """
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
        if suggested_reply["template_key"] == "shipped_2":
            include = True

        # 【2026-09-15 2nd stepへ変更】AI判定「返信不要候補」
        # （suggested_reply["ai_no_reply_candidate"]）の取引は、対応一覧から自動的に
        # 除外する。これは表示条件（include）だけの変更であり、
        # trx.vendor_message.reply_skippedは一切更新しない（DBは変更しない）。
        # メッセージ送信も行わない。ai_no_reply_candidateはdetermine_suggested_reply()が
        # 最新の出品者メッセージ本文から毎回計算する値のため、新しい出品者メッセージが
        # 届けば最新メッセージが変わり、その新しい内容で改めてAI判定される
        # （人による確認が必要な内容と判定されればFalseに戻り、自然に一覧へ再表示される）。
        # ai_no_reply_candidateはtemplate_key=="shipped_2"（条件B）とは同時に成立しない
        # （determine_suggested_reply()の設計上、shipped_2はai_no_reply_candidate=Falseで
        # しか返らない）ため、条件Bで含めた取引を誤って除外することはない。
        # 【2026-09-10 1st stepへ戻す】から方針転換。「配達後の受取評価忘れアラート」
        # （_apply_arrival_reminders()の強制表示）はこの除外の対象外
        # （到着後の受取評価忘れは、メッセージ内容やAI判定に関係なく引き続き表示する）。
        if suggested_reply["ai_no_reply_candidate"]:
            include = False
            print(f"[messages] AI判定NO_REPLY_CANDIDATEのため一覧から除外: "
                  f"vendor_name={vendor_name} vendor_item_id={vendor_item_id}")

        if not include:
            continue

        items.append(_build_message_item(
            vendor_name, vendor_item_id, order_info["product_names"], is_shipped, history, suggested_reply
        ))

    return items


def _build_transaction_url(vendor_name, vendor_item_id):
    builder = TRANSACTION_URL_BUILDERS.get(vendor_name)
    return builder(vendor_item_id) if builder else None


def _build_message_item(vendor_name, vendor_item_id, product_names, is_shipped, history, suggested_reply):
    """
    /messages表示用のitem辞書を1件分組み立てる。

    【2026-09-11 重複除去】_build_pending_seller_message_items()と
    _apply_arrival_reminders()の両方に、can_skipの算出式とitem辞書の各フィールド
    組み立てが一字一句同一のコードとして重複していたため、挙動を一切変えずに
    ここへ統合した（ステージ2）。latest_message・can_skip・seller_nameはhistoryから
    ここで計算する（呼び出し元は渡す必要が無い）。呼び出し元が別の目的（含めるか
    どうかの判定・reply_skippedによる強制表示スキップの判定）でhistory[-1]を
    先に参照していても、ここでの再計算は同じ入力から同じ結果を返すだけなので
    問題ない。arrival_date_text/days_since_arrivalは常にNoneで初期化する
    （到着日情報はfetch_pending_seller_messages()経由で_apply_arrival_reminders()が
    新規追加分・既存分の両方へ後から一括付与するため、ここでは関与しない）。
    """
    latest = history[-1] if history else None
    # 返信不要ボタンは、実際にフラグを立てられる対象（最新メッセージが出品者で、
    # まだ「返信不要」にされていない場合）にのみ表示する。
    can_skip = bool(latest and latest["sender_type"] == "出品者" and not latest["reply_skipped"])
    seller_messages = [m for m in history if m["sender_type"] == "出品者"]

    return {
        "vendor_name": vendor_name,
        "vendor_item_id": vendor_item_id,
        "product_names": product_names,
        "transaction_url": _build_transaction_url(vendor_name, vendor_item_id),
        "seller_name": seller_messages[-1]["sender_name"] if seller_messages else None,
        "is_shipped": is_shipped,
        "latest_message": latest,
        "history": history,
        "suggested_reply": suggested_reply,
        "can_skip": can_skip,
        "arrival_date_text": None,
        "days_since_arrival": None,
    }


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
# 既にこの趣旨の返信を送信済みかどうかの判定に使うキーワード（両方を含む場合のみ一致）。
# 【2026-09-11修正（段階B）】従来はOR（いずれか一方でも部分一致）だったため、無関係な
# メッセージが偶然どちらか一方のキーワードだけ含む場合に誤って「送信済み」と判定される
# リスクがあった（例: 「到着を楽しみに待ってます」とだけ書かれた、テンプレ由来ではない
# 独立したメッセージ）。TEMPLATE_SHIPPEDは元々両方のフレーズを含む文面のため、AND
# （両方とも含む場合のみ一致）へ厳格化しても、テンプレそのもの・軽微な編集を含む送信は
# 引き続き検出できる（実データ335件で検証済み、判定結果への影響なし）。2つのフレーズの
# 順序は問わない（zero-widthの先読みのため、一致した位置の文字列は消費しない）。
_TEMPLATE_SHIPPED_DETECT_RE = re.compile(r"(?=.*到着を楽しみに)(?=.*受取通知)", re.DOTALL)

# 出品者の最新メッセージの「意味」による分類（AI・gpt-4o-mini）。
# 【2026-09-08】以前は「発送完了の連絡かどうか」を正規表現（_SHIPPED_COMPLETE_MESSAGE_RE）
# で判定していたが、実例が出るたびに表現の追加・調整が必要になり
# （m37241994132「発送（投函）いたしました」、m50492038090「発送しましたら」等）、
# 個別の言い回しを列挙し続ける保守負債になっていた。また「商品状態の追加説明＋
# 問題なければ発送する」といった、購入者の確認・判断が必要な内容（実例
# m96926753890）を機械的なキーワード列挙だけで安全に見分けるのは困難なため、
# 正規表現ではなくメッセージ全体の意味をAIに分類させる方式に変更した。
# apps/etc/fetch_messages_ebay.py の analyze_price_negotiation()/_extract_offer() と
# 同じ gpt-4o-mini・JSON応答・例外時は安全側にフォールバックするパターンを踏襲する。
#
# 将来的に「フリマ情報取得」実行時、安全に定型返信できるものだけを自動送信し、
# それ以外は/messagesに残して人間が判断する運用を見据えているため、
# 「自動返信しても安全か」を最優先の基準とする。分類に自信が持てない場合・
# API呼び出し自体が失敗した場合は、必ずHUMAN_REVIEW（定型文なし）にする
# （本来定型文でよいものを人間判断に回す誤りは許容するが、本来人間が確認すべき
# ものに定型文をセットする誤りは避ける、という優先順位）。
MERCARI_REPLY_CLASSIFICATION_MODEL = "gpt-4o-mini"
MERCARI_REPLY_CATEGORIES = ("SAFE_GREETING", "SHIPPED", "HUMAN_REVIEW")

# メッセージ本文(str)→分類結果(str)。/messages画面はGETのたびに未返信の全取引を
# 再分類していたため、送信直後のlocation.reload()等で同じ本文へ何度もAPI課金・
# 待ち時間（実測1件あたり約1〜1.5秒）が発生していた。本文が変わらない限り結果も
# 変わらない前提でプロセス内メモリにキャッシュする（プロセス再起動でクリアされる想定）。
# API例外時のフォールバック（HUMAN_REVIEW）は「その時点でAPIが呼べなかった」だけの
# 結果のため、キャッシュしない（次回呼び出し時に再度APIを試みられるようにする）。
_MERCARI_REPLY_CLASSIFICATION_CACHE_MAX_SIZE = 500
_mercari_reply_classification_cache: dict = {}


def classify_mercari_seller_message(message_body: str) -> str:
    """
    出品者の最新メッセージ本文をAIで分類する。戻り値はMERCARI_REPLY_CATEGORIESのいずれか。
      - SAFE_GREETING: 単純な挨拶・お礼・発送予定等、購入者の確認や判断を必要としない
      - SHIPPED: 発送完了の連絡
      - HUMAN_REVIEW: 質問・確認依頼・商品状態の追加説明・説明との相違・欠品や傷等の
        問題・キャンセルや変更の相談・購入者の了承や判断が必要な内容、その他判断に
        迷うもの全般
    """
    if message_body in _mercari_reply_classification_cache:
        return _mercari_reply_classification_cache[message_body]

    from openai import OpenAI

    client = OpenAI()
    try:
        resp = client.chat.completions.create(
            model=MERCARI_REPLY_CLASSIFICATION_MODEL,
            max_tokens=50,
            messages=[{"role": "user", "content":
                "Classify this Japanese message from a Mercari (フリマ) seller to a buyer into "
                "exactly one category:\n"
                '- "SAFE_GREETING": a simple greeting, thanks, or shipping-schedule notice that '
                "requires no confirmation, decision, or answer from the buyer.\n"
                '- "SHIPPED": the seller states the item has already been shipped/handed to a carrier. '
                "This still counts as SHIPPED even if the message also includes the routine standard "
                "closing asking the buyer to leave a receipt rating once the item arrives "
                "(e.g. \"到着後にご確認・受け取り評価をお願いいたします\") — that is a boilerplate "
                "closing on almost every shipping notice, not a decision the buyer must make now. "
                "IMPORTANT: only use SHIPPED when the seller states the item has ALREADY been "
                "shipped/handed to a carrier (past tense, e.g. \"発送しました\", \"発送済みです\", "
                "\"本日発送完了しました\"). A future-tense promise to ship later "
                "(e.g. \"今夜発送します\", \"今夜発送させて頂きます\", \"明日発送予定です\", "
                "\"発送準備中です\") is NOT shipped yet — classify those as SAFE_GREETING "
                "(a shipping-schedule notice), never SHIPPED.\n"
                '- "HUMAN_REVIEW": anything that asks the buyer a question, requests confirmation or '
                "approval about something OTHER than the routine post-arrival rating, describes an "
                "additional item condition/defect/discrepancy from the listing, offers or discusses a "
                "cancellation or change, or is otherwise not a plain greeting or shipping notice.\n\n"
                'If you are not confident, always answer "HUMAN_REVIEW".\n'
                'Reply JSON only: {"category": "SAFE_GREETING"|"SHIPPED"|"HUMAN_REVIEW"}\n\n'
                f"Message: {message_body}"
            }]
        )
        raw = resp.choices[0].message.content
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        category = data.get("category") if isinstance(data, dict) else None
        result = category if category in MERCARI_REPLY_CATEGORIES else "HUMAN_REVIEW"
    except Exception:
        return "HUMAN_REVIEW"

    if len(_mercari_reply_classification_cache) >= _MERCARI_REPLY_CLASSIFICATION_CACHE_MAX_SIZE:
        _mercari_reply_classification_cache.pop(next(iter(_mercari_reply_classification_cache)))
    _mercari_reply_classification_cache[message_body] = result
    return result


# ------------------------------------------------------------
# 発送後メッセージの「AI返信不要候補」判定（確認運用）
# ------------------------------------------------------------
# 【2026-09-10 確認運用として追加】発送お礼(shipped_2)を送信済みの後、出品者から
# 追加で届いたメッセージ（受取評価の催促・単純なお礼への返信・無言の追加連絡等）は、
# 従来determine_suggested_reply()のどの分岐にも該当せず、常に「候補なし」（人が
# 内容を読んで都度判断）になっていた。ここでは既存の一覧・除外の仕組みには一切
# 手を加えず、あくまで「AIが単純なお礼・了承・挨拶と判定した」という印を
# suggested_replyへ付加するだけにとどめる（DBスキーマ変更なし。この印は
# messages_blueprint.py の /api/messages/send 等には一切使われず、
# 「返信不要」ボタン（trx.vendor_message.reply_skipped）を人が押した場合のみ、
# 従来どおり対象から除外される）。
POST_SHIPPING_NO_REPLY_MODEL = "gpt-4o-mini"
POST_SHIPPING_NO_REPLY_CATEGORIES = ("NO_REPLY_CANDIDATE", "HUMAN_REVIEW")

# 【2026-09-15 プロンプトv2】実機不具合m54422719245・m83916728734を受けて修正。
# 旧プロンプトは「受取評価への言及があれば理由を問わず一律HUMAN_REVIEW」だったため、
# 「評価は急がなくてよい／期限内でよい」という、購入者へのプレッシャーを解除する
# だけの言い回し（催促の逆）まで、単に"評価"という単語に触れているというだけで
# HUMAN_REVIEWに巻き込んでいた。プロンプトv2では、催促（急かす・依頼するニュアンス）
# と、催促の否定（急がなくてよい旨の許容表現）を明確に区別する。

# 【2026-09-15 プロンプトv3】v2ではm83916728734（純粋な許容表現のみ）は解消したが、
# m54422719245（「ゆっくりで構いません」＋「ご確認でき次第受取評価していただけたら
# と思います」のように、急がせない表現に加えて“受取評価そのものへの軽い依頼”が
# 伴うケース）はHUMAN_REVIEWのままだった。v3では、この特定の組み合わせ
# （①急がせない表現があり、②依頼内容が「到着・商品確認後の受取評価」だけに限定され、
# ③それ以外の質問・別の依頼・催促・期限を強調する表現・苦情・配送や商品のトラブルが
# 一切無い）場合に限り、NO_REPLY_CANDIDATEに含める。「お願いします」等の依頼表現を
# 全般的に返信不要へ広げるものではなく、あくまで受取評価１点への軽い依頼＋急がせない
# 表現の組み合わせだけを対象にする（実データ98件でのバックテストで、この組み合わせに
# 該当しない依頼系メッセージ（例:「こちらのご評価もお願い致します。」等）が
# NO_REPLY_CANDIDATE化しないことを確認済み）。
# それ以外の分岐（質問・別の依頼・トラブル・返品・苦情・期限を強調する催促・判断に
# 迷う場合は必ずHUMAN_REVIEW、API失敗時もHUMAN_REVIEW）は変更しない。
#
# プロンプト文面の作成過程で、次の2つの過剰一般化（危険な誤判定）を検出し、明示的な
# 禁止例をプロンプトへ追記して修正済み（実データ98件のバックテストで再検証済み）。
#   - 「ご確認いただければと思います」のように"評価"という語を含まない一般的な
#     "確認"依頼まで、文脈から受取評価だと推測して誤って対象に含めてしまう挙動
#     → (ii)の適用には「評価」「受取評価」等の語を本文が明示していることを必須化した。
#   - 「お待ちしております」のように、急かさない旨の明示的な言い回しを伴わずに
#     「（評価を）待っている」と述べるだけの文を、許容表現(i)と誤認する挙動
#     → 「お待ちしております」単体は許容表現に当たらない旨を明記した。
#
# 【2026-09-15 プロンプトv4】v3を実データ98件でバックテストした結果、単純な
# 「よろしくお願いします」「了解しました」等（他に一切内容を伴わない）14件までもが
# HUMAN_REVIEW側へ巻き戻ってしまっていた（v3のcase(a)の説明が短く、モデルが自信を
# 持てなかったとみられる）。v4ではcase(a)の記述を具体化し、お礼・了承・結び言葉を
# 組み合わせただけの文面（他に新情報・依頼・状況報告を一切伴わない）の実例を明示して
# 復元した。一方、次の4種は依然としてHUMAN_REVIEW対象であることを明示的に固定した
# （実データで人の確認が必要と判断した7件のうち、発送状況の連絡・独自の値引き提案・
# トラブルへの言及・対象不明な「お手隙の際」等がこれに該当）：
#   - 発送・配達状況の連絡（例:「発送完了しております」）→ 発送状況バーとshipped_2の
#     送信状況によって返信要否が変わるため、常にHUMAN_REVIEW
#   - 迷惑・不手際・過去の行き違いへの言及や謝罪
#   - 値引き等の新たな申し出・約束（単純なお礼・了承ではない）
#   - 「お手隙の際で大丈夫です」等、対象が本文中に明示されない「急がなくてよい」表現
#
# 【2026-09-15 決定的セーフガード追加】「受け取り」「受領」「受取」を含みながら直後に
# 「評価」と続かない（＝受取評価そのものと確実には言い切れない）曖昧な文面は、
# AIがNO_REPLY_CANDIDATEと分類した場合でも実行時にHUMAN_REVIEWへ強制的に倒す。
# 本番はAIの1回の判定結果でtrx.vendor_messageの当該メッセージが一覧から除外され、
# しかもその結果はプロンプトバージョン単位でプロセス内キャッシュされるため、
# プロンプト調整だけでは「稀にNO_REPLY_CANDIDATEに揺れた結果がそのままキャッシュ
# され続ける」リスクを完全には排除できない（実機検証で、この種の曖昧な文言について
# 3〜5回に1回程度、AIの判定が割れることを確認済み）。正規表現によるこのガードは
# AIの応答を受け取った直後・キャッシュに保存する前に適用するため、ゆれた結果が
# キャッシュされること自体を防げる。「受取評価」「受け取り評価」「受領評価」等、
# 直後に「評価」と続く場合はこのガードの対象外（曖昧ではないため）。
_AMBIGUOUS_RECEIPT_WORDING_RE = re.compile(r"受(?:け取り|領|取)(?!評価)")
#
# プロンプトを変更すると同じ本文でも過去のキャッシュ結果（旧プロンプトでの判定）が
# 混在してしまうため、キャッシュキーにプロンプトバージョンを含め、旧バージョンの
# キャッシュ内容とは独立させる。
POST_SHIPPING_NO_REPLY_PROMPT_VERSION = "v9-2026-09-15"

# classify_mercari_seller_message()と同じ理由（同じ本文への再判定・API課金を防ぐ）で
# 別キャッシュを持つ（判定基準・プロンプトが異なるため、既存キャッシュとは共有しない）。
# キーは(プロンプトバージョン, message_body)のタプル。
_POST_SHIPPING_NO_REPLY_CACHE_MAX_SIZE = 500
_post_shipping_no_reply_cache: dict = {}


def classify_post_shipping_seller_message(message_body: str) -> str:
    """
    発送お礼を送信済みの取引で、出品者から追加で届いた最新メッセージをAIで分類する。
    戻り値はPOST_SHIPPING_NO_REPLY_CATEGORIESのいずれか。
      - NO_REPLY_CANDIDATE:
          (a) 単純なお礼・了承・挨拶のみ、
          (b) 「評価は急がなくてよい・期限内でよい」という購入者へのプレッシャーを
              解除するだけの連絡（他の内容を一切伴わない）、
          (c) 急がせない表現があり、かつ依頼内容が「到着・商品確認後の受取評価」
              だけに限定され、それ以外の質問・別の依頼・催促・期限を強調する表現・
              苦情・配送や商品のトラブルが一切無いもの
        のいずれか。(c)は「お願いします」等の依頼表現全般を対象にするものではなく、
        受取評価１点への軽い依頼＋急がせない表現の組み合わせだけに限定する。
      - HUMAN_REVIEW: 質問、受取評価以外の依頼、期限を強調する催促、発送トラブル、
        返送、未着、住所の問題、苦情・不満、その他判断が曖昧なもの全般。分類に
        自信が持てない場合・API呼び出し失敗時も必ずこちらにする（本来HUMAN_REVIEWで
        良いものをNO_REPLY_CANDIDATEに誤判定する方を避ける、という優先順位は
        classify_mercari_seller_message()と同じ）。

    【決定的セーフガード】AIがNO_REPLY_CANDIDATEと判定した場合でも、本文に「受け取り」
    「受領」「受取」を含み、かつ直後に「評価」と続かない（＝受取評価そのものと確実には
    言い切れない）場合は、この関数がAI応答を受け取った直後・キャッシュへ保存する前に
    強制的にHUMAN_REVIEWへ上書きする（_AMBIGUOUS_RECEIPT_WORDING_RE）。AIの1回の判定が
    稀に揺れてNO_REPLY_CANDIDATE側に倒れても、それがそのままキャッシュされて一覧除外に
    使われる事態を防ぐための正規表現ベースの安全側固定であり、HUMAN_REVIEW→
    NO_REPLY_CANDIDATEへの上書きは行わない（安全側にしか倒さない）。
    """
    cache_key = (POST_SHIPPING_NO_REPLY_PROMPT_VERSION, message_body)
    if cache_key in _post_shipping_no_reply_cache:
        return _post_shipping_no_reply_cache[cache_key]

    from openai import OpenAI

    client = OpenAI()
    try:
        resp = client.chat.completions.create(
            model=POST_SHIPPING_NO_REPLY_MODEL,
            max_tokens=50,
            messages=[{"role": "user", "content":
                "This is a Japanese flea-market (フリマ) transaction. The seller has already "
                "shipped the item and the buyer has already sent the routine shipping "
                "acknowledgement reply. Classify this ADDITIONAL message the seller sent "
                "afterward into exactly one category:\n"
                '- "NO_REPLY_CANDIDATE": ONLY one of these three cases:\n'
                "  (a) The message consists ENTIRELY of thanks, a simple acknowledgement/agreement "
                "(了解しました/承知いたしました/かしこまりました), and/or a closing goodwill phrase "
                "(よろしくお願いします系) — alone or combined with each other and/or with a brief "
                "generic personal remark that adds no new information, request, or status "
                "(e.g. \"心配性ですので助かります\") — with NOTHING else added. Being longer or "
                "combining several such elements does not disqualify it. Examples of case (a): "
                "\"よろしくお願いします。\", \"了解しました。\", \"かしこまりました、よろしくお願いいたし"
                "ます。\", \"ご丁寧にありがとうございます。承知いたしました。引き続きよろしくお願いいた"
                "します。\", \"了解しました。こちらもなにかあれば対応させて頂きますので取引終了までよろ"
                "しくお願いします。\", \"丁寧にご連絡頂き有難うございます。心配性ですので助かります。承"
                "知致しました。\".\n"
                "  (b) The seller reassures the buyer that leaving the receipt rating/evaluation "
                "late, within the deadline, or whenever convenient is completely fine and there "
                "is no need to rush, using an EXPLICIT no-rush/no-need-to-hurry phrase (e.g. "
                "\"評価は期限内にいただければ全く構いません\", \"受取評価はゆっくりで大丈夫です\", \"急が"
                "なくて結構です\", \"無理なさらず\"), with nothing else asked, requested, or "
                "mentioned.\n"
                "  (c) A no-rush / take-your-time expression (e.g. \"ゆっくりで構いません\") is "
                "combined with a request that EXPLICITLY names the receipt rating/evaluation "
                "itself using a word that contains 評価 (e.g. 評価, 受け取り評価, 受取評価) — plain "
                "\"受け取り\"/\"受領\" WITHOUT 評価, or a generic \"ご確認ください\"/\"確認していただけ"
                "れば\", do NOT count as naming it (e.g. \"受け取りなどお願い致します\" does not "
                "qualify — it never says 評価) — and asks "
                "for NOTHING beyond that rating, after the item arrives and has been checked "
                "(e.g. \"ゆっくりで構いませんので、ご確認でき次第受取評価していただけたらと思います\").\n"
                '- "HUMAN_REVIEW": ANYTHING else. In particular, always answer HUMAN_REVIEW — '
                "regardless of how friendly or polite the tone is, and even if it superficially "
                "resembles (a)/(b)/(c) — when the message contains ANY of:\n"
                "  * a statement about shipping/delivery status or timing (e.g. \"発送完了しており"
                "ます\", \"もう少しで届くと思われます\", \"本日発送しました\") — this is status "
                "information, not mere thanks;\n"
                "  * an apology or reference to any inconvenience, mistake, complaint, or past "
                "friction (e.g. \"ご迷惑をおかけし申し訳ありませんでした\", \"催促したみたいで申し訳あり"
                "ません\");\n"
                "  * a new offer, promise, discount, or any other substantive statement beyond "
                "thanks/acknowledgement/closing (e.g. \"次回リピーター割引致します\");\n"
                "  * a \"take your time\"/\"no rush\" expression whose grammatical target is NOT "
                "explicitly the rating/evaluation itself — e.g. it refers to \"取引\" (the "
                "transaction in general), an unspecified \"お手隙の際\"/\"ご都合\"/\"タイミング\", or "
                "anything other than a word like 評価/受取評価/受け取り評価 (examples: \"お手隙の際で"
                "大丈夫です\" alone, \"はい！お手隙の際によろしくお願いいたします✨\", \"取引急いでおり"
                "ませんので、都合でご対応ください\") — none of these satisfy (b) or (c), which both "
                "require the no-rush wording to explicitly attach to the rating/evaluation, not to "
                "the transaction/timing/convenience in general;\n"
                "  * a phrase that merely expresses the seller is waiting/looking forward to the "
                "rating or receipt notification (e.g. \"お待ちしております\") WITHOUT an explicit "
                "no-rush phrase — this is anticipation, not the reassurance required by (b);\n"
                "  * a question, any request other than the narrow receipt-rating case in (c), a "
                "rating request/reminder that lacks a clear no-rush qualifier (e.g. \"こちらのご評"
                "価もお願い致します。\" alone), wording that presses/emphasizes a deadline (simply "
                "stating a deadline is fine either way, as in (b), does not count as pressing "
                "it), a return/refund discussion, the item not arriving, an address/delivery "
                "problem, or anything else ambiguous or not confidently (a)/(b)/(c).\n\n"
                'If you are not confident it clearly falls under NO_REPLY_CANDIDATE, always '
                'answer "HUMAN_REVIEW".\n'
                'Reply JSON only: {"category": "NO_REPLY_CANDIDATE"|"HUMAN_REVIEW"}\n\n'
                f"Message: {message_body}"
            }]
        )
        raw = resp.choices[0].message.content
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        category = data.get("category") if isinstance(data, dict) else None
        result = category if category in POST_SHIPPING_NO_REPLY_CATEGORIES else "HUMAN_REVIEW"
    except Exception:
        return "HUMAN_REVIEW"

    # 決定的セーフガード（AIの応答直後・キャッシュ保存前に適用）。「受け取り」「受領」
    # 「受取」を含みながら直後に「評価」と続かない曖昧な文面は、AIがNO_REPLY_CANDIDATEと
    # 判定していても強制的にHUMAN_REVIEWへ倒す。これにより、ゆれた判定結果が
    # キャッシュされること自体を防ぐ（HUMAN_REVIEW→NO_REPLY_CANDIDATEへの上書きは
    # 行わない。安全側にしか倒さない）。
    if result == "NO_REPLY_CANDIDATE" and _AMBIGUOUS_RECEIPT_WORDING_RE.search(message_body):
        result = "HUMAN_REVIEW"

    if len(_post_shipping_no_reply_cache) >= _POST_SHIPPING_NO_REPLY_CACHE_MAX_SIZE:
        _post_shipping_no_reply_cache.pop(next(iter(_post_shipping_no_reply_cache)))
    _post_shipping_no_reply_cache[cache_key] = result
    return result


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
# 過剰な意味解析はせず、「お願い」に続く一般的な表記揺れ（します/いたします/致します/
# 申し上げます/申しあげます）のみ拾う。
# 【2026-09-11修正】実機不具合m31821957285: 出品者メッセージ「よろしくお願い申し上げます」が
# 旧regex（お願いします|お願いいたします|お願い致します の完全な単語単位の列挙）に
# 一致せず、first_reply_onegai（「こちらこそ、お手数を…」）が選ばれるべきところ
# first_reply_plainになっていた。個別の言い回しを都度追加し続ける保守負債を避けるため、
# 「お願い」＋末尾の丁寧語バリエーションという構造でまとめて拾う形にした。
_ONEGAI_RE = re.compile("お願い(?:します|いたします|致します|申し上げます|申しあげます)")


def determine_suggested_reply(history: list, is_shipped: bool) -> dict:
    """
    history: message_no昇順の会話履歴（sender_type='出品者'|'購入者'）。空のこともある
             （無言発送で一度もメッセージが交換されていない場合）。
    is_shipped: 呼び出し元が判定した「発送済みの肯定的証拠」（メッセージ本文以外）。
                全サイト共通でAccess日常.eBayステータス（is_shipped_status()）由来
                （2026-09-10 trx.vendor_purchase廃止に伴い統一）。

    優先順位:
      1. is_shipped（ステータス由来の発送済みの肯定的証拠）があり、まだ発送のお礼
         (shipped_2)を送っていなければ「shipped_2」を提案する。ここではメッセージ
         内容は見ない（ステータスだけで確定できるため、AI分類を呼ぶまでもない）。
         shipped_2はこのステップでのみ発行する（下記ステップ2ではAIがSHIPPEDと
         分類しても発行しない）。
      2. 上記に該当せず（＝is_shippedがFalse。サイトの詳細画面はまだ発送前と
         判定している）、出品者から最初の取引メッセージを受けた後、まだこちらが
         返信していない場合は、その出品者の最新メッセージをclassify_mercari_seller_message()
         でAI分類する。
           - SAFE_GREETING: 初回挨拶（「お願いします」系の有無で1-1/1-2に分岐）
           - SHIPPED／HUMAN_REVIEW（分類失敗・不明含む）: 定型文なし（人が判断する）
         【2026-09-11修正】以前はここでAIがSHIPPEDと分類した場合もshipped_2を
         発行していたが、AI判定はメッセージ本文の意味解釈にすぎず、サイトの詳細
         画面の配送状態（is_shipped）と食い違うことがある。実機不具合z613953926
         （出品者の「今夜発送させて頂きます」という未来形メッセージをAIがSHIPPEDと
         誤判定し、実際は発送前なのにshipped_2＝発送済みのお礼文が提案されていた）
         を受けて廃止した。shipped_2は詳細画面で発送済み・配送中・配達済みを
         確認できた場合（ステップ1）だけで使う。AI判定よりサイトの配送ステータスを
         優先し、判断不能・食い違いの場合も発送済みには倒さない。
         【2026-09-08】以前は正規表現（_SHIPPED_COMPLETE_MESSAGE_RE）で発送完了の
         連絡かどうかを判定し、それ以外は無条件で初回挨拶を出していたが、実例
         m96926753890「商品状態の追加説明＋問題なければ発送します＋気になるなら
         キャンセル可能です」のような、購入者の確認・判断が必要な内容にまで初回挨拶
         （「よろしくお願いします」）を自動セットしてしまう問題があった。将来
         「フリマ情報取得」実行時に安全な相手だけ自動返信する運用を見据え、
         個別キーワードの列挙ではなくメッセージ全体の意味をAIに分類させ、
         「自動返信しても安全か」を最優先の基準にする。分類に自信が持てない場合・
         API呼び出し失敗時は必ずHUMAN_REVIEW（定型文なし）にフォールバックする。
      3. 上記いずれにも該当しない場合（既に最初のメッセージへ返信済みで、その後も
         出品者から新しいメッセージが届いている状態）は候補なし（人が判断する）が、
         is_shipped かつ 発送お礼を送信済み の場合のみ、その最新メッセージを
         classify_post_shipping_seller_message()で追加分類する（確認運用、
         2026-09-10追加）。
           - NO_REPLY_CANDIDATE: suggested_reply["ai_no_reply_candidate"]をTrueにする
             （一覧からは除外しない。画面上に「AI判定：返信不要候補」の印を出すだけ）。
           - HUMAN_REVIEW（分類失敗・不明含む）: 通常表示（印を付けない）。
      4. 出品者からの実質メッセージが無ければ候補なし（人が判断する）。

    出品者の最初の実質メッセージより前に買い手自身が送ったメッセージ（値引き交渉・
    購入完了のお礼等）は、取引開始後の初回挨拶への返信としては数えない
    （実例m50492038090）。

    【2026-09-01 スタンプによる誤判定対策】メルカリのスタンプメッセージは実際の
    絵柄・文言を取得できないため、mercari_get_messages()が固定のプレースホルダー
    文字列"スタンプ"を本文として保存する（既存コードの既知の仕様）。このプレースホルダーは
    出品者からの通常のテキストメッセージと同じsender_type='出品者'の1行として
    historyに残るため、実質的な出品者メッセージとしては数えない
    （本文が"スタンプ"と完全一致するものだけを除外し、推測による除外は行わない）。

    【2026-09-11 3層分離（ステージ3）】上記の優先順位（判定結果）は一切変更せず、
    内部実装のみを「①取引状態の判定(_assess_shipment_state)」「②会話順序の判定
    (_assess_conversation_progress)」「③文章分類と定型文選択
    (_classify_first_contact_message / _classify_post_shipping_message)」の3層へ
    分離した。この関数自体は3層の結果を組み合わせて優先順位どおりに分岐するだけの
    orchestratorとする。
    """
    state = _assess_shipment_state(history, is_shipped)
    conversation = _assess_conversation_progress(history)

    # ステップ1: 発送済みの肯定的証拠（ステータス由来）
    if state["is_shipped"] and not state["already_sent_shipped_thanks"]:
        return {"text": TEMPLATE_SHIPPED, "source": "template", "template_key": "shipped_2",
                "ai_no_reply_candidate": False}

    # ステップ2: 出品者から最初の実質メッセージを受けた後、まだこちらが返信していない
    # 場合のみ、その最新メッセージをAI分類する（出品者の最初の実質メッセージより前に
    # 買い手自身が送ったメッセージは、初回挨拶への返信としては数えない）。
    # 【2026-09-11修正】ただし、既に発送お礼(shipped_2)を送信済み(already_sent_shipped_thanks)
    # の場合は、たとえ出品者の最初の実質メッセージより後に購入者発言が見当たらなくても
    # このステップに入らない（下のステップ3の発送後フローに進ませる）。実機不具合
    # l1244030615・m93160436190で確認: 無言発送でこちらがshipped_2を先に送信済み
    # （message_no=1）で、出品者のメッセージ（message_no=2、shipped_2への単なる了承の
    # 返礼）が唯一の出品者メッセージだった場合、「shipped_2より後に購入者発言が無い」＝
    # 「初回メッセージにまだ返信していない」と誤判定され、本来は発送後の
    # classify_post_shipping_seller_message()（ステップ3、NO_REPLY_CANDIDATE）で
    # 拾うべきところを、取引開始直後の初回挨拶用AI分類(classify_mercari_seller_message)
    # が誤って適用され、first_reply_onegai/first_reply_plainが提案されていた。
    # already_sent_shipped_thanksは「発送お礼を既に送ったかどうか」そのものであり、
    # message_noの前後関係に依存しないため、この条件を優先して先に評価する。
    if conversation["meaningful_seller_messages"]:
        if (not conversation["replied_after_first_seller_message"]
                and not state["already_sent_shipped_thanks"]):
            seller_text = conversation["latest_seller_message"]["message_body"] or ""
            return _classify_first_contact_message(seller_text)

        # 【2026-09-10 確認運用として追加】既に最初のメッセージへ返信済み（＝発送お礼を
        # 送信済みで、その後もやり取りが続いている）場合、まだ自分が返信していない
        # 最新の出品者メッセージがあれば、それをAI判定する（対象一覧からは除外しない。
        # あくまで「AI判定：返信不要候補」という印をsuggested_replyに付けるだけ）。
        if state["is_shipped"] and state["already_sent_shipped_thanks"]:
            if not conversation["replied_after_latest_seller_message"]:
                seller_text = conversation["latest_seller_message"]["message_body"] or ""
                return _classify_post_shipping_message(seller_text)

    # ステップ3: それ以外は候補なし
    return {"text": "", "source": None, "template_key": None, "ai_no_reply_candidate": False}


def _assess_shipment_state(history: list, is_shipped: bool) -> dict:
    """
    ①取引状態の判定。会話の順序・メッセージの文章内容の分類には一切関与しない。

    is_shippedは呼び出し元（Access日常.eBayステータス由来、is_shipped_status()）から
    そのまま受け取ったものをそのまま返す。already_sent_shipped_thanksは、購入者(自分)の
    過去メッセージのいずれかが発送お礼の定型文(TEMPLATE_SHIPPED)と同趣旨かどうかを、
    _TEMPLATE_SHIPPED_DETECT_RE（"到着を楽しみに"と"受取通知"の両方を含むか、AND判定）で
    判定したもの。
    """
    own_messages = [m for m in history if m["sender_type"] == "購入者"]
    already_sent_shipped_thanks = any(
        _TEMPLATE_SHIPPED_DETECT_RE.search(m["message_body"] or "") for m in own_messages
    )
    return {
        "is_shipped": is_shipped,
        "already_sent_shipped_thanks": already_sent_shipped_thanks,
    }


def _assess_conversation_progress(history: list) -> dict:
    """
    ②会話順序の判定。取引状態(is_shipped等)・メッセージの文章内容の分類には一切関与しない。

    meaningful_seller_messagesは、出品者メッセージのうちメルカリのスタンプ
    プレースホルダー("スタンプ"と完全一致する本文)を除いたもの。
    first/latest_seller_messageはその先頭/末尾（無ければNone）。
    replied_after_first/latest_seller_messageは、対応するメッセージのmessage_noより
    後に購入者(自分)発言があるかどうか（出品者の最初の実質メッセージより前に買い手自身が
    送ったメッセージ=値引き交渉・購入完了のお礼等は、初回挨拶への返信としては数えない。
    実例m50492038090）。meaningful_seller_messagesが空の場合はいずれもFalse
    （呼び出し元はmeaningful_seller_messagesが空でない場合のみこれらの値を使う）。
    """
    seller_messages = [m for m in history if m["sender_type"] == "出品者"]
    meaningful_seller_messages = [
        m for m in seller_messages if (m["message_body"] or "") != "スタンプ"
    ]

    first_seller_message = meaningful_seller_messages[0] if meaningful_seller_messages else None
    latest_seller_message = meaningful_seller_messages[-1] if meaningful_seller_messages else None

    def _replied_after(seller_message):
        if seller_message is None:
            return False
        return any(
            own["sender_type"] == "購入者" and own["message_no"] > seller_message["message_no"]
            for own in history
        )

    return {
        "meaningful_seller_messages": meaningful_seller_messages,
        "first_seller_message": first_seller_message,
        "latest_seller_message": latest_seller_message,
        "replied_after_first_seller_message": _replied_after(first_seller_message),
        "replied_after_latest_seller_message": _replied_after(latest_seller_message),
    }


def _classify_first_contact_message(seller_text: str) -> dict:
    """
    ③文章分類と定型文選択（取引開始直後の初回挨拶用）。

    出品者からの最初の実質メッセージに、まだこちらが返信していない場合の返信案を、
    classify_mercari_seller_message()の分類結果から決定する。
      - SAFE_GREETING: 初回挨拶（「お願いします」系の有無でonegai/plainに分岐）
      - SHIPPED／HUMAN_REVIEW（分類失敗・不明含む）: 定型文なし（人が判断する）
    【2026-09-11修正】ここでAIがSHIPPEDと分類しても、shipped_2は発行しない
    （呼び出し元はis_shippedがFalseか、既に発送お礼を送信済みの場合しかこの関数を
    呼ばない。AI判定よりサイトの配送ステータスを優先する。実機不具合z613953926
    「今夜発送させて頂きます」という未来形メッセージをAIがSHIPPEDと誤判定した件を参照）。
    """
    category = classify_mercari_seller_message(seller_text)

    if category == "SAFE_GREETING":
        if _ONEGAI_RE.search(seller_text):
            return {"text": TEMPLATE_FIRST_REPLY_ONEGAI, "source": "template", "template_key": "first_reply_onegai",
                    "ai_no_reply_candidate": False}
        return {"text": TEMPLATE_FIRST_REPLY_PLAIN, "source": "template", "template_key": "first_reply_plain",
                "ai_no_reply_candidate": False}

    # HUMAN_REVIEW、またはSHIPPED（サイトはまだ発送前と判定しているため採用しない）: 定型文なし
    return {"text": "", "source": None, "template_key": None, "ai_no_reply_candidate": False}


def _classify_post_shipping_message(seller_text: str) -> dict:
    """
    ③文章分類と定型文選択（発送お礼送信済み後の追加メッセージ用、確認運用）。

    発送お礼(shipped_2)を送信済み後、出品者から追加で届いた最新メッセージに、まだ
    こちらが返信していない場合の判定を、classify_post_shipping_seller_message()の
    分類結果から決定する（2026-09-10追加。1st step＝一覧からの除外はせず、画面上の
    「AI返信不要候補」バッジ表示のみに使う）。
      - NO_REPLY_CANDIDATE: ai_no_reply_candidateをTrueにする
      - HUMAN_REVIEW（分類失敗・不明含む）: 通常表示（印を付けない）
    """
    category = classify_post_shipping_seller_message(seller_text)
    if category == "NO_REPLY_CANDIDATE":
        return {"text": "", "source": None, "template_key": None, "ai_no_reply_candidate": True}
    return {"text": "", "source": None, "template_key": None, "ai_no_reply_candidate": False}


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

# trx.vendor_message の vendor_name。既存システムで使われている表記に合わせる。
# 【2026-09-10 trx.vendor_purchase廃止に伴い変更】以前はtrx.vendor_purchase /
# trx.vendor_purchase_unregisteredでも使っていたが、この2テーブルへの書き込みは
# 廃止した（Access日常「フリマ取引中」フラグへ統合。詳細はsync_flema_active_orders()
# 参照）。trx.vendor_purchase_unregisteredテーブル自体はまだ削除していない。
MERCARI_VENDOR_NAME = "メルカリ"

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
# 【2026-09-10 trx.vendor_purchase廃止に伴い削除】sync_arrival_status_to_access()
# （trx.vendor_purchase→日常への同期。到着日はメルカリ自身のステータス検知日への
# フォールバックだったが、これは「配達済みを確認した日」であり実際の配達日ではない
# ため廃止した。実際の配達日はsync_carrier_tracking_to_daily()が引き続き担当する）と
# sync_unregistered_daily_items()（trx.vendor_purchase_unregisteredへ記録するだけで
# 日常への自動追加は行わなかった）は、sync_flema_active_orders()に統合されたため
# 削除した。trx.vendor_purchase_unregisteredテーブル自体はまだ削除していない。


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

# 【2026-09-10 追加】上記のうち「受取評価をしました」（購入者側の対応完了）だけを
# 区別して判定するための文言。出品者側がまだ評価していない・「取引が完了しました」に
# なっていなくても、購入者側の対応は既に完了しているため、フリマ取引中の対象からは
# 外してよい（is_shippedやAccessの他の列には影響しない、フラグだけの話）。
MERCARI_RECEIPT_RATED_HEADING = "受取評価をしました"


def mercari_is_receipt_rated(driver) -> bool:
    """
    現在表示中のMercari取引ページの見出し（[data-testid="status-heading"]）が
    「受取評価をしました」で始まるかどうかを、完全な文言一致で判定する
    （本文メッセージ等、曖昧な情報からは判定しない）。mercari_get_raw_status()と
    同じ要素を見るが、「取引が完了しました」とは区別して判定する専用関数。
    """
    status_heading = driver.find_elements(By.CSS_SELECTOR, '[data-testid="status-heading"]')
    if not status_heading:
        return False
    return status_heading[0].text.strip().startswith(MERCARI_RECEIPT_RATED_HEADING)


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


# 【2026-09-10 trx.vendor_purchase廃止に伴い削除】mercari_is_transaction_completed()/
# mercari_mark_transaction_completed()（「取引が完了しました」の個別確認・
# trx.vendor_purchase.statusへの記録）は、元々どこからも呼ばれていない未使用コード
# だった（mercari_main()の通常巡回には組み込まれておらず、"/messages候補の事後確認
# としてのみ使う想定"のまま実装されずにいた）。trx.vendor_purchase自体を廃止する
# ため削除した。取引完了の判定は、現在は日常.フリマ取引中フラグ（各サイトの
# 「取引中」一覧に無ければ自動的にOFFになる）で代替している。

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

# 【2026-09-15追加】送信前（本文入力・送信ボタンクリック前）の新着確認用メッセージ取得
# （mercari_get_messages→_capture_mercari_api_responsesのCDP経由API捕捉）が稀に
# タイムアウトする実機不具合を受けて追加（実例: m73359763614ほか計5件が
# 「APIレスポンスの捕捉に失敗しました: ['transaction_messages/get_messages']」で
# 自動送信エラーになった）。この時点ではまだ本文入力・送信ボタンクリックの
# いずれも行っておらず実サイトへの送信は発生していないため、二重送信の心配なく
# ページ再読み込みからやり直せる。本文入力・送信クリック後は絶対にリトライしない
# （このブロックの外では一切リトライ処理を追加しない）。
MERCARI_PRE_SEND_MESSAGE_FETCH_MAX_ATTEMPTS = 3
MERCARI_PRE_SEND_MESSAGE_FETCH_RETRY_WAIT_SEC = 2.0


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

    # 送信前（本文入力・送信ボタンクリックより前）の新着確認用メッセージ取得。
    # まだ実サイトへの送信は一切発生していない段階のため、失敗してもページ再読み込みから
    # 安全にやり直せる。最大MERCARI_PRE_SEND_MESSAGE_FETCH_MAX_ATTEMPTS回まで試行し、
    # それでも失敗した場合のみ従来通り例外を送出する（この後の本文入力・送信ボタン
    # クリック以降のリトライは一切行わない＝二重送信防止ロジックはここでは変更しない）。
    current_messages = None
    last_fetch_error = None
    for attempt in range(1, MERCARI_PRE_SEND_MESSAGE_FETCH_MAX_ATTEMPTS + 1):
        try:
            current_messages = mercari_get_messages(driver, order_id)
            break
        except Exception as e:
            last_fetch_error = e
            print(f"[send] {order_id}: 送信前メッセージ取得に失敗しました"
                  f"（{attempt}/{MERCARI_PRE_SEND_MESSAGE_FETCH_MAX_ATTEMPTS}回目、"
                  f"送信前のためページ再読み込みしてリトライします）: {e}")
            if attempt < MERCARI_PRE_SEND_MESSAGE_FETCH_MAX_ATTEMPTS:
                time.sleep(MERCARI_PRE_SEND_MESSAGE_FETCH_RETRY_WAIT_SEC)
                driver.get(f"https://jp.mercari.com/transaction/{order_id}")
                time.sleep(4)

    if current_messages is None:
        raise RuntimeError(
            f"送信前メッセージ取得に{MERCARI_PRE_SEND_MESSAGE_FETCH_MAX_ATTEMPTS}回失敗したため中断しました"
            f"（本文入力・送信ボタンクリックのいずれも行っていません）: {last_fetch_error}"
        )

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


# 【2026-09-10 trx.vendor_purchase廃止に伴い削除】determine_status()（trx.vendor_purchase.
# status用のメルカリ専用ステータス合成）は、全サイト共通のdetermine_access_status()に
# 統合された（mercari_main()はupdate_daily_purchase_status()を直接呼ぶ）。

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
def mercari_main(wanted_ids=None):
    """
    wanted_ids: 指定時は「注文ID(取引URL末尾)」の集合で対象を絞り込む（テスト用）。
    Noneの場合（通常運用）は、直接の呼び出し元が明示的に絞り込みを指定していないため、
    コマンドライン引数(--item-ids)からの指定を後方互換として受け付ける
    （python furima_purchase.py --item-ids m12345 のような従来の直接手動実行用）。
    """
    if wanted_ids is None:
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
            # 【2026-09-14修正】チェックボックス操作失敗時(2026-09-10)と同じ理由でreturnを
            # raiseへ変更。returnのままだと1件も処理していないのにmain()側で"success"と
            # 記録されてしまう（自動送信フェーズが古いデータのまま実行されるリスクもある）。
            raise RuntimeError("ログインされていない可能性があるため、処理を中断しました")

        try:
            _ensure_in_transaction_checkbox_checked(driver)
        except Exception as e:
            print(f"NG: 「取引中の商品」チェックボックスの操作に失敗しました: {e}")
            print("NG: 取得対象が確定できないため、今回の処理を中断します。")
            # 【2026-09-10 実機不具合を受けて修正】旧実装はここでreturnしていたが、
            # main()側はrun_func()が例外を出さなければ"success"と記録するため、
            # 実際には1件も処理していないのに実行結果が成功扱いになっていた
            # （実例: m85217087095が新規購入・メッセージ受信後の巡回で欠落したが、
            # runner_statusはMercari=successのまま記録されていた）。呼び出し元で
            # 正しくerrorとして記録されるよう、returnではなく例外を送出する。
            raise RuntimeError(
                "「取引中の商品」チェックボックスの操作に失敗したため、"
                "取得対象を確定できず処理を中断しました"
            ) from e

        if wanted_ids is not None:
            # 指定IDは「取引中の商品」一覧に既に出てこない（評価済み等で外れた）ことがあるため、
            # 一覧経由ではなく取引URLを直接組み立てる（テスト用の絞り込み時のみ）。
            transaction_urls = [f"https://jp.mercari.com/transaction/{iid}" for iid in wanted_ids]
        else:
            transaction_urls = _collect_transaction_urls(driver)

        print(f"取引URL数: {len(transaction_urls)}")
        print()

        # 【2026-09-10 trx.vendor_purchase廃止に伴い追加】一覧取得が完全に成功した直後
        # （＝ここまで例外なく到達できた時点）でのみ、日常.フリマ取引中を店舗単位で
        # 一括更新する。--item-ids指定時（テスト用の絞り込み）は「現在取引中の全件」
        # ではないため、フラグ更新は行わない（他の現在取引中の注文を誤ってOFFに
        # してしまうため）。
        if wanted_ids is None:
            mercari_active_ids = [get_vendor_item_id(u) for u in transaction_urls]
            sync_result = sync_flema_active_orders(access_conn, MERCARI_VENDOR_NAME, mercari_active_ids)
            print(f"日常フリマ取引中フラグ更新: リセット{sync_result['reset']}行, "
                  f"ON{sync_result['updated']}件, 新規追加{sync_result['created']}件")
            print()

        failed_ids = []
        for url in transaction_urls:

            # URLからの注文ID抽出は文字列操作のみで失敗しないため、リトライの外で1回だけ行う
            # （2026-09-14追加のfailed_ids記録に使う）。
            vendor_item_id = get_vendor_item_id(url)

            # 【2026-09-14追加】この取引だけをITEM_COLLECTION_MAX_ATTEMPTS回まで試行する。
            last_error = None
            for attempt in range(1, ITEM_COLLECTION_MAX_ATTEMPTS + 1):
                try:

                    driver.get(url)
                    _wait_for_transaction_page_ready(driver)

                    # 【2026-09-10 追加】購入者側の対応（受取評価）が既に完了している場合、
                    # 出品者側がまだ「取引が完了しました」にしておらず取引中の商品一覧に
                    # 残っていても、フリマ取引中の対象からは外す。完全な文言一致
                    # （status-headingの「受取評価をしました」）でのみ判定し、他のDOM要素・
                    # 本文メッセージからは判定しない。フラグ以外（eBayステータス・到着日・
                    # メッセージ履歴）は変更しないため、以降の更新処理はスキップする。
                    if mercari_is_receipt_rated(driver):
                        inactivated = mark_flema_inactive(access_conn, MERCARI_VENDOR_NAME, vendor_item_id)
                        print(url)
                        print(f"受取評価をしました（購入者側の対応完了）を検出。"
                              f"日常のフリマ取引中をFalseにしました（{inactivated}行）。"
                              "eBayステータス・到着日・メッセージ履歴は変更していません。")
                        print()
                        last_error = None
                        break

                    raw_status       = mercari_get_raw_status(driver)
                    item_name        = get_item_name(driver)
                    purchase_datetime, purchase_price = get_purchase_info(driver)
                    messages         = mercari_get_messages(driver, vendor_item_id)
                    has_seller_message = any(m["is_from_seller"] for m in messages)
                    tracking_number, carrier = mercari_get_tracking_info(driver)

                    with conn.cursor() as cur:
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

                    # 日常に注文IDのレコードが無い場合（仕入入力忘れ／私用購入で未入力。
                    # 通常はsync_flema_active_orders()が既に作成済みのはずだが、念のため）は
                    # 新規追加する。既存レコードがある場合は何もしない。
                    created = ensure_daily_record(
                        access_conn, MERCARI_VENDOR_NAME, vendor_item_id,
                        item_name, purchase_datetime.date(), purchase_price
                    )

                    # eBayステータスは日常テーブルへ直接反映する（trx.vendor_purchase経由は廃止）。
                    daily_updated = update_daily_purchase_status(access_conn, vendor_item_id, raw_status, has_seller_message)

                    # 送り状番号・配送会社は日常テーブルへ直接保存する。
                    update_daily_tracking_info(access_conn, vendor_item_id, tracking_number, carrier)

                    print(url)
                    print(f"raw_status={raw_status}  price={purchase_price}  messages={len(messages)}  "
                          f"item={item_name[:30]}  日常更新={'OK' if daily_updated else '対象行なし'}")
                    if created:
                        print(f"日常: 新規レコード追加（注文ID={vendor_item_id}）")
                    print()

                    last_error = None
                    break

                except Exception as e:
                    last_error = e
                    if attempt < ITEM_COLLECTION_MAX_ATTEMPTS:
                        print(f"WARN: {url} の処理に失敗しました（{attempt}/{ITEM_COLLECTION_MAX_ATTEMPTS}回目）。"
                              f"リトライします: {e}")
                        time.sleep(ITEM_COLLECTION_RETRY_WAIT_SEC)

            if last_error is not None:
                print(url)
                print(f"ERROR: {last_error}")
                print()
                failed_ids.append(vendor_item_id)

        sync_carrier_tracking_to_daily(access_conn)

        # 【2026-09-14追加】リトライしても失敗した取引IDを呼び出し元へ返す
        # （自動送信フェーズが、今回収集に失敗した取引を対象から除外するために使う）。
        return failed_ids

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

# 【2026-09-09 実機確認済みで追加】購入一覧は最初の50件しか表示されず、
# 「もっと見る」を押すごとに追加で読み込まれる（実機確認: 1回目クリックで50→100件）。
# 購入日が古い（PURCHASE_LIST_HISTORY_DAYS日より前の）取引まで遡って展開する。
PURCHASE_LIST_HISTORY_DAYS = 120
# 安全弁（想定外の無限ループを防ぐ上限。実機では2回程度で全履歴の末尾に到達した）。
PURCHASE_LIST_EXPAND_MAX_CLICKS = 30
PURCHASE_LIST_EXPAND_WAIT_SEC = 10.0
# 実機確認済み: 「もっと見る」は<span>要素（button/aタグではない）で、
# JSのclick()で反応する（Selenium座標クリックは使わない、他サイトと同じ方針）。
PURCHASE_LIST_MORE_BUTTON_XPATH = "//span[normalize-space(text())='もっと見る']"

PAYPAY_ORDER_ID_RE = re.compile(r"/item/([A-Za-z0-9]+)/trade/buyer")

# 一覧に表示されるステータス文言（実機確認済み）
STATUS_BEFORE_SHIP = "発送待ち"
STATUS_IN_TRANSIT = "商品が到着したら評価をしてください"

# 【2026-09-09 実機確認済みで変更】一覧のscrape対象は、まだ追跡が必要な
# 「発送待ち」「商品が到着したら評価をしてください」の2状態のみに限定する
# （ホワイトリスト方式）。旧実装は「取引完了」という文字列を含む場合のみ除外する
# 除外リスト方式だったため、「取引キャンセル」（"取引完了"という文字列を含まない）や
# 「未評価の場合は評価してください」（到着済みで評価待ちのみ、追跡不要）が
# 対象に紛れ込み、不要に個別取引ページを開いてしまっていた（実機確認済み、
# 50件中: 取引完了30件・商品が到着したら評価をしてください12件・発送待ち5件・
# 未評価の場合は評価してください2件・取引キャンセル1件）。
# 未知の状態文言（将来サイト側の表示が変わった場合等）も、このリストに
# 無ければ自動的に対象外になる（rakuma_get_raw_status()等と同じ「未確認の
# 状態は安全側に倒す」方針に合わせた）。
ACTIVE_LIST_STATUSES = (STATUS_BEFORE_SHIP, STATUS_IN_TRANSIT)

# 個別取引ページ（詳細）で判定する。一覧の文言は「商品が到着したら評価をしてください」
# 「未評価の場合は評価してください」など複数のバリエーションがあり一覧文言だけでは
# 判定しきれないことを実機確認したため、詳細ページの共通見出しで判定する。
DETAIL_ARRIVED_MARKER = "受取評価をして取引を完了してください"
DETAIL_BEFORE_SHIP_MARKER = "出品者の発送をお待ちください"
# 【2026-09-09 実機確認済みで追加】発送済み・配送中（まだ買い手が受取評価していない）
# 状態の本文文言。この状態が未実装だったため、該当する取引はraw_status取得時に
# 例外になり、update_daily_purchase_status()（Access更新）まで到達できず、
# 日常.eBayステータスが空欄のまま更新されない不具合があった
# （実例: z592951146, z653620242, z678003294, z618303236）。
DETAIL_SHIPPED_MARKER = "商品の到着をお待ちください"

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
def _expand_purchase_list(driver) -> int:
    """
    購入一覧の「もっと見る」を、末尾の取引の購入日が PURCHASE_LIST_HISTORY_DAYS 日より
    前に達するまで、またはこれ以上読み込めなくなるまでクリックする。
    クリック回数を返す。

    【2026-09-09 実機確認済み】「もっと見る」の<span>要素は、読み込める全履歴を
    読み終えた後もDOM上に残り続け、is_displayed()もTrueのままだった（ボタンが
    消える・disabledになる、といった見た目上の変化が無い）。そのため、ボタンの
    表示有無ではなく「クリック後に実際にリンク数が増えたか」で終了判定する
    （実機確認済み: このアカウントは全履歴107件で、2回目のクリック以降は
    クリックしてもリンク数が増えなかった）。
    """
    cutoff = datetime.now() - timedelta(days=PURCHASE_LIST_HISTORY_DAYS)
    clicks = 0

    for _ in range(PURCHASE_LIST_EXPAND_MAX_CLICKS):
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/trade/buyer']")
        if not links:
            break

        last_lines = (links[-1].text or "").split("\n")
        if len(last_lines) >= 2:
            try:
                if parse_japanese_datetime(last_lines[1]) < cutoff:
                    break  # 120日より前まで遡れた
            except ValueError:
                pass  # 日時をパースできない場合は無視し、件数増加の有無だけで判断する

        more_els = [
            el for el in driver.find_elements(By.XPATH, PURCHASE_LIST_MORE_BUTTON_XPATH)
            if el.is_displayed()
        ]
        if not more_els:
            break  # ボタン自体が無い（このアカウントでは通常発生しない想定だが念のため）

        prev_count = len(links)
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_els[0])
        driver.execute_script("arguments[0].click();", more_els[0])
        clicks += 1

        deadline = time.time() + PURCHASE_LIST_EXPAND_WAIT_SEC
        grew = False
        while time.time() < deadline:
            if len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/trade/buyer']")) > prev_count:
                grew = True
                break
            time.sleep(0.3)
        if not grew:
            break  # クリックしても増えない＝全履歴を読み込み済み

    return clicks


def collect_active_transactions(driver, retries: int = 6, interval: float = 2.0):
    """
    「もっと見る」を購入日がPURCHASE_LIST_HISTORY_DAYS日前に達するまで展開したうえで、
    ステータスが ACTIVE_LIST_STATUSES（発送待ち／商品が到着したら評価をしてください）の
    いずれかに一致する取引URL・生ステータス文言の一覧を返す。
    戻り値: [(url, list_status_text), ...]

    【2026-08-31 メルカリの実機不具合を受けて共通の考え方を適用】retries回待っても
    フィルタ前の生リンクが1件も見つからない場合は、0件と決めつけず例外を送出する
    （一覧ページの描画・読み込みに失敗している可能性があるため）。ACTIVE_LIST_STATUSES
    のフィルタで結果的に0件になるのは正常な状態のため区別する（その場合は生リンクは
    見つかっている）。

    【2026-09-09 実機確認済みで方式変更】旧実装は「もっと見る」を押さず最初に
    表示されている範囲だけを対象にしており、かつ「取引完了」という文字列を含む場合
    のみ除外する除外リスト方式だったため、「取引キャンセル」（"取引完了"という文字列を
    含まない）や「未評価の場合は評価してください」（到着済みで評価待ちのみ、追跡不要）
    が対象に紛れ込み、不要に個別取引ページを開いてしまっていた（実機確認済み、50件中:
    取引完了30件・商品が到着したら評価をしてください12件・発送待ち5件・
    未評価の場合は評価してください2件・取引キャンセル1件）。
    ACTIVE_LIST_STATUSESに明示的に含まれる状態だけを対象にするホワイトリスト方式に
    変更し、あわせて_expand_purchase_list()で直近120日分まで一覧を展開してから
    対象を抽出するようにした。判定は各カード（a要素）のテキストの最終行（状態文言）で
    行う（class名はビルドごとに変わりうるハッシュ値のため使わない。実機確認済み）。
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

    expand_clicks = _expand_purchase_list(driver)
    links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/trade/buyer']")
    print(f"購入一覧展開: 「もっと見る」{expand_clicks}回クリック（展開後 生リンク{len(links)}件）")

    results = []
    seen = set()
    for link in links:
        href = link.get_attribute("href")
        text = link.text or ""
        if not href or href in seen:
            continue
        list_status = text.strip().split("\n")[-1].strip() if text.strip() else ""
        if list_status not in ACTIVE_LIST_STATUSES:
            continue  # 取引完了・取引キャンセル・未評価の場合は評価してください・未知の状態は対象外
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

    【2026-09-09 実機確認済みで追加】「発送済み・配送中」状態は本文の
    DETAIL_SHIPPED_MARKER（「商品の到着をお待ちください」）で判定する。この状態が
    未実装だったため、該当する取引はここで例外になり、paypay_main()側の
    update_daily_purchase_status()（Access更新）まで到達できず、日常.eBayステータスが
    空欄のまま更新されない不具合があった（実例: z592951146, z653620242, z678003294,
    z618303236）。到着済み(DETAIL_ARRIVED_MARKER)が確認できる場合はそちらを優先する。
    """
    body_text = driver.find_element(By.TAG_NAME, "body").text
    if DETAIL_ARRIVED_MARKER in body_text:
        return "☆出荷可能"
    if DETAIL_BEFORE_SHIP_MARKER in body_text:
        return "発送前"
    if DETAIL_SHIPPED_MARKER in body_text:
        return "発送済み"

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

    def _confirm_via_fresh_messages():
        """
        【2026-09-09 案A追加】クリック自体は既に実行済みのため、CDPでのレスポンス捕捉に
        失敗しても実サイトへの送信自体は成功している場合がある（実機不具合 z677826054で
        確認: 実際には送信済みなのに捕捉失敗によりok=False・DB保存スキップとなり、
        trx.vendor_messageに反映されず画面からも消えなかった）。
        再クリックはせず、paypay_get_messages()で現在のDOMを再取得し、送信前の件数から
        ちょうど1件だけ増えていて、かつ最新メッセージが自分の発言で送信本文と完全一致する
        場合のみ「実際には送信成功していた」とみなす（厳密一致・件数一致の両方を要求し、
        既存の別メッセージを誤って成功と判定しないようにする）。
        """
        time.sleep(1.0)
        fresh = paypay_get_messages(driver, seller_name)
        if len(fresh) == expected_count + 1:
            last = fresh[-1]
            if last["sender_type"] == "購入者" and (last["message_body"] or "") == reply_text:
                return fresh
        return None

    if target_request_id is None:
        fallback_messages = _confirm_via_fresh_messages()
        if fallback_messages is not None:
            return {"ok": True, "error": None, "reason": None, "new_messages": fallback_messages}
        return {"ok": False, "error": "送信リクエスト（POST .../message）の発生を確認できませんでした"
                                       "（クリックが反映されていない可能性があります）。再送信はせず、必ず状況を確認してください。",
                "reason": None, "new_messages": None}

    if status != 200 or not body or "thread" not in body:
        fallback_messages = _confirm_via_fresh_messages()
        if fallback_messages is not None:
            return {"ok": True, "error": None, "reason": None, "new_messages": fallback_messages}
        return {"ok": False, "error": f"PayPayフリマ側の実レスポンスで送信成功を確認できませんでした"
                                       f"（status={status}, body={body}）",
                "reason": None, "new_messages": None}

    # 実送信成功。paypay_get_messages()を再実行し、送信分を含む最新の全件を返す
    # （通常scrapeと同じsave_vendor_messages()経路でDB保存できるようにするため）。
    # 【2026-09-11追加】ここでの再取得はCDP（ChromeDriverセッション）経由のDOM操作であり、
    # 実送信自体は直前のCDPレスポンス捕捉（status==200かつthread有）で既に確定済み。
    # 実機不具合z676630458で確認: 送信直後にChromeDriverとの接続が切れ
    # （ConnectionRefusedError等）、ここが例外を送出して/api/messages/send全体が
    # 500エラーになった。実送信は成功しているため、ここで例外が起きても「送信失敗」
    # として扱ってはならない（二重送信を誘発するため）。取得に失敗した場合は
    # new_messages=NoneのままON=Trueを返し、呼び出し元でのDB保存はスキップさせる
    # （次回の通常scrape、または手動確認で反映される）。
    def _refetch_reflects_sent_message(messages):
        return (len(messages) == expected_count + 1
                and messages[-1]["sender_type"] == "購入者"
                and (messages[-1]["message_body"] or "") == reply_text)

    time.sleep(1.0)
    try:
        fresh_messages = paypay_get_messages(driver, seller_name)
    except Exception as e:
        return {"ok": True,
                "error": f"実サイトへの送信は成功しましたが、送信後の最新メッセージ取得に失敗しました（{e}）。"
                         "再送信はしないでください。内容は次回の自動取得または実サイト確認で反映されます。",
                "reason": "post_send_refetch_failed", "new_messages": None}

    # 【2026-09-14追加】再取得自体は例外なく完了しても、DOM更新がまだ間に合っておらず
    # 送信分を含まない古い件数のまま返ってくることがある（実機不具合z640111984: 再取得
    # 件数が送信前と変わらず、送信済みの購入者発言が欠落したままDBへ保存され、実際には
    # 送信済みなのに/messagesにも送信ボタンが残ったままになっていた）。件数が+1件増えて
    # いて最新が自分の送信文と完全一致することを確認できない場合は、もう少し待って
    # 1回だけ再取得する（再クリックはしない、DOM再取得のみ）。それでも確認できなければ
    # new_messages=Noneを返し、呼び出し元のフォールバック保存に委ねる。
    if not _refetch_reflects_sent_message(fresh_messages):
        time.sleep(2.0)
        try:
            fresh_messages = paypay_get_messages(driver, seller_name)
        except Exception as e:
            return {"ok": True,
                    "error": f"実サイトへの送信は成功しましたが、送信後の最新メッセージ取得に失敗しました（{e}）。"
                             "再送信はしないでください。内容は次回の自動取得または実サイト確認で反映されます。",
                    "reason": "post_send_refetch_failed", "new_messages": None}
        if not _refetch_reflects_sent_message(fresh_messages):
            return {"ok": True,
                    "error": "実サイトへの送信は成功しましたが、再取得したメッセージ一覧に送信分が"
                             "まだ反映されていません。再送信はしないでください。内容は次回の自動取得"
                             "または実サイト確認で反映されます。",
                    "reason": "post_send_refetch_stale", "new_messages": None}

    return {"ok": True, "error": None, "reason": None, "new_messages": fresh_messages}


# ------------------------------------------------------------
# メイン
# ------------------------------------------------------------
def paypay_main(wanted_ids=None):
    """wanted_ids: 指定時は注文IDの集合で対象を絞り込む（テスト用。mercari_main()と同じ考え方）。"""
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

        if wanted_ids is not None:
            # 指定IDは一覧に出てこないことがあるため、一覧経由ではなく取引URLを
            # 直接組み立てる（テスト用の絞り込み時のみ。mercari_main()と同じ方針）。
            transactions = [
                (f"https://paypayfleamarket.yahoo.co.jp/item/{iid}/trade/buyer", "(テストモード直接指定)")
                for iid in wanted_ids
            ]
        else:
            transactions = collect_active_transactions(driver)
        print(f"取引URL数(取引完了を除く): {len(transactions)}")
        print()

        # 【2026-09-10 trx.vendor_purchase廃止に伴い追加】一覧取得が完全に成功した
        # 直後（＝ここまで例外なく到達できた時点）でのみ、日常.フリマ取引中を
        # 店舗単位で一括更新する。--item-ids指定時（テスト用の絞り込み）は
        # 「現在取引中の全件」ではないため、フラグ更新は行わない（mercari_main()と
        # 同じ理由。他の現在取引中の注文を誤ってOFFにしてしまうため）。
        if wanted_ids is None:
            paypay_active_ids = [paypay_get_order_id(u) for u, _status_text in transactions]
            sync_result = sync_flema_active_orders(access_conn, PAYPAY_VENDOR_NAME, paypay_active_ids)
            print(f"日常フリマ取引中フラグ更新: リセット{sync_result['reset']}行, "
                  f"ON{sync_result['updated']}件, 新規追加{sync_result['created']}件")
            print()

        failed_ids = []
        for url, list_status_text in transactions:
            # URLからの注文ID抽出は文字列操作のみで失敗しないため、リトライの外で1回だけ行う。
            order_id = paypay_get_order_id(url)

            # 【2026-09-14追加】この取引だけをITEM_COLLECTION_MAX_ATTEMPTS回まで試行する。
            last_error = None
            for attempt in range(1, ITEM_COLLECTION_MAX_ATTEMPTS + 1):
                try:
                    driver.get(url)
                    time.sleep(4)

                    if not order_id:
                        print(url)
                        print("ERROR: 注文IDを取得できませんでした")
                        print()
                        last_error = None
                        break

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

                    last_error = None
                    break

                except Exception as e:
                    last_error = e
                    if attempt < ITEM_COLLECTION_MAX_ATTEMPTS:
                        print(f"WARN: {url} の処理に失敗しました（{attempt}/{ITEM_COLLECTION_MAX_ATTEMPTS}回目）。"
                              f"リトライします: {e}")
                        time.sleep(ITEM_COLLECTION_RETRY_WAIT_SEC)

            if last_error is not None:
                print(url)
                print(f"ERROR: {last_error}")
                print()
                failed_ids.append(order_id)

        sync_carrier_tracking_to_daily(access_conn)

        # 【2026-09-14追加】リトライしても失敗した取引IDを呼び出し元へ返す。
        return failed_ids

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
# 【2026-09-09 実機確認済み】発送済み・配送中（まだ買い手が受取確認していない）状態の
# .status-title文言。ステータスが空欄のまま処理が中断されていた2件（実例:
# f10489d4dfb3adc4feae232705f5d1b8, 547b7e5fcbb8c61e22af152317535045）はいずれも
# この文言だった（実機確認済み）。ARRIVED_MARKER（配送会社側の配達完了）とは別の
# 状態のため、判定順はARRIVED_MARKERの後（配達完了が確認できていれば到着済み優先）。
SHIPPED_MARKER = "受取確認と評価をしてください"

# 【2026-09-14追加】購入者側の受取評価は既に完了しており、出品者側の評価だけが
# 残っている状態の.status-title文言（実機確認済み、実例:
# 0b15ecb49ef103de0923a54623b7bbc7。「受取評価日：YYYY年M月D日」も併記される）。
# mercari_is_receipt_rated()と同じ考え方: 購入者側の対応は既に完了しているため
# 追跡不要とし、フリマ取引中の対象から外してよい（eBayステータス・到着日・
# メッセージ履歴・受取評価日のAccessへの転記は一切行わない、フラグだけの話）。
# 既存の他の状態判定（BEFORE_SHIP_MARKER・ARRIVED_MARKER・SHIPPED_MARKER）は変更しない。
RAKUMA_SELLER_RATING_PENDING_MARKER = "出品者からの評価をお待ちください"


def rakuma_is_buyer_response_complete(driver) -> bool:
    """
    現在表示中のラクマ取引ページの.status-title文言に
    RAKUMA_SELLER_RATING_PENDING_MARKER（「出品者からの評価をお待ちください」）が
    含まれるかどうかを判定する（rakuma_get_raw_status()の他のマーカー判定
    ＝BEFORE_SHIP_MARKER/SHIPPED_MARKERと同じ部分一致方針。実機では「受取評価日：
    YYYY年M月D日」等が同じ要素内に併記されることがあるため、完全一致ではなく
    部分一致にする。本文メッセージ等、曖昧な情報からは判定しない）。
    """
    status_title_els = driver.find_elements(By.CSS_SELECTOR, ".status-title")
    if not status_title_els:
        return False
    return RAKUMA_SELLER_RATING_PENDING_MARKER in status_title_els[0].text.strip()


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

    【2026-09-09 実機確認済みで追加】「発送済み・配送中」状態は.status-title=
    SHIPPED_MARKER（「受取確認と評価をしてください」）で判定する。この状態が
    未実装だったため、該当する取引はここで例外になり、rakuma_main()側の
    update_daily_purchase_status()（Access「連絡あり」/「【購入済】」の反映）まで
    到達できず、日常.eBayステータスが空欄のまま更新されない不具合があった
    （実例: f10489d4dfb3adc4feae232705f5d1b8, 547b7e5fcbb8c61e22af152317535045）。
    配送会社側の配達完了（ARRIVED_MARKER）が確認できる場合はそちらを優先する
    （買い手がまだ「受取確認」操作をしていなくても、配達自体は完了しているため）。
    """
    status_title_els = driver.find_elements(By.CSS_SELECTOR, ".status-title")
    status_title_text = status_title_els[0].text.strip() if status_title_els else ""

    if BEFORE_SHIP_MARKER in status_title_text:
        return "発送前"

    body_text = driver.find_element(By.TAG_NAME, "body").text
    if ARRIVED_MARKER in body_text:
        return "☆出荷可能"

    if SHIPPED_MARKER in status_title_text:
        return "発送済み"

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

    def _confirm_via_fresh_messages():
        """
        【2026-09-09 案A追加】クリック自体は既に実行済みのため、CDPでのレスポンス捕捉に
        失敗しても実サイトへの送信自体は成功している場合がある（PayPayフリマ側の実機不具合
        z677826054と同種の問題をラクマでも未然に防ぐため）。
        再クリックはせず、rakuma_get_messages()で現在のDOMを再取得し、送信前の件数から
        ちょうど1件だけ増えていて、かつ最新メッセージが自分の発言で送信本文と完全一致する
        場合のみ「実際には送信成功していた」とみなす（厳密一致・件数一致の両方を要求し、
        既存の別メッセージを誤って成功と判定しないようにする）。
        """
        time.sleep(1.0)
        fresh = rakuma_get_messages(driver, self_name)
        if len(fresh) == expected_count + 1:
            last = fresh[-1]
            if last["sender_type"] == "購入者" and (last["message_body"] or "") == reply_text:
                return fresh
        return None

    if target_request_id is None:
        fallback_messages = _confirm_via_fresh_messages()
        if fallback_messages is not None:
            return {"ok": True, "error": None, "reason": None, "new_messages": fallback_messages}
        return {"ok": False, "error": "送信リクエスト（POST .../comment/add）の発生を確認できませんでした"
                                       "（クリックが反映されていない可能性があります）。再送信はせず、必ず状況を確認してください。",
                "reason": None, "new_messages": None}

    if status != 200 or not isinstance(body, dict) or not body.get("result"):
        fallback_messages = _confirm_via_fresh_messages()
        if fallback_messages is not None:
            return {"ok": True, "error": None, "reason": None, "new_messages": fallback_messages}
        return {"ok": False, "error": f"ラクマ側の実レスポンスで送信成功を確認できませんでした"
                                       f"（status={status}, body={body}）",
                "reason": None, "new_messages": None}

    # 実送信成功。rakuma_get_messages()を再実行し、送信分を含む最新の全件を返す
    # （通常scrapeと同じsave_vendor_messages()経路でDB保存できるようにするため）。
    # 【2026-09-14追加】ここでの再取得失敗時にも実送信成功(ok=True)を維持する
    # （paypay_send_chat_message()の実機不具合z676630458と同じ理由。取得できなければ
    # new_messages=Noneを返し、呼び出し元でのDB保存はスキップさせる）。
    def _refetch_reflects_sent_message(messages):
        return (len(messages) == expected_count + 1
                and messages[-1]["sender_type"] == "購入者"
                and (messages[-1]["message_body"] or "") == reply_text)

    time.sleep(1.0)
    try:
        fresh_messages = rakuma_get_messages(driver, self_name)
    except Exception as e:
        return {"ok": True,
                "error": f"実サイトへの送信は成功しましたが、送信後の最新メッセージ取得に失敗しました（{e}）。"
                         "再送信はしないでください。内容は次回の自動取得または実サイト確認で反映されます。",
                "reason": "post_send_refetch_failed", "new_messages": None}

    # 【2026-09-14追加】再取得自体は例外なく完了しても、DOM更新がまだ間に合っておらず
    # 送信分を含まない古い件数のまま返ってくることがある（PayPayフリマの実機不具合
    # z640111984と同種の問題をラクマでも未然に防ぐため）。件数が+1件増えていて最新が
    # 自分の送信文と完全一致することを確認できない場合は、もう少し待って1回だけ
    # 再取得する（再クリックはしない、DOM再取得のみ）。それでも確認できなければ
    # new_messages=Noneを返し、呼び出し元のフォールバック保存に委ねる。
    if not _refetch_reflects_sent_message(fresh_messages):
        time.sleep(2.0)
        try:
            fresh_messages = rakuma_get_messages(driver, self_name)
        except Exception as e:
            return {"ok": True,
                    "error": f"実サイトへの送信は成功しましたが、送信後の最新メッセージ取得に失敗しました（{e}）。"
                             "再送信はしないでください。内容は次回の自動取得または実サイト確認で反映されます。",
                    "reason": "post_send_refetch_failed", "new_messages": None}
        if not _refetch_reflects_sent_message(fresh_messages):
            return {"ok": True,
                    "error": "実サイトへの送信は成功しましたが、再取得したメッセージ一覧に送信分が"
                             "まだ反映されていません。再送信はしないでください。内容は次回の自動取得"
                             "または実サイト確認で反映されます。",
                    "reason": "post_send_refetch_stale", "new_messages": None}

    return {"ok": True, "error": None, "reason": None, "new_messages": fresh_messages}


# ------------------------------------------------------------
# メイン
# ------------------------------------------------------------
def rakuma_main(wanted_ids=None):
    """wanted_ids: 指定時は注文IDの集合で対象を絞り込む（テスト用。mercari_main()と同じ考え方）。"""
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

        if wanted_ids is not None:
            # 指定IDは一覧に出てこないことがあるため、一覧経由ではなく取引URLを
            # 直接組み立てる（テスト用の絞り込み時のみ。mercari_main()と同じ方針）。
            transaction_urls = [f"https://fril.jp/transaction?item_id={iid}" for iid in wanted_ids]
        else:
            transaction_urls = collect_in_progress_transaction_urls(driver)
        print(f"取引URL数(取引中のみ): {len(transaction_urls)}")
        print()

        # 【2026-09-10 trx.vendor_purchase廃止に伴い追加】一覧取得が完全に成功した
        # 直後（＝ここまで例外なく到達できた時点）でのみ、日常.フリマ取引中を
        # 店舗単位で一括更新する。--item-ids指定時（テスト用の絞り込み）は
        # 「現在取引中の全件」ではないため、フラグ更新は行わない（mercari_main()と
        # 同じ理由。他の現在取引中の注文を誤ってOFFにしてしまうため）。
        if wanted_ids is None:
            rakuma_active_ids = [rakuma_get_order_id(u) for u in transaction_urls]
            sync_result = sync_flema_active_orders(access_conn, RAKUMA_VENDOR_NAME, rakuma_active_ids)
            print(f"日常フリマ取引中フラグ更新: リセット{sync_result['reset']}行, "
                  f"ON{sync_result['updated']}件, 新規追加{sync_result['created']}件")
            print()

        failed_ids = []
        for url in transaction_urls:
            # URLからの注文ID抽出は文字列操作のみで失敗しないため、リトライの外で1回だけ行う。
            order_id = rakuma_get_order_id(url)

            # 【2026-09-14追加】この取引だけをITEM_COLLECTION_MAX_ATTEMPTS回まで試行する。
            last_error = None
            for attempt in range(1, ITEM_COLLECTION_MAX_ATTEMPTS + 1):
                try:
                    driver.get(url)
                    time.sleep(4)

                    if not order_id:
                        print(url)
                        print("ERROR: 注文ID(item_id)を取得できませんでした")
                        print()
                        last_error = None
                        break

                    # 【2026-09-14追加】購入者側の受取評価は既に完了しており、出品者側の
                    # 評価だけが残っている状態（mercari_is_receipt_rated()と同じ考え方）。
                    # rakuma_get_raw_status()はこの文言を認識できず例外を送出していた
                    # （実例: 0b15ecb49ef103de0923a54623b7bbc7）。フリマ取引中の対象からは
                    # 外すが、eBayステータス・到着日・メッセージ履歴は変更しない。
                    if rakuma_is_buyer_response_complete(driver):
                        inactivated = mark_flema_inactive(access_conn, RAKUMA_VENDOR_NAME, order_id)
                        print(url)
                        print(f"出品者からの評価をお待ちください（購入者側の対応完了）を検出。"
                              f"日常のフリマ取引中をFalseにしました（{inactivated}行）。"
                              "eBayステータス・到着日・メッセージ履歴は変更していません。")
                        print()
                        last_error = None
                        break

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

                    last_error = None
                    break

                except Exception as e:
                    last_error = e
                    if attempt < ITEM_COLLECTION_MAX_ATTEMPTS:
                        print(f"WARN: {url} の処理に失敗しました（{attempt}/{ITEM_COLLECTION_MAX_ATTEMPTS}回目）。"
                              f"リトライします: {e}")
                        time.sleep(ITEM_COLLECTION_RETRY_WAIT_SEC)

            if last_error is not None:
                print(url)
                print(f"ERROR: {last_error}")
                print()
                failed_ids.append(order_id)

        sync_carrier_tracking_to_daily(access_conn)

        # 【2026-09-14追加】リトライしても失敗した取引IDを呼び出し元へ返す。
        return failed_ids

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
#   【2026-09-02変更】mouse・HRSP-server等、複数PCから本ファイルが実行される実態が
#   あるにもかかわらず、旧実装はPIDロックファイル(furima_purchase_runner.lock)を
#   各PCのローカルディスク(D:\apps_nostock)に置いており、他PCの実行を一切検知できない
#   欠陥があった（psutil.pid_exists()は自PCのプロセス表しか見られない）。
#   一方、実際に書き込む先（Access日常＝Y:\ヤフオクDB.accdb、trx.*＝SQL Server）は
#   PC間で共有されているため、二重起動時に競合し得る状態だった。
#   このため、ロックファイルは廃止し、各PCから共通で参照できるY:\（Access日常と
#   同じ共有領域）上の状態ファイル1つだけで二重起動防止を行う方式に変更した。
#   PIDロックのような「プロセスの生死」チェックはできなくなるため、
#   state=running のまま1時間以上経過した場合は自動解除せず、異常終了の疑いとして
#   人間の確認を促すメッセージを表示するに留める（古いrunningの自動解除はしない）。
#
# 状態ファイル(Y:\furima_purchase_runner_status.txt)に running/requested/done/error、
# 実行元(hostname)・PID（排他制御には使わない。調査用）、開始/終了時刻、
# サイトごとの結果(success/error)を書き込む。Access側のフォームタイマーが
# これをポーリングして表示更新・Requeryに使う。VBA側にJSONパーサーを新設
# せずに読めるよう、あえてJSONではなく1行1個の"key=value"形式にしている。
#
# 【2026-09-13変更】収集実行ホストの一本化（Pythonはmouseだけで動かす方式）:
#   Selenium/Chromeによる実収集は、各フリマサイトへログイン済みのChromeプロファイルを
#   持つmouseでしか正しく動作しない。従来はAccess「到着日入力」フォームの
#   「フリマ情報取得」ボタンがVBA側のShell()呼び出しで
#   `python.exe D:\apps_nostock\apps\etc\furima_purchase.py` を起動しており、
#   これはmouse以外のPC（HP-PC・DELL-PC等）からもそのまま実行できてしまっていた。
#
#   【方針転換】mouse以外のPCにはPython自体を配置・実行しない。ボタンのVBA側で
#   ローカルのpython.exeをShell()起動するのをやめ、共有状態ファイル
#   （Y:\furima_purchase_runner_status.txt。以前からある二重起動防止・進捗表示用の
#   ファイルをそのまま使う）へ直接 state=requested を書き込むだけにする
#   （VBAのファイルI/Oのみで完結。Pythonの呼び出しは一切発生しない）。
#
#   mouse側は、Windowsタスクスケジューラの新規タスク(FurimaPurchaseCollectorWatcher)
#   により `python.exe D:\apps_nostock\apps\etc\furima_purchase.py --poll-only` を
#   1分間隔（mouseにログオン中のみ実行）で起動する。--poll-only起動時は
#   state=requestedの場合のみ実収集(_run_collection())へ進み、それ以外
#   （requestedでない）は何もせず即座に終了する（Chromeを毎分起動することはない）。
#   Pythonはmouse以外では一切実行されない前提のため、ホスト名による分岐は
#   もう不要（過去に導入したCOLLECTOR_HOSTNAMES方式は廃止した）。
#
#   【テストモード】status.get("test_site")・status.get("test_item_id")が指定されて
#   いる場合、_run_collection()はSITESのうち指定サイトだけを、指定の注文ID1件だけに
#   絞り込んで実行する（他の2サイトはスキップする。本番の全件収集ロジック自体は
#   変更していない）。VBA側は「フリマ情報取得」ボタンとは別の操作（例:
#   Shift+クリックやテスト専用ボタン）でこの2キーを追加で書き込む想定。
#
#   排他制御はこれまで通り状態ファイル1つによるベストエフォート方式のまま
#   （読み取り→書き込みの間の競合を完全には防げない。人が手動で押す頻度を
#   前提にした従来からの割り切りを維持し、新たな排他機構は導入しない）。
# ============================================================================
# ============================================================================
STATUS_FILE = Path(r"Y:\furima_purchase_runner_status.txt")

# state=running のまま、これ以上経過していたら異常終了の疑いとして扱う（自動解除はしない）。
RUNNING_STALE_WARNING_SEC = 3600

# (状態ファイル上の表示名, 呼び出す各サイトのmain())
SITES = [
    ("Mercari", mercari_main),
    ("YahooFurima", paypay_main),
    ("Rakuma", rakuma_main),
]

# 【2026-09-14追加】収集プロセスの異常終了検知（実機不具合PID 22052を受けて追加）。
# state=runningのまま記録されたPIDが既に終了している場合、正常終了時の
# finally節（状態ファイル更新）まで到達できなかったことを意味する
# （実機不具合: DELL-PCから依頼した1件テストの収集プロセスが実行途中で応答なく
# 終了し、状態ファイルがrunningのまま残り続け、Access側が「依頼中」の表示・
# ボタン無効のまま戻らなくなった。Windowsのタスクスケジューラ履歴・イベントログ
# （Application/System/Windows Defender）・実行ログのいずれにも、プロセスが
# 異常終了したことを示す記録が無く、終了原因そのものは特定できていない）。
# mouse上のタスクスケジューラ(--poll-only、1分間隔)が毎回この生死を確認し、
# 既に終了していればstate=errorへ確定する。まだpendingのまま（着手前・未完了）の
# サイトは、送信結果が不明な取引を誤って再送信しないよう"error"として記録するのみで、
# 自動での再収集・再送信は一切行わない（次にボタンを押した時に改めて依頼できる）。
def _is_pid_alive(pid: int) -> bool:
    """
    Windows専用: 指定PIDのプロセスが現在も存在するかどうかをtasklistコマンドで
    確認する（新規の外部ライブラリ依存を増やさないため、既にimport済みのsubprocess
    のみを使う）。tasklist自体の実行に失敗した場合は判定不能とみなし、Trueを返す
    （安全側＝本当に動いているものを誤ってerror確定させないため）。
    """
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10
        )
    except Exception:
        return True
    # プロセスが存在する場合はCSV1行（"python.exe","22052",...）が返る。
    # 存在しない場合は「情報: 条件に一致するタスクは実行されていません。」等の
    # メッセージ行（ダブルクオートで始まらない）になる。
    return result.stdout.strip().startswith('"')


def _reap_if_dead(status: dict) -> None:
    """state=runningのPIDが既に終了していれば、state=errorへ確定する。生きている、
    またはPIDが判定できない場合は何もしない（呼び出し元は--poll-only起動時、
    state=runningの場合のみ呼ぶ）。"""
    try:
        pid = int(status.get("PID", ""))
    except (TypeError, ValueError):
        return
    if pid <= 0 or _is_pid_alive(pid):
        return

    started_at = status.get("started_at", "")
    # 状態ファイルの既存内容（requested_by・test_site・test_item_id・各サイトの
    # 既知の結果等）を引き継ぐ。_write_status()が別途書き込むstate/hostname/PID/
    # started_at/finished_atは重複させないよう取り除く。
    results = dict(status)
    for key in ("state", "hostname", "PID", "started_at", "finished_at"):
        results.pop(key, None)

    for name, _ in SITES:
        if results.get(name) == "pending":
            results[name] = "error"
            results[f"{name}_error"] = (
                "収集プロセスが応答なく終了したため、この取引の処理結果は確認できていません。"
                "自動での再収集・再送信は行っていません。"
            )

    results["watcher_note"] = (
        f"収集プロセス(PID {pid})が実行中に応答なく終了したため、"
        "監視タスクが状態をerrorへ確定しました。"
    )

    _write_status("error", results, started_at, finished_at=datetime.now().isoformat())
    print(f"[watcher] PID {pid} が終了していたため、state=errorへ確定しました。")


# 状態ファイル上の表示名 → trx.vendor_message/日常.店舗のDB表記。
# 【2026-09-14追加】自動送信フェーズが、各main()の戻り値(failed_ids)をDB表記の
# 店舗名で突き合わせるために使う。
SITE_DISPLAY_TO_VENDOR_NAME = {
    "Mercari": MERCARI_VENDOR_NAME,
    "YahooFurima": PAYPAY_VENDOR_NAME,
    "Rakuma": RAKUMA_VENDOR_NAME,
}


def _write_status(state: str, results: dict, started_at: str, finished_at: str = "") -> None:
    lines = [
        f"state={state}",
        f"hostname={socket.gethostname()}",
        f"PID={os.getpid()}",
        f"started_at={started_at}",
        f"finished_at={finished_at}",
    ]
    lines += [f"{name}={value}" for name, value in results.items()]
    STATUS_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_status() -> dict:
    """Y:\\の共通状態ファイルをkey=value形式で読み込む。存在しない場合は空辞書
    （初回実行等、まだ一度も書き込まれていない状態として扱う＝実行可能）。"""
    if not STATUS_FILE.exists():
        return {}
    status = {}
    for line in STATUS_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            status[key.strip()] = value.strip()
    return status


# 【2026-09-14追加】--poll-only起動の最初の状態確認がハングして次の起動をブロックする
# 不具合を受けて追加（実機不具合PID 24260: 17:59:16に起動した--poll-onlyが、CPU使用率
# ほぼゼロ・ネットワーク接続なしのまま約10分間ブロックされ続けた。Y:\は共有ネットワーク
# ドライブのため、_read_status()のファイルI/O自体が稀に長時間ブロックされることがある
# と考えられる。タスクスケジューラの-MultipleInstances IgnoreNew設定により、この
# ハングしたプロセスが終了しない限り後続の毎分の起動がすべて拒否され続け
# （LastTaskResult=0x800710E0「オペレーターまたは管理者が要求を拒否しました」）、
# DELL-PCからの依頼(state=requested)の検知が数分遅延した）。
# --poll-only起動の最初の状態確認だけをデーモンスレッドで実行し、
# POLL_STATUS_READ_TIMEOUT_SEC以内に完了しなければ今回のポーリングを諦めて
# 即座に終了する（次の1分後の起動に委ねる）。デーモンスレッドはメインスレッドが
# 終了すればOSに強制的に破棄されるため、ファイルI/Oが実際にどれだけ長くブロック
# されていても、プロセス自体は速やかに終了できる。実収集フェーズ（Chrome操作・
# DB書き込み等、数分〜十数分かかりうる）にはこのタイムアウトを適用しない
# （_run_collection()自体は対象外）。
POLL_STATUS_READ_TIMEOUT_SEC = 15.0


def _read_status_with_timeout(timeout_sec: float = POLL_STATUS_READ_TIMEOUT_SEC):
    """_read_status()をデーモンスレッドで実行し、timeout_sec以内に完了しなければ
    Noneを返す（呼び出し元は今回のポーリングを諦めて即座に終了する）。"""
    result = {}
    errors = []

    def _worker():
        try:
            result["status"] = _read_status()
        except Exception as e:
            errors.append(e)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)

    if t.is_alive():
        print(f"[watcher] 状態ファイルの読み込みが{timeout_sec:.0f}秒以内に完了しませんでした。"
              "今回のポーリングは中断し、次回の起動に委ねます。", flush=True)
        return None
    if errors:
        raise errors[0]
    return result.get("status", {})


def _running_guard_message(status: dict) -> str:
    """
    state=running（または2026-09-13追加のstate=requested、mouseへ実行依頼済みで
    まだ拾われていない状態）中に新規起動をブロックする際の表示文言を組み立てる。
    running側はstarted_atから現在までの経過時間を表示し、RUNNING_STALE_WARNING_SEC
    (1時間)以上経過している場合は異常終了の疑いとして人間の確認を促す
    （このファイル自身は古いrunningを自動解除しない）。
    """
    if status.get("state") == "requested":
        requested_by = status.get("requested_by", "?")
        return f"フリマ情報取得は既にmouseへ依頼済みです（依頼元: {requested_by}）。\nmouse側の実行をお待ちください。"

    try:
        started_at = datetime.fromisoformat(status.get("started_at", ""))
        elapsed_sec = (datetime.now() - started_at).total_seconds()
    except (ValueError, TypeError):
        elapsed_sec = None

    if elapsed_sec is not None and elapsed_sec >= RUNNING_STALE_WARNING_SEC:
        return (
            "フリマ情報取得は実行中のまま1時間以上経過しています。\n"
            "異常終了している可能性があります。谷川まで連絡してください。"
        )
    if elapsed_sec is not None:
        return f"フリマ情報取得は既に実行中です。\n開始から{int(elapsed_sec // 60)}分経過しています。"
    return "フリマ情報取得は既に実行中です。"


EXECUTION_LOG_FILE = Path(r"D:\apps_nostock\logs\furima_purchase_log.txt")


class _StreamToLogger:
    """print()等でのsys.stdout/stderr書き込みをloggingへ橋渡しする
    （waitress_server_5097.py の _LoggerWriter と同じパターン）。"""

    def __init__(self, logger: logging.Logger, level: int) -> None:
        self._logger = logger
        self._level = level

    def write(self, message: str) -> None:
        message = message.rstrip()
        if message:
            self._logger.log(self._level, message)

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return False


def _setup_execution_logging() -> None:
    """
    【2026-09-10 実機不具合を受けて追加】Access VBAのShell()から起動される本スクリプト
    （python.exe D:\\apps_nostock\\apps\\etc\\furima_purchase.py）の標準出力（処理件数・
    ERROR行等）は、従来どこにも保存されておらず、実行時に何が起きたかを事後に確認する
    手段が無かった（実例: m85217087095がrunner_status上"success"のまま欠落した際、
    原因究明に使えるログが一切残っていなかった）。webapp側のwaitress_server_5097.pyと
    同じRotatingFileHandlerパターンで標準出力・標準エラーをファイルへ保存する。
    このスクリプトを直接実行した場合（__main__）にのみ有効化する
    （messages_blueprint.py経由でWebアプリへimportされた場合は、Webアプリ側の
    ロギング設定をそのまま使い、二重にリダイレクトしない）。
    """
    EXECUTION_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = RotatingFileHandler(
        str(EXECUTION_LOG_FILE), maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    sys.stdout = _StreamToLogger(root_logger, logging.INFO)
    sys.stderr = _StreamToLogger(root_logger, logging.ERROR)


# ------------------------------------------------------------
# 自動送信（2026-09-14追加、第一段階: mouse上での実行のみ）
# ------------------------------------------------------------
# 本番の全件収集後、各取引の最新状態(is_shipped)・会話履歴からdetermine_suggested_reply()が
# 提案する定型文のうち、この3種類だけを自動送信する。
#   - shipped_2: 発送済みの肯定的証拠（Access日常.eBayステータス）で確定できる定型文
#   - first_reply_onegai / first_reply_plain: 出品者の初回メッセージへの初回挨拶
# AI返信不要候補(ai_no_reply_candidate)・定型文なし(template_key=None)・既に送信済み
# （determine_suggested_reply()が上記いずれのステップにも該当せずtemplate_key=Noneを
# 返すことで自然に除外される）は、determine_suggested_reply()自体の判定にすべて任せる
# （ここで別途キーワード等の判定は行わない）。
AUTO_SEND_TEMPLATE_KEYS = ("shipped_2", "first_reply_onegai", "first_reply_plain")


def _auto_send_one_reply(sql_conn, vendor_name: str, vendor_item_id: str, history: list, suggested_reply: dict) -> dict:
    """
    1取引分の自動送信を行う。既存のsend_mercari_reply/send_paypay_reply/send_rakuma_reply
    （実サイトへの送信＋実際のレスポンスによる送信確認、/messages画面の「送信」ボタンと
    全く同じ処理）をそのまま使う。DB保存も、messages_blueprint.pyの手動送信時と同じ
    ロジック（メルカリはMERCARI_SQL_UPSERT_VENDOR_MESSAGE_BY_ID、PayPayフリマ・ラクマは
    save_vendor_messages()）でtrx.vendor_messageへ反映する。

    戻り値: {"outcome": "sent"|"skipped_new_message"|"needs_review"|"error",
             "detail": str, "sent_text": str|None}
      - sent: 実際に送信し、DBへも反映した
      - skipped_new_message: 送信前に新着メッセージを検出したため送信しなかった
        （安全機構。取得済みの新着はDBへ反映済み。自動再送はしない）
      - needs_review: 送信結果を実サイトのレスポンスで確認できなかった（「要確認」。
        自動再送はしない。人が実サイトを確認する必要がある）
      - error: 送信処理自体が例外で失敗した
    """
    expected_count = len(history)
    reply_text = suggested_reply["text"]

    try:
        if vendor_name == MERCARI_VENDOR_NAME:
            expected_last_message_id = history[-1]["message_id"] if history else None
            result = send_mercari_reply(vendor_item_id, expected_count, reply_text,
                                         expected_last_message_id=expected_last_message_id)
        elif vendor_name == PAYPAY_VENDOR_NAME:
            result = send_paypay_reply(vendor_item_id, expected_count, reply_text)
        elif vendor_name == RAKUMA_VENDOR_NAME:
            result = send_rakuma_reply(vendor_item_id, expected_count, reply_text)
        else:
            return {"outcome": "error", "detail": f"未対応の店舗です: {vendor_name}", "sent_text": None}
    except Exception as e:
        return {"outcome": "error", "detail": f"送信処理中に例外が発生しました: {e}", "sent_text": None}

    if result.get("reason") == "new_message_detected":
        new_messages = result.get("new_messages") or []
        try:
            if vendor_name == MERCARI_VENDOR_NAME:
                with sql_conn.cursor() as cur:
                    for msg in new_messages:
                        cur.execute(
                            MERCARI_SQL_UPSERT_VENDOR_MESSAGE_BY_ID,
                            (
                                vendor_name, vendor_item_id, msg["message_id"], msg["message_no"],
                                msg["sender_name"], "出品者" if msg["is_from_seller"] else "購入者",
                                msg["message_datetime"], msg["message_body"],
                            )
                        )
                sql_conn.commit()
            else:
                save_vendor_messages(sql_conn, vendor_name, vendor_item_id, new_messages)
        except Exception as e:
            print(f"[auto_send] 新着メッセージのDB保存に失敗しました {vendor_item_id}: {e}")
        return {"outcome": "skipped_new_message",
                "detail": "送信前に新しいメッセージを検出したため送信しませんでした（自動再送はしません）",
                "sent_text": None}

    if not result.get("ok"):
        # 【要確認】結果が確認できない（クリックは行ったが実レスポンスで成功を確認できない等）。
        # 自動再送はしない。人が実サイトを確認する必要がある。
        return {"outcome": "needs_review", "detail": result.get("error") or "送信結果を確認できませんでした",
                "sent_text": None}

    # 実送信成功。DBへ反映する（messages_blueprint.pyの手動送信成功時と同じロジック）。
    try:
        if vendor_name == MERCARI_VENDOR_NAME:
            with sql_conn.cursor() as cur:
                cur.execute(
                    MERCARI_SQL_UPSERT_VENDOR_MESSAGE_BY_ID,
                    (
                        vendor_name, vendor_item_id, result["message_id"], result["message_no"],
                        "自分", "購入者", result["message_datetime"], reply_text,
                    )
                )
            sql_conn.commit()
        else:
            new_messages = result.get("new_messages")
            if new_messages:
                save_vendor_messages(sql_conn, vendor_name, vendor_item_id, new_messages)
            else:
                # 【2026-09-14追加、実機不具合z680665644/z680665936を受けて対応】
                # 実サイトへの送信自体は成功済み（result["ok"]=True）だが、送信後の
                # 最新メッセージ再取得に失敗し(post_send_refetch_failed等)、
                # new_messagesが空のまま何も保存されない状態だった。このまま放置すると
                # trx.vendor_messageに送信済みの事実が一切残らず、次回の自動送信サイクルで
                # 同じ取引に再度同じ定型文が提案され、二重送信されるおそれがある。
                # 実際に送信したことが分かっている本文だけを、暫定的に1行(message_no=
                # expected_count+1)として自分で保存しておく（次回の通常巡回スクレイプが
                # 実際の内容へ上書き・整合させる。save_vendor_messages()は
                # (vendor_name, vendor_item_id, message_no)キーのMERGEのため上書き前提でも安全）。
                fallback_message_no = expected_count + 1
                save_vendor_messages(sql_conn, vendor_name, vendor_item_id, [{
                    "message_no": fallback_message_no,
                    "sender_name": "自分",
                    "sender_type": "購入者",
                    "message_datetime_text": "たった今",
                    "message_body": reply_text,
                }])
                print(f"[auto_send] {vendor_item_id}: 送信後の最新メッセージ再取得に失敗したため、"
                      f"送信文面を暫定的に1行だけ保存しました（次回の巡回で実際の内容に整合されます）。"
                      f"詳細: {result.get('error')}")
    except Exception as e:
        # 実送信自体は成功済みのため"sent"のまま。DB保存の失敗だけログで検知する
        # （次回の通常巡回スクレイプで反映される）。
        print(f"[auto_send] 送信成功後のDB保存に失敗しました {vendor_item_id}: {e}")

    detail = "送信しました"
    if result.get("error"):
        detail += f"（{result['error']}）"
    return {"outcome": "sent", "detail": detail, "sent_text": reply_text}


def _auto_send_replies(sql_conn, access_conn, exclude_ids_by_vendor: dict = None) -> None:
    """
    本番の全件収集後に呼ぶ。現在アクティブな全取引（Access日常.フリマ取引中=True、
    TARGET_VENDOR_NAMES）について、determine_suggested_reply()の判定結果が
    AUTO_SEND_TEMPLATE_KEYSのいずれかの取引だけを自動送信する。

    exclude_ids_by_vendor: {vendor_name(DB表記): {vendor_item_id, ...}}。今回の収集で
    リトライしても失敗した取引はここに含まれ、対象から除外する（「取得失敗の取引は
    送信せず、成功した取引は続行する」ため）。
    """
    exclude_ids_by_vendor = exclude_ids_by_vendor or {}

    active_orders = fetch_active_orders(access_conn)
    target_ids = [oid for oid, info in active_orders.items() if info["vendor_name"] in TARGET_VENDOR_NAMES]
    if not target_ids:
        print("自動送信対象なし（アクティブな取引がありません）。")
        return

    history_by_key = _fetch_histories_for_orders(sql_conn, target_ids)
    for oid in target_ids:
        key = (active_orders[oid]["vendor_name"], oid)
        history_by_key.setdefault(key, [])

    sent_count = 0
    skipped_count = 0
    review_count = 0
    error_count = 0

    for (vendor_name, vendor_item_id), history in sorted(history_by_key.items()):
        order_info = active_orders.get(vendor_item_id)
        if order_info is None:
            continue

        if vendor_item_id in exclude_ids_by_vendor.get(vendor_name, ()):
            print(f"{vendor_name} {vendor_item_id}: 今回の収集に失敗したため自動送信の対象から除外しました。")
            continue

        is_shipped = is_shipped_status(order_info["ebay_status"])
        suggested_reply = determine_suggested_reply(history, is_shipped)
        template_key = suggested_reply["template_key"]

        if template_key not in AUTO_SEND_TEMPLATE_KEYS:
            continue

        print(f"\n{vendor_name} {vendor_item_id}: template_key={template_key}")
        print(f"  送信文面: {suggested_reply['text']!r}")

        result = _auto_send_one_reply(sql_conn, vendor_name, vendor_item_id, history, suggested_reply)

        print(f"  結果: {result['outcome']} - {result['detail']}")

        if result["outcome"] == "sent":
            sent_count += 1
        elif result["outcome"] == "skipped_new_message":
            skipped_count += 1
        elif result["outcome"] == "needs_review":
            review_count += 1
        else:
            error_count += 1

    print(f"\n自動送信結果: 送信{sent_count}件, 新着検出でスキップ{skipped_count}件, "
          f"要確認{review_count}件, エラー{error_count}件")


def _run_collection(requested_by: str = "", test_site: str = "", test_item_id: str = "") -> None:
    """
    実収集本体（従来のmain()のループ部分）。mouse上でのみ呼び出される
    （Pythonはmouse以外では実行しない方針のため、ホスト名チェックはしない）。

    requested_by: VBA側が状態ファイルへ書き込んだ依頼元hostname（Access直押しの
        通常運用では常にこれが入っている想定。空文字は開発時の直接手動実行のみ）。

    test_site・test_item_id: 【2026-09-13追加】テストモード。ともに指定された場合、
        SITESのうちtest_siteと一致するサイトだけを、test_item_id 1件だけに絞り込んで
        実行し、他のサイトは完全にスキップする（本番の全件収集ロジック自体
        ＝各サイトのmain()の中身は一切変更しない。絞り込みは各main()が既に
        受け付けるwanted_ids引数に1件だけ渡すことで実現する）。
        いずれか一方だけが指定された場合は無視し、通常の全件収集を行う
        （中途半端な絞り込みで意図しない全件スキップ等を避けるため）。

    【2026-09-13修正】失敗時、results[name]は従来通り"success"/"error"のいずれか
    （既存のVBA側が文字列完全一致で判定している可能性があるため、この値自体は
    変更しない）。原因が分かるエラーメッセージは別キー{name}_errorへ追加で書き込む
    （改行はAccess側の1行1key=value形式を壊さないよう空白へ置換し、長さも制限する）。
    """
    started_at = datetime.now().isoformat()

    if test_site and test_item_id:
        sites_to_run = [(name, func) for name, func in SITES if name == test_site]
    else:
        sites_to_run = SITES
        test_site = ""
        test_item_id = ""

    results = {name: "pending" for name, _ in sites_to_run}
    if requested_by:
        results["requested_by"] = requested_by
    if test_site:
        results["test_site"] = test_site
        results["test_item_id"] = test_item_id
    _write_status("running", results, started_at)

    # 【2026-09-14追加】各main()がリトライしても失敗した取引IDを店舗(DB表記)単位で集約する。
    # 自動送信フェーズが、今回の収集に失敗した取引を対象から除外するために使う。
    failed_ids_by_vendor = {}

    for name, run_func in sites_to_run:
        print(f"\n{'=' * 20} {name} {'=' * 20}", flush=True)
        try:
            if test_site == name:
                run_func(wanted_ids={test_item_id})
            else:
                failed_ids = run_func()
                vendor_name = SITE_DISPLAY_TO_VENDOR_NAME.get(name)
                if vendor_name:
                    failed_ids_by_vendor[vendor_name] = set(failed_ids or [])
            results[name] = "success"
        except Exception as e:
            print(f"[ERROR] {name} の実行中にエラーが発生しました: {e}", flush=True)
            results[name] = "error"
            error_text = str(e).replace("\r", " ").replace("\n", " ").strip()
            results[f"{name}_error"] = error_text[:300]
        # 1サイト終わるたびに書き込み、Access側が進捗を見られるようにする。
        _write_status("running", results, started_at)

    # resultsにはrequested_by・test_site・{name}_error等の補助キーも混在するため、
    # overall_stateの判定は今回実行したサイト名の値だけを見る。
    site_names = {name for name, _ in sites_to_run}
    overall_state = "done" if all(results[name] == "success" for name in site_names) else "error"

    if test_site:
        # 1件テストモード（test_site指定時）では自動送信を一切行わないため、
        # 収集完了時点でdone/errorを確定してよい。
        _write_status(overall_state, results, started_at, finished_at=datetime.now().isoformat())
        return

    # 【2026-09-14修正】以前はここでstate=done/errorを書き込んでから自動送信していたため、
    # 実際にはまだ自動送信（実サイトへの送信操作）が進行中なのに、Access側が「完了」と
    # 表示し、ボタンも再度押せる状態になってしまっていた（実機で確認）。自動送信フェーズが
    # 終わるまでstate=running のまま維持し（二重起動防止ガードも継続して有効にする）、
    # 全て終わってから最終的なdone/errorを書き込む。
    print(f"\n{'=' * 20} 自動送信 {'=' * 20}", flush=True)
    try:
        auto_send_sql_conn = get_sql_server_connection()
        auto_send_access_conn = get_access_connection()
        try:
            _auto_send_replies(auto_send_sql_conn, auto_send_access_conn,
                                exclude_ids_by_vendor=failed_ids_by_vendor)
        finally:
            auto_send_access_conn.close()
            auto_send_sql_conn.close()
    except Exception as e:
        print(f"[ERROR] 自動送信処理全体でエラーが発生しました: {e}", flush=True)
        overall_state = "error"
        results["auto_send_error"] = str(e).replace("\r", " ").replace("\n", " ").strip()[:300]

    _write_status(overall_state, results, started_at, finished_at=datetime.now().isoformat())


def main(poll_only: bool = False) -> None:
    """
    2026-09-13の方針転換後のエントリポイント。Pythonはmouse上でのみ実行される
    （Access「フリマ情報取得」ボタンはVBA側のファイルI/Oのみでstate=requestedを
    書き込むよう変更済みで、mouse以外のPCではpython.exe自体が起動されない）。

      - poll_only=True（mouse上のタスクスケジューラ`FurimaPurchaseCollectorWatcher`
        による1分間隔の定期起動）: 最初の状態確認は_read_status_with_timeout()で行う
        （2026-09-14追加。POLL_STATUS_READ_TIMEOUT_SEC以内に読めなければ今回は諦めて
        即座に終了し、次回に委ねる。実機不具合PID 24260の再発防止）。
        state=requestedの場合のみ実収集へ進む。state=runningの場合は、記録された
        PIDが既に終了していないか確認し（_reap_if_dead()、2026-09-14追加）、
        終了していればstate=errorへ確定する。それ以外（requested・runningの
        いずれでもない）は何もせず即座に終了する（毎分Chromeを起動することはない）。
        状態ファイルのtest_site/test_item_idも引き継いで_run_collection()へ渡す。
      - poll_only=False（mouseでの開発・保守用の直接手動実行）: running中でなければ
        従来通り即座に実収集する（依頼状態を経由しない）。
    """
    if poll_only:
        status = _read_status_with_timeout()
        if status is None:
            return  # 状態確認自体がタイムアウトした。今回は諦め、次回の起動に委ねる。
        state = status.get("state")

        if state == "running":
            _reap_if_dead(status)
            return
        if state != "requested":
            return  # 依頼が無ければChromeは起動しない。
        _run_collection(
            requested_by=status.get("requested_by", ""),
            test_site=status.get("test_site", ""),
            test_item_id=status.get("test_item_id", ""),
        )
        return

    status = _read_status()
    state = status.get("state")
    if state == "running":
        print(_running_guard_message(status))
        return
    _run_collection()


if __name__ == "__main__":
    _setup_execution_logging()
    # mercari_main()内部のargparse（--item-ids）が未知の引数として--poll-onlyを
    # 拒否してしまう（parser.parse_args()がSystemExit(2)を送出し、_run_collection()の
    # except Exceptionでは捕捉できずプロセスが状態ファイル更新前に落ちる）ため、
    # ここで--poll-onlyを読み取った後、後続のどの引数解析にも渡らないようsys.argvから
    # 取り除いておく。
    _poll_only = "--poll-only" in sys.argv
    if _poll_only:
        sys.argv.remove("--poll-only")
    main(poll_only=_poll_only)
