# apps/inventory/fetch_orders_ebay.py
#
# 【このファイルについて】
# eBay注文取得の本番スクリプト。新規受注取得・DB登録・メール/LINE通知・後続処理に加え、
# 受注後のキャンセル・出荷・配達・返品・未着・INAD・ケース・返金・支払い異議申し立て
# の状態を継続取得する。
# （2026-09-26: fetch_orders_ebay_new.pyから本ファイル名へ改名。旧fetch_orders_ebay.pyはGit履歴のみ）
#
# 運用:
#   - realtime : 新規受注取得 + 出荷前キャンセル確認。タスクスケジューラEbayOrderFetchで常駐
#   - daily    : 既存注文の状態・追跡番号(SpeedPAKはOCラストマイル番号)確認。定期実行
#   - backfill : 過去分の補完。必要時のみ手動実行（通知は常に送らない）
#   - daily/backfillは--live未指定時はdry-run（trx.ebay_ordersへのUPDATEを行わない）

import argparse
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

# --------------------------------------------------
# 動作スイッチ
# --------------------------------------------------
# True の間は、ステータス変化のメール・LINEを実送信せずログにのみ出力する。
# また realtime モードの新規受注パイプライン（メール/LINE/サンキュー/Access登録/
# trx.ebay_orders INSERT）も実行せず、ログにのみ「実行予定だった内容」を記録する。
# 2026-09-09: 本番切替のためFalseに変更。
TEST_MODE = False

# 2026-09-12: 新規受注通知は本番機能のため常時有効。status変化通知（キャンセル等）とは
# 独立して制御する（2026-09-12、ユーザー指示により分離）。
NEW_ORDER_NOTIFICATION_ENABLED = True

# status変化通知（キャンセル等）専用のマスタースイッチ。開発中はFalseを維持する。
# Falseの間は、status変化のメール・LINEを一切送信せず、
# 「通知抑止」としてログにのみ記録する（ユーザーが明示的に指示するまでFalseを維持）。
# eBay購入者へのサンキューメッセージ（メール・LINEではない）はどちらのスイッチの対象外で、
# 現行仕様のまま実行する。
NOTIFICATION_ENABLED = False

# ==== VS Code ▶ 実行対応 ====
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ==== 既存資産 ====
from apps.adapters.ebay_api import get_access_token_new, send_buyer_thankyou_message
from apps.common.utils import USD_JPY_RATE, get_sql_server_connection

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
_STATE_FILE = _LOG_DIR / "fetch_orders_ebay.state.json"
_BACKFILL_REPORT_CSV = _LOG_DIR / "fetch_orders_ebay.backfill_report.csv"


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("fetch_orders_ebay")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            str(_LOG_FILE), maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except Exception:
        # ファイルログの初期化自体に失敗しても、コンソールへはフォールバックしない
        # （コンソール書き込みブロックの再発を避けるため）。ログなしで処理を継続する。
        logger.addHandler(logging.NullHandler())

    # 開発時に進捗を確認できるよう、ファイルログと同内容をコンソールにも出力する。
    # print()は使わずlogging.StreamHandler経由にすることで、file/console間で
    # フォーマットとログレベルを常に一致させる。
    # 💡 pythonw.exe(コンソールなしのバックグラウンド実行)ではsys.stdout/stderrが
    # Noneになり、StreamHandlerを追加してもログ書き込みのたび失敗するだけで
    # 意味がない(ファイルログには影響しないが無駄な処理になる)ため、
    # コンソールが実際に存在する場合のみ追加する。
    if sys.stderr is not None:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

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


def _load_state() -> dict:
    """アカウント別の最終チェック日時などを保持する簡易ステートファイル。"""
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        log.warning("stateファイル読み込みに失敗しました", exc_info=True)
    return {}


def _save_state(data: dict) -> None:
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(_STATE_FILE)
    except Exception:
        log.warning("stateファイル書き込みに失敗しました", exc_info=True)


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
        # MOUSEはTailscale/VPN越しのネットワーク共有のため、接続不能時に長時間
        # 待たされないよう明示的にタイムアウトを指定する（SQL_ATTR_LOGIN_TIMEOUT）
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb;",
            timeout=5,
        )
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
        # MOUSEはTailscale/VPN越しのネットワーク共有のため、接続不能時に長時間
        # 待たされないよう明示的にタイムアウトを指定する（SQL_ATTR_LOGIN_TIMEOUT）
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb;",
            timeout=5,
        )
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


# --------------------------------------------------
# Access「日常」への ebay_tracking_number 反映（2026-09-13追加）
# --------------------------------------------------
# 目的: eBay出荷用の追跡番号(trx.ebay_orders.tracking_number)を、Access「日常」の
# 新設カラム ebay_tracking_number へ反映する。
# 既存の「日常.tracking_number」「日常.carrier」は仕入商品（購入元からの入荷）の
# 追跡情報であり、eBay出荷（買い手への出荷）の追跡番号とは別物なので、
# 絶対に変更・上書きしない（ユーザー指示）。
#
# 識別方法: 既存のAccess書き込み処理(_insert_access_nichinichi/_access_order_exists)
# と同じ (amazon注文番号=order_id, 注文ID=vendor_item_id) の組み合わせで対応する
# 日常レコードを特定する。0件（対応なし）・2件以上（複数候補）の場合は
# 推測で更新せず、ログに記録した上でスキップする。
_access_ebay_tracking_column_cache: dict = {"value": None}


def _access_ebay_tracking_column_available() -> bool:
    """Access「日常」テーブルに ebay_tracking_number カラムが追加済みかを確認する。
    未追加の環境（ALTER TABLE実行前）でも本スクリプトが例外にならないための安全弁。
    プロセス内で1回だけ確認しキャッシュする。"""
    if _access_ebay_tracking_column_cache["value"] is not None:
        return _access_ebay_tracking_column_cache["value"]
    conn = None
    try:
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb;",
            timeout=5,
        )
        col_names = {row.column_name for row in conn.cursor().columns(table="日常")}
        available = "ebay_tracking_number" in col_names
    except Exception:
        log.warning("[ebay_tracking] 日常テーブルのカラム確認に失敗しました。未追加として扱います", exc_info=True)
        available = False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    _access_ebay_tracking_column_cache["value"] = available
    return available


def sync_ebay_tracking_to_access(
    order_id: str, vendor_item_id: str, ebay_tracking_number: str | None, dry_run: bool = False,
) -> None:
    """trx.ebay_orders.tracking_numberをAccess「日常」.ebay_tracking_numberへ反映する。
    - ebay_tracking_numberが空/Noneの場合は何もしない（既存値を消さない）。
    - (amazon注文番号, 注文ID)で日常レコードが一意に特定できる場合のみUPDATEする。
    - 0件（対応なし）・2件以上（複数候補）の場合は更新せずWARNINGログを残す。
    - 日常.tracking_number / 日常.carrier（仕入用）には一切触れない。
    - ALTER TABLE未実施の環境ではログにのみ記録し、例外にはしない。
    - dry_run=Trueの場合、実UPDATEは行わず予定内容をログに記録する。
    """
    if not ebay_tracking_number:
        return
    if not _access_ebay_tracking_column_available():
        log.info(
            f"[ebay_tracking][SKIP:カラム未追加] order_id={order_id} vendor_item_id={vendor_item_id} "
            f"value={ebay_tracking_number} （日常.ebay_tracking_number未追加のため書き込みなし）"
        )
        return

    conn = None
    try:
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb;",
            timeout=5,
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM 日常 WHERE amazon注文番号=? AND 注文ID=?",
            order_id or "", vendor_item_id or "",
        )
        count = cur.fetchone()[0]
        if count == 0:
            log.warning(
                f"[ebay_tracking] 日常に対応レコードが見つからないため更新しません: "
                f"order_id={order_id} vendor_item_id={vendor_item_id}"
            )
            return
        if count > 1:
            log.warning(
                f"[ebay_tracking] 日常に複数({count}件)の候補があるため推測せず更新しません: "
                f"order_id={order_id} vendor_item_id={vendor_item_id}"
            )
            return

        if dry_run:
            log.info(
                f"[ebay_tracking][DRY-RUN] UPDATE予定: "
                f"order_id={order_id} vendor_item_id={vendor_item_id} value={ebay_tracking_number}"
            )
            return

        cur.execute(
            "UPDATE 日常 SET ebay_tracking_number = ? "
            "WHERE amazon注文番号 = ? AND 注文ID = ? "
            "AND (ebay_tracking_number IS NULL OR ebay_tracking_number <> ?)",
            ebay_tracking_number, order_id or "", vendor_item_id or "", ebay_tracking_number,
        )
        conn.commit()
        if cur.rowcount:
            log.info(
                f"[ebay_tracking] 日常.ebay_tracking_number更新: "
                f"order_id={order_id} vendor_item_id={vendor_item_id} value={ebay_tracking_number}"
            )
    except Exception:
        log.warning(
            f"[ebay_tracking] Access更新失敗: order_id={order_id} vendor_item_id={vendor_item_id}",
            exc_info=True,
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# --------------------------------------------------
# trx.ebay_orders.status → Access「日常」.eBayステータス 同期（2026-09-27追加）
# --------------------------------------------------
# 日常.eBayステータスは出荷前は仕入・出荷準備の業務管理（【購入済】☆出荷可能 等）にも
# 使っているため、出荷後の状態を表す以下のstatusだけを反映する。
# 新規受注（出荷前）とキャンセル済み（出荷前キャンセルは日常レコード自体を削除する運用）は反映しない。
ACCESS_EBAY_STATUS_SYNC_TARGETS = {
    "出荷済み", "配送遅延", "配達済み", "返金済み", "要確認",
    "INAD申告", "未着申告", "返品申請中", "返品返送中", "キャンセル申請中",
}

# 日常.eBayステータスがこの値のレコードは上書きしない（在庫管理の別ワークフローで手動設定する値。
# 2026-09-27、ユーザー指示）。
ACCESS_EBAY_STATUS_SYNC_PROTECTED = {"◎有在庫"}

# daily/backfillの最後に日常.eBayステータス同期を行うか。
# 仕入側（furima_purchase.py等）も同じ日常.eBayステータスを独自の値・進行度ランクで読み書き
# しているが、実運用上の衝突は起きない想定で同期する（2026-09-27、ユーザー判断。
# 問題が発生した場合はその時点で仕入側の保護リスト等を検討する）。
ACCESS_EBAY_STATUS_SYNC_ENABLED = True


def sync_ebay_status_to_access(
    cn, keys: list[tuple[str, str]] | None = None, dry_run: bool = True,
) -> dict:
    """trx.ebay_orders.status を Access「日常」.eBayステータスへ反映する（daily/backfill/初回一括同期で共通）。
    keys: 対象の(order_id, ebay_id)一覧。Noneの場合はtrx.ebay_orders全件を対象とする。

    対応キーは trx.ebay_orders(order_id, ebay_id) と 日常(amazon注文番号, eBayID) の完全一致
    （realtimeの日常登録処理がこの2列にorder_id/ebay_idを書き込んでいる）。1注文に複数明細が
    あり得るためorder_idだけでは紐付けない。前後空白の補正などの推測による突き合わせは行わない。
    日常側で同一キーが複数行ある場合は1対1で対応できないため更新しない。
    片方にしか存在しないレコードは何もしない（新規作成・削除・補完はしない）。
    日常.eBayステータスがACCESS_EBAY_STATUS_SYNC_PROTECTED（◎有在庫）のレコードは上書きしない。
    戻り値: 集計dict（matched/changed/changed_by_status/not_in_access/duplicate_in_access/
    protected_in_access/not_target_status/unchanged/errors）。"""
    cur = cn.cursor()
    cur.execute("SELECT order_id, ebay_id, status FROM trx.ebay_orders")
    db_rows = cur.fetchall()
    if keys is not None:
        key_set = set(keys)
        db_rows = [r for r in db_rows if (r[0], r[1]) in key_set]

    stats = {
        "target": len(db_rows), "matched": 0, "changed": 0, "unchanged": 0,
        "changed_by_status": {}, "not_target_status": 0, "not_in_access": 0,
        "duplicate_in_access": 0, "protected_in_access": 0, "errors": 0,
    }
    conn = None
    try:
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=\\MOUSE\My Documents\日常せどり\ヤフオクDB.accdb;",
            timeout=5,
        )
        acur = conn.cursor()
        acur.execute("SELECT amazon注文番号, eBayID, eBayステータス FROM 日常 WHERE amazon注文番号 IS NOT NULL")
        access_by_key: dict[tuple[str, str], list] = {}
        for order_no, ebay_item_id, ebay_status in acur.fetchall():
            access_by_key.setdefault((order_no, ebay_item_id), []).append(ebay_status)

        for order_id, ebay_id, status in db_rows:
            if status not in ACCESS_EBAY_STATUS_SYNC_TARGETS:
                stats["not_target_status"] += 1
                continue
            access_statuses = access_by_key.get((order_id, ebay_id))
            if not access_statuses:
                stats["not_in_access"] += 1
                continue
            if len(access_statuses) > 1:
                stats["duplicate_in_access"] += 1
                log.warning(
                    f"[ebay_status] 日常に同一キーが{len(access_statuses)}行あるため更新しません: "
                    f"order_id={order_id} ebay_id={ebay_id}"
                )
                continue
            if access_statuses[0] in ACCESS_EBAY_STATUS_SYNC_PROTECTED:
                stats["protected_in_access"] += 1
                continue
            stats["matched"] += 1
            if access_statuses[0] == status:
                stats["unchanged"] += 1
                continue
            stats["changed"] += 1
            stats["changed_by_status"][status] = stats["changed_by_status"].get(status, 0) + 1
            if dry_run:
                log.info(
                    f"[ebay_status][DRY-RUN] 日常.eBayステータス更新予定: order_id={order_id} ebay_id={ebay_id} "
                    f"{access_statuses[0]} -> {status}"
                )
                continue
            try:
                acur.execute(
                    "UPDATE 日常 SET eBayステータス = ? WHERE amazon注文番号 = ? AND eBayID = ?",
                    status, order_id, ebay_id,
                )
                if acur.rowcount != 1:
                    conn.rollback()
                    stats["errors"] += 1
                    log.error(
                        f"[ebay_status] 更新行数が1行ではないため取り消しました(rowcount={acur.rowcount}): "
                        f"order_id={order_id} ebay_id={ebay_id}"
                    )
                    continue
                conn.commit()
                log.info(
                    f"[ebay_status] 日常.eBayステータス更新: order_id={order_id} ebay_id={ebay_id} "
                    f"{access_statuses[0]} -> {status}"
                )
            except Exception:
                stats["errors"] += 1
                log.error(f"[ebay_status] Access更新失敗: order_id={order_id} ebay_id={ebay_id}", exc_info=True)
    except Exception:
        stats["errors"] += 1
        log.error("[ebay_status] Access接続・読込に失敗しました", exc_info=True)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    log.info(f"[ebay_status][サマリ] dry_run={dry_run} {stats}")
    return stats


def _order_ship_to(order: dict) -> tuple[str | None, int]:
    """注文レスポンスのfulfillmentStartInstructionsから (配送先国コード, is_ag) を取り出す
    （realtimeの新規受注処理とbackfillのAPI補完で共通。2026-09-27切り出し）。"""
    fi = order.get("fulfillmentStartInstructions", [])
    if not fi:
        return None, 0
    addr0 = (fi[0].get("shippingStep", {}).get("shipTo", {}).get("contactAddress")
             or fi[0].get("shippingStep", {}).get("shipTo", {}).get("address")
             or {})
    return addr0.get("countryCode"), (1 if _is_ga_address(addr0) else 0)


def _insert_trx_ebay_order(
    cur, *, account, order_id, buyer, ebay_id, vendor_item_id, qty, order_date,
    price_usd, country, is_ag, item_title, ship_by_date,
) -> None:
    """trx.ebay_ordersへ1明細をINSERTする（commitは呼び出し側）。realtimeの新規受注処理と
    backfillのAPI補完(target=api)で共通（2026-09-27切り出し）。"""
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


# ====================================================================
# ここから追加機能：受注後ステータス継続取得
# ====================================================================
#
# 【対応eBay API】
#   - Fulfillment API   GET /sell/fulfillment/v1/order/{orderId}
#         → orderFulfillmentStatus, cancelStatus, paymentSummary.refunds,
#           lineItems[].lineItemFulfillmentStatus
#   - Fulfillment API   GET /sell/fulfillment/v1/order/{orderId}/shipping_fulfillment
#         → shipmentTrackingNumber, shippingServiceCode, shippedDate
#           ⚠ 配達済み(実際にバイヤーへ届いたかどうか)を示すフィールドは
#             存在しない。minEstimatedDeliveryDate/maxEstimatedDeliveryDateは
#             あくまで「予測」であり、配達確認ではない。
#   - Payment Dispute API GET https://apiz.ebay.com/sell/fulfillment/v1/payment_dispute_summary
#         → 支払い異議申し立て（チャージバック等）
#   - Post-Order API（レガシーだが2026-09-08時点で現行認証情報から利用可能と確認済み）
#         GET https://api.ebay.com/post-order/v2/cancellation/search
#         GET https://api.ebay.com/post-order/v2/return/search
#         GET https://api.ebay.com/post-order/v2/inquiry/search        (未着=INR)
#         GET https://api.ebay.com/post-order/v2/casemanagement/search (eBayケース)
#         ⚠ これらはPost-Order API独自の認証方式で、Authorizationヘッダの
#           スキームは "Bearer" ではなく "IAF" でなければならない
#           （"Bearer <token>" は401 "Bad scheme: Bearer" になることを実機確認済み）。
#         ⚠ item_id / transaction_id 等での絞り込みパラメータは実機検証の結果
#           効果がない（全件返る）ことを確認したため、日付範囲(creation_date_range_*)
#           のみで絞り込み、注文への対応付けはレスポンス中のorderId /
#           itemId・transactionId をこちら側でtrx.ebay_ordersと突き合わせる。
#
# 【業務用status対応表（2026-09-08 実機検証で観測した値ベース）】
#   観測済み(値をAPIレスポンスで実際に確認した)ものには [観測済み] を付す。
#   未観測（該当イベントが検証時点でアカウント内に存在せず、eBay公式ドキュメントの
#   スキーマ上は定義されているが実データで確認できていない）ものには [文書ベース/未観測]
#   を付す。[文書ベース/未観測] の値は本番切替前に実例が出た時点で必ず確認すること。
#
#   【0. 最優先ガード（2026-09-12追加）】
#   cancellation/return/inquiry/case/payment_disputeのいずれかのAPI取得が失敗している
#   場合（429・HTTPエラー・通信例外・JSON解析失敗）、post_order_ok=Falseを渡すことで
#   status判定そのものをスキップし、status=None（既存statusを維持）を返す。
#   「API失敗」を「該当なし」と解釈して誤ったstatusへ書き換える事故
#   （2026-09-11、キャンセル済み→返金済みへの誤書き換え）を防ぐため。
#
#   優先順位（上にあるものほど優先。複数該当時は最も上のものを採用）
#   1. ケース対応中     : casemanagement caseStatusEnum == "OPEN" [文書ベース/未観測]
#                         return.status == "ESCALATED" [観測済み]
#                         payment_dispute open [文書ベース/未観測]
#   2. INAD申告         : return.state in (RETURN_STARTED, ITEM_READY_TO_SHIP)
#                         and reasonType == "SNAD" [観測済み]
#   3. 未着申告         : inquiry（INR）が open [文書ベース/未観測。仕様上の
#                         inquiryStatus値は未確認。inquiry/searchで1件以上ヒットした
#                         時点で「未着申告」として扱い、詳細フィールド名は
#                         実例発生時に確認する]
#   4. 返品返送中       : return.state == "ITEM_SHIPPED"（買い手が返送発送）[文書ベース/未観測]
#   5. 返品到着         : return.state == "ITEM_DELIVERED"（返送品が返送先に到着。
#                         返品返送中とは明確に区別する。返金確定はまだ先）[文書ベース/未観測。
#                         2026-09-09追加]
#   6. 返品申請中       : return.state in (RETURN_STARTED, ITEM_READY_TO_SHIP)
#                         かつ reasonType != "SNAD" [観測済み(READY_FOR_SHIPPING)]
#   7. キャンセル申請中 : cancellation.cancelState == "APPROVAL_PENDING" [観測済み
#                         (1件実例あり。買い手申請→売り手承認待ち)]。
#                         PENDING/IN_PROGRESS/REQUESTEDは文書ベースの類推値で未観測。
#   8. キャンセル済み   : cancellation.cancelState == "CLOSED" かつ
#                         cancelStatus == "CANCEL_CLOSED_WITH_REFUND" [観測済み(76件)]。
#                         cancelState=="CLOSED"でもcancelStatus=="CANCEL_REJECTED"
#                         （売り手拒否／出荷済み通知による自動却下、2026-09-09に3件観測）は
#                         キャンセル不成立であり、この判定から除外し通常フローへ進める。
#   9. 要確認           : return.state/status=="CLOSED"だがsellerTotalRefund/
#                         buyerTotalRefundにactualRefundAmountが存在しないケース
#                         [観測済み・604件中8件]。sellerResponseDueが残ったまま・
#                         timeoutDateが未来のままで、返品却下／撤回／未返送のまま終了した
#                         のか実返金されたのかAPI値だけでは判別できないため、
#                         推測せず「要確認」を返す（本番切替前に手動確認が必要）。
#                         「返品済み」という曖昧な名称は使用しない（2026-09-09、ユーザー指示）。
#   10. 返金済み        : return.state/status=="CLOSED"かつ実際にactualRefundAmountが
#                         存在する場合のみ [観測済み・18件]。
#                         ⚠ 2026-09-12廃止: 旧「返品/キャンセルを介さないpaymentSummary.
#                         refunds実績」という汎用フォールバックは、2026-09-11に
#                         Post-Order検索が429で失敗した際、本来「キャンセル済み」の
#                         注文を誤って「返金済み」にする事故の原因となったため削除した
#                         （キャンセルに伴う返金もpaymentSummary.refundsに現れるため、
#                         cancellation検索が失敗しただけの状態と区別できなかった）。
#                         「返金済み」はreturn CLOSED+実返金額ありの経路のみを根拠とする。
#   11. 配達済み        : Trading API GetOrders の ActualDeliveryTime が存在する
#                         [観測済み・2026-09-09追加]。DB保存済みの配達完了日時・OCのDelivered・
#                         出荷から90日超のみなし配達も配達済み（2026-09-29: 取引完了は廃止）。
#   12. 配送遅延        : 自動判定しない（2026-09-27廃止、ユーザー指示）。以前は
#                         「EstimatedDeliveryTimeMaxを超過＝配送遅延」とプログラムが独自に
#                         判定していた（eBayが遅延状態を返しているわけではない）。今後は人間が
#                         追跡確認・調査依頼等の対応中の注文に手動で設定するstatusとし、
#                         dailyは配送遅延を出荷済みへ戻さない（配達完了・返金等の進展時のみ更新）。
#   13. 出荷済み        : lineItemFulfillmentStatus == "FULFILLED" [観測済み]かつ
#                         配達完了を確認できない（配達予定日を過ぎていても出荷済み）。
#   14. 新規受注        : lineItemFulfillmentStatus == "NOT_STARTED" [観測済み]。
#                         v1では注文レベルのorderFulfillmentStatus（IN_PROGRESSを
#                         含む集約値）を使っていたが、複数明細のうち一部だけ未出荷でも
#                         注文全体がIN_PROGRESSになり、既に出荷済みの明細まで
#                         「新規受注」に誤判定する問題があった。2026-09-09に
#                         明細単位のlineItemFulfillmentStatus（NOT_STARTED/FULFILLEDの
#                         二値のみ、実機確認済み）を使う方式に修正した。
#   （取引完了: 2026-09-29廃止。配達確認済み・みなし配達はすべて配達済みに統一）
#
#   【配達済み・配送遅延判定について(2026-09-09追加)】
#   Fulfillment APIのshipping_fulfillmentには配達完了を示すフィールドが無いが、
#   Trading API GetOrders の
#     Transaction/ShippingServiceSelected/ShippingPackageInfo/ActualDeliveryTime
#   に実配達完了日時が入ることを実機確認した(未配達注文では空であることも確認済み)。
#   （2026-09-27: EstimatedDeliveryTimeMax超過による「配送遅延」の自動判定は廃止。
#   配達予定日はestimated_delivery_max_atとして保存のみ行う）
#   「追跡情報が一定期間更新されていない」「配送例外」の検知は、配送会社側API/
#   スクレイピングが別途必要なため今回は対象外（別フェーズ）。
#
#   【追跡番号・配送会社について(2026-09-09更新)】
#   - tracking_number ← Trading API ShipmentTrackingDetails.ShipmentTrackingNumber
#   - shipping_carrier ← Trading API ShipmentTrackingDetails.ShippingCarrierUsed
#     （実際の配送会社名。例: "SpeedPAK" "DHL Express International" "FedExHomeDelivery"）
#   - shipping_service_code ← Fulfillment API shippingServiceCode（参考値として別列に保持）
#   - 1注文(1明細)に複数のShipmentTrackingDetailsがある場合（eBay International
#     Shippingの国内区間+国際区間など）、情報を1件も捨てず、tracking_numberと
#     shipping_carrierを同じ順番で"; "区切りに連結する。件数・順序は必ず一致させる。
#
# 【通知が必要な状態変化】(2026-09-10 ユーザー指示により縮小)
#   メール・LINEを送信するのは以下2種類のみ。realtime/dailyどちらの検出でも共通。
#     1. 新規受注（既存の別経路 send_new_order_mail/send_line_new_order。本ブロックの対象外）
#     2. キャンセル（キャンセル申請中・キャンセル済み）
#   それ以外（出荷済み・配達済み・配送遅延・返品申請中・返品返送中・返品到着・
#   返金済み・INAD申告・未着申告・ケース対応中・要確認）は通知しない。
#   → 現在DB上のstatusと新しいstatusが異なる場合のみ通知する（同一状態の重複通知はしない）。
#   初回補完（backfillモード）では絶対に通知しない。
# ====================================================================

STATUSES_NEEDING_ATTENTION = {
    "ケース対応中", "INAD申告", "未着申告", "返品返送中", "返品到着",
    "返品申請中", "キャンセル申請中", "要確認",
}
NOTIFY_ON_STATUS = {
    # 2026-09-10、ユーザー指示によりメール/LINE通知は新規受注(既存の別経路)と
    # キャンセル(申請中/済み)のみに縮小。realtime/dailyどちらの検出でも共通。
    "キャンセル申請中", "キャンセル済み",
}
NO_NOTIFY_STATUS = {
    "出荷済み", "配達済み", "配送遅延",
    "返品申請中", "返品返送中", "返品到着", "返金済み",
    "INAD申告", "未着申告", "ケース対応中", "要確認",
    "新規受注",
}

MONITOR_WINDOW_DAYS = 90
# 90日を超えても監視を継続する「未解決」ステータス
UNRESOLVED_STATUSES = {
    "キャンセル申請中", "返品申請中", "返品返送中", "返品到着",
    "未着申告", "INAD申告", "ケース対応中", "配送遅延", "要確認",
    # 2026-09-26追加: 90日を超えても未出荷の注文は異常であり継続確認が必要
    # （未出荷のまま全額返金された2025年の注文が新規受注のまま残っていた）
    "新規受注",
    # 2026-09-27追加: 注文日から90日を過ぎても出荷日から90日以内の注文があるため、
    # 「出荷から90日超のみなし配達済み」を判定できるよう出荷済みも継続確認する
    "出荷済み",
}

# 出荷日からこの日数を超えても配達完了を確認できず「出荷済み」のままの注文は「配達済み」とみなす
# （2026-09-27、ユーザー指示。eBayから配達完了を取得できなくなった古い注文が出荷済みのまま
# 大量に残り、実際に配送中の注文を判別できなくなっていたため）。
DEEMED_DELIVERED_DAYS = 90

# Post-Order API（キャンセル/返品/未着/ケース）の記録を根拠に決まるstatus。
# これらのstatusの注文で、今回Post-Orderの記録を1件も取得できなかった場合は
# 「記録が存在しない」ではなく「取得できなかった」とみなし既存statusを維持する（2026-09-26追加）。
POST_ORDER_DERIVED_STATUSES = {
    "キャンセル申請中", "キャンセル済み", "返品申請中", "返品返送中", "返品到着",
    "INAD申告", "未着申告", "ケース対応中", "返金済み",
}

# 配送進捗の順位。取得できる情報が減っただけ（例: 注文から約90日を過ぎTrading APIが
# 注文を返さなくなった）で進捗が後退する判定結果になった場合は既存statusを維持する（2026-09-26追加）
_DELIVERY_RANK = {"新規受注": 0, "出荷済み": 1, "配送遅延": 1, "配達済み": 2}


# --------------------------------------------------
# DBスキーマ検出（本番ALTER TABLE未実施でも安全に動かすため）
# --------------------------------------------------
_new_columns_cache: dict = {"value": None}


def _new_columns_available(cn) -> bool:
    """trx.ebay_ordersにstatus等の新カラムが追加済みかを確認する。
    未追加の環境でも本スクリプトが例外にならないようにするための安全弁。
    ALTER TABLE実行前は常にFalseになり、状態はログにのみ出力される。"""
    if _new_columns_cache["value"] is not None:
        return _new_columns_cache["value"]
    try:
        cur = cn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'trx' AND TABLE_NAME = 'ebay_orders' AND COLUMN_NAME = 'status'
        """)
        available = cur.fetchone()[0] > 0
    except Exception:
        log.warning("新カラム検出に失敗しました。未追加として扱います", exc_info=True)
        available = False
    _new_columns_cache["value"] = available
    return available


# --------------------------------------------------
# eBay API ラッパー（受注後ステータス系）
# --------------------------------------------------
def _bearer_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }


def _iaf_headers(token: str) -> dict:
    # Post-Order API専用。"Bearer"では401になることを実機確認済み。
    return {
        "Authorization": f"IAF {token}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }


def get_order_detail(account: str, order_id: str) -> dict | None:
    """Fulfillment API: 単一注文の詳細（cancelStatus/orderFulfillmentStatus等）を取得。"""
    token = get_access_token_new(account)
    if not token:
        return None
    try:
        r = requests.get(
            f"https://api.ebay.com/sell/fulfillment/v1/order/{order_id}",
            headers=_bearer_headers(token), timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"[get_order_detail] {order_id}: {r.status_code} {r.text[:300]}")
            return None
        return r.json()
    except Exception:
        log.warning(f"[get_order_detail] {order_id} で例外発生", exc_info=True)
        return None


def get_shipping_fulfillments(account: str, order_id: str) -> list[dict] | None:
    """Fulfillment API: 追跡番号・配送サービスコード・出荷日の一覧を取得。
    1注文に複数口ある場合も全件返す（情報を捨てない）。
    取得失敗時はNone（「出荷記録なし」の空リストと区別する。2026-09-26変更）。"""
    token = get_access_token_new(account)
    if not token:
        return None
    try:
        r = requests.get(
            f"https://api.ebay.com/sell/fulfillment/v1/order/{order_id}/shipping_fulfillment",
            headers=_bearer_headers(token), timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"[shipping_fulfillment] {order_id}: {r.status_code} {r.text[:300]}")
            return None
        return r.json().get("fulfillments", [])
    except Exception:
        log.warning(f"[shipping_fulfillment] {order_id} で例外発生", exc_info=True)
        return None


def get_line_item_fulfillment_status(order_detail: dict | None, ebay_id: str) -> str | None:
    """order_detail(Fulfillment API GetOrder)から、対象ebay_id(legacyItemId)の
    明細単位のlineItemFulfillmentStatusを取り出す。
    注文全体のorderFulfillmentStatus（IN_PROGRESS等、複数明細の集約値）ではなく、
    明細単位の値（NOT_STARTED/FULFILLEDの二値、実機確認済み）を使うことで、
    「一部だけ出荷済みの注文」を誤って新規受注のままにしない。"""
    if not order_detail:
        return None
    for li in order_detail.get("lineItems", []):
        if li.get("legacyItemId") == ebay_id:
            return li.get("lineItemFulfillmentStatus")
    return None


TRADING_NS = {"e": "urn:ebay:apis:eBLBaseComponents"}


def _trading_call(account: str, call_name: str, body_inner: str, compat: str = "1157") -> "ET.Element | None":
    """Trading API(XML/IAF-TOKEN方式)共通呼び出し。ebay_api.pyのTRADING_ENDPOINT/
    TRADING_COMPAT_LEVELをそのまま流用する（新しいエンドポイント設定は増やさない）。"""
    import xml.etree.ElementTree as ET
    from apps.adapters.ebay_api import TRADING_ENDPOINT

    token = get_access_token_new(account)
    if not token:
        return None
    headers = {
        "X-EBAY-API-CALL-NAME": call_name,
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": compat,
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml",
    }
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<{call_name}Request xmlns="urn:ebay:apis:eBLBaseComponents">
  <ErrorLanguage>en_US</ErrorLanguage>
  <WarningLevel>High</WarningLevel>
  {body_inner}
</{call_name}Request>""".encode("utf-8")
    try:
        r = requests.post(TRADING_ENDPOINT, headers=headers, data=body, timeout=45)
        if r.status_code != 200:
            log.warning(f"[trading:{call_name}] {account}: {r.status_code} {r.text[:300]}")
            return None
        root = ET.fromstring(r.text)
        ack = root.findtext("e:Ack", default="", namespaces=TRADING_NS)
        if ack not in ("Success", "Warning"):
            log.warning(f"[trading:{call_name}] {account}: Ack={ack} {r.text[:300]}")
            return None
        return root
    except Exception:
        log.warning(f"[trading:{call_name}] {account} で例外発生", exc_info=True)
        return None


def _parse_trading_dt(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def get_order_trading_detail(account: str, order_id: str) -> dict[str, dict]:
    """Trading API GetOrdersで配達完了日時・配達予定日・追跡情報を明細(ItemID)単位で取得する。
    Fulfillment APIには無い ActualDeliveryTime（実配達完了日時）と、
    ShippingCarrierUsed（実際の配送会社名）、複数区間分のShipmentTrackingDetailsを
    ここから得る（2026-09-09 実機検証で確認済み）。

    戻り値: {legacyItemId(str): {
        "actual_delivery_time": datetime|None,
        "estimated_delivery_max": datetime|None,
        "shipped_time": datetime|None,
        "is_multi_leg": bool,
        "tracking_pairs": [(carrier, tracking_number), ...],  # 出現順そのまま。1件も欠かさない
    }}
    注文が見つからない/取得失敗の場合は空dict。"""
    root = _trading_call(
        account, "GetOrders",
        f"<OrderIDArray><OrderID>{order_id}</OrderID></OrderIDArray><OrderRole>Seller</OrderRole>",
    )
    if root is None:
        return {}

    result: dict[str, dict] = {}
    for order in root.findall(".//e:Order", TRADING_NS):
        is_multi_leg = order.findtext("e:IsMultiLegShipping", default="", namespaces=TRADING_NS) == "true"
        for txn in order.findall(".//e:Transaction", TRADING_NS):
            item_id = txn.findtext("e:Item/e:ItemID", default="", namespaces=TRADING_NS)
            if not item_id:
                continue
            pkg_info = txn.find("e:ShippingServiceSelected/e:ShippingPackageInfo", TRADING_NS)
            actual_delivery = _parse_trading_dt(
                pkg_info.findtext("e:ActualDeliveryTime", default=None, namespaces=TRADING_NS) if pkg_info is not None else None
            )
            est_max = _parse_trading_dt(
                pkg_info.findtext("e:EstimatedDeliveryTimeMax", default=None, namespaces=TRADING_NS) if pkg_info is not None else None
            )
            shipped_time = _parse_trading_dt(txn.findtext("e:ShippedTime", default=None, namespaces=TRADING_NS))

            tracking_pairs = []
            for std in txn.findall("e:ShippingDetails/e:ShipmentTrackingDetails", TRADING_NS):
                carrier = std.findtext("e:ShippingCarrierUsed", default="", namespaces=TRADING_NS)
                tracking = std.findtext("e:ShipmentTrackingNumber", default="", namespaces=TRADING_NS)
                if tracking:
                    tracking_pairs.append((carrier, tracking))

            result[item_id] = {
                "actual_delivery_time": actual_delivery,
                "estimated_delivery_max": est_max,
                "shipped_time": shipped_time,
                "is_multi_leg": is_multi_leg,
                "tracking_pairs": tracking_pairs,
            }
    return result


def get_payment_dispute_summary(account: str, order_id: str) -> tuple[list[dict], bool]:
    """Payment Dispute API（支払い異議申し立て）。apiz.ebay.com専用ホストに注意。
    ⚠ [文書ベース/未観測] 検証時点でこのアカウント群に支払い異議申し立ての実例が
    無く(total=0)、開いた場合のレスポンス中のリストキー名（"paymentDisputeSummaries"
    と仮定）を実データで確認できていない。本番切替前に実例が発生した時点で
    レスポンス構造を確認し、必要ならキー名を修正すること。

    2026-09-12: 戻り値を(結果, 成功フラグ)に変更。
    「該当なし(成功・0件)」と「API呼び出し自体の失敗」を呼び出し元が区別できるようにする
    （2026-09-11の429連発時に、失敗を「該当なし」と誤認識してstatusを誤判定した事故の再発防止）。"""
    token = get_access_token_new(account)
    if not token:
        return [], False
    try:
        r = requests.get(
            "https://apiz.ebay.com/sell/fulfillment/v1/payment_dispute_summary",
            headers=_bearer_headers(token), params={"order_id": order_id}, timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"[payment_dispute] {order_id}: {r.status_code} {r.text[:300]}")
            return [], False
        data = r.json()
        return (data.get("paymentDisputeSummaries", []) or data.get("paymentDisputes", [])), True
    except Exception:
        log.warning(f"[payment_dispute] {order_id} で例外発生", exc_info=True)
        return [], False


def _post_order_search(account: str, path: str, extra_params: dict, date_from: datetime, date_to: datetime) -> tuple[list[dict], bool]:
    """Post-Order API系のsearchエンドポイント共通処理（IAF認証・ページング）。
    2026-09-12: 戻り値を(結果, 成功フラグ)に変更。429・HTTPエラー・通信例外・JSON解析失敗の
    いずれでも成功フラグをFalseにし、「該当なし」と区別できるようにする。
    途中ページで失敗した場合、それまでに取得できた分は結果に残すが成功フラグはFalseとする
    （部分的な結果を「完全な該当なし/該当あり」と誤認しないため、呼び出し元はFalse時は
    resultsの中身を業務判定に使わない設計とすること）。"""
    token = get_access_token_new(account)
    if not token:
        return [], False
    headers = _iaf_headers(token)
    params = dict(extra_params)
    params["creation_date_range_from"] = date_from.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    params["creation_date_range_to"] = date_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    params["limit"] = 200
    params["offset"] = 0

    results: list[dict] = []
    list_key_candidates = ("cancellations", "members")
    while True:
        try:
            r = requests.get(f"https://api.ebay.com/post-order/v2/{path}", headers=headers, params=params, timeout=30)
        except Exception:
            log.warning(f"[post_order:{path}] {account} で例外発生", exc_info=True)
            return results, False
        if r.status_code != 200:
            log.warning(f"[post_order:{path}] {account}: {r.status_code} {r.text[:300]}")
            return results, False
        try:
            data = r.json()
        except Exception:
            log.warning(f"[post_order:{path}] {account}: JSON解析失敗")
            return results, False

        page_items = []
        for key in list_key_candidates:
            if key in data:
                page_items = data.get(key) or []
                break
        results.extend(page_items)

        pag = data.get("paginationOutput") or {}
        total_pages = pag.get("totalPages") or 1
        current_page = (params["offset"] // params["limit"]) + 1
        if current_page >= total_pages or not page_items:
            break
        params["offset"] += params["limit"]

    return results, True


def search_cancellations(account: str, date_from: datetime, date_to: datetime) -> tuple[list[dict], bool]:
    return _post_order_search(account, "cancellation/search", {}, date_from, date_to)


def search_returns(account: str, date_from: datetime, date_to: datetime) -> tuple[list[dict], bool]:
    return _post_order_search(account, "return/search", {}, date_from, date_to)


def search_inquiries(account: str, date_from: datetime, date_to: datetime) -> tuple[list[dict], bool]:
    return _post_order_search(account, "inquiry/search", {}, date_from, date_to)


def search_cases(account: str, date_from: datetime, date_to: datetime) -> tuple[list[dict], bool]:
    return _post_order_search(account, "casemanagement/search", {}, date_from, date_to)


def get_case_detail(account: str, case_id: str) -> dict | None:
    """Post-Order API: caseId指定でケース詳細を1件取得する（2026-09-26追加）。
    casemanagement/searchは直近約2.5ヶ月のケースしか返さないため、returnが
    ESCALATEDの古い注文はケースを確認できずstatus=NULLのまま残っていた（19件）。
    returnのescalationInfo.caseIdで個別取得すれば古いケースも取得できることを実機確認済み
    （19件全て200）。個別取得のレスポンスは状態を"status"キーで返し、検索結果の
    "caseStatusEnum"と同じ値になる（同一caseIdを両APIで取得し9件一致を確認）ため、
    determine_statusがそのまま使えるようcaseStatusEnumへ詰め替えて返す。
    取得失敗時はNone（呼び出し元は従来通り「ケース状態不明」として扱う）。"""
    token = get_access_token_new(account)
    if not token:
        return None
    try:
        r = requests.get(
            f"https://api.ebay.com/post-order/v2/casemanagement/{case_id}",
            headers=_iaf_headers(token), timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"[case_detail] {account} caseId={case_id}: {r.status_code} {r.text[:300]}")
            return None
        case = r.json()
    except Exception:
        log.warning(f"[case_detail] {account} caseId={case_id} で例外発生", exc_info=True)
        return None
    case["caseStatusEnum"] = case.get("status")
    return case


# --------------------------------------------------
# Orange Connex ラストマイル追跡番号解決（dailyモード専用）
# --------------------------------------------------
# 2026-09-09/10 実機調査で確認済み:
#   - shipping_carrier="SpeedPAK"の注文は、eBay APIから取得できる追跡番号が
#     Orange Connex(OC)発番のもので、実際に配達するラストマイル業者(FedEx/DHL等)の
#     追跡番号ではない。
#   - 以下のOrange Connex公開API（認証不要・通常のHTTPリクエストで到達可能）に
#     OC番号を渡すと、ラストマイル追跡番号(waybill.trackingNumber)が
#     waybill.websiteTrackingNumber(=入力したOC番号)と対で返る。
#   - 1リクエスト最大30件。90日分・様々な経過日数のサンプルで100%解決できたが、
#     本番運用では失敗（HTTPエラー/JSON不正/success=false/対象なし）を
#     想定して必ずログに残し、daily処理全体は止めない。
#   - shipping_carrierの値はSpeedPAKのまま保持する。ラストマイル業者名
#     （FedEx/DHL等）は本APIのレスポンスだけでは確実に特定できないため、
#     推測してshipping_carrierへ書き込むことはしない（ユーザー指示）。
_ORANGE_CONNEX_TRACK_URL = "https://azure-cn.orangeconnex.com/oc/capricorn-website/website/v1/tracking/traces/jp"
_ORANGE_CONNEX_BATCH_SIZE = 30
_ORANGE_CONNEX_TIMEOUT_SEC = 20


def _fetch_orange_connex_waybills(oc_tracking_numbers: list[str]) -> tuple[dict[str, dict], dict[str, str]]:
    """
    OC追跡番号のリストを受け取り、(OC番号->waybill生データ, 失敗理由{OC番号: 理由})を返す。
    最大30件ずつバッチ化してOrange Connexの公開追跡APIへ問い合わせる。
    HTTPエラー・JSON不正・success=false・対象番号なしのいずれでも例外を投げず、
    そのバッチ分は「未解決」として扱い、理由をログと戻り値の両方に記録する。
    過剰な再試行はしない（1バッチにつき1回のみ）。
    waybill生データにはtrackingNumber(ラストマイル番号)のほか、lastStatus/traces等が含まれ、
    複数OC番号候補から有効な出荷を判別する際に使う。
    """
    waybills_map: dict[str, dict] = {}
    failed_reason: dict[str, str] = {}

    unique_numbers = list(dict.fromkeys(n for n in oc_tracking_numbers if n))
    for i in range(0, len(unique_numbers), _ORANGE_CONNEX_BATCH_SIZE):
        batch = unique_numbers[i:i + _ORANGE_CONNEX_BATCH_SIZE]
        try:
            r = requests.post(
                _ORANGE_CONNEX_TRACK_URL,
                json={"trackingNumbers": batch, "language": "ja"},
                headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
                timeout=_ORANGE_CONNEX_TIMEOUT_SEC,
            )
        except Exception as e:
            log.warning(f"[orange_connex] 通信エラー（{len(batch)}件）: {e}")
            for n in batch:
                failed_reason[n] = f"通信エラー: {e}"
            continue

        if r.status_code != 200:
            log.warning(f"[orange_connex] HTTPエラー status={r.status_code}（{len(batch)}件）: {r.text[:200]}")
            for n in batch:
                failed_reason[n] = f"HTTPエラー status={r.status_code}"
            continue

        try:
            data = r.json()
        except Exception as e:
            log.warning(f"[orange_connex] JSON解析失敗（{len(batch)}件）: {e}")
            for n in batch:
                failed_reason[n] = f"JSON解析失敗: {e}"
            continue

        if not data.get("success"):
            log.warning(f"[orange_connex] success=false（{len(batch)}件）: {str(data)[:300]}")
            for n in batch:
                failed_reason[n] = "success=false"
            continue

        result = data.get("result") or {}
        waybills = result.get("waybills") or []
        not_exists = result.get("notExistsTrackingNumbers") or []
        if not_exists:
            log.info(f"[orange_connex] 対象番号なし({len(not_exists)}件): {not_exists}")
            for n in not_exists:
                failed_reason[n] = "対象番号なし(notExistsTrackingNumbers)"

        matched_oc_numbers = set()
        for w in waybills:
            oc_no = w.get("websiteTrackingNumber")
            last_mile = w.get("trackingNumber")
            if oc_no and last_mile:
                waybills_map[oc_no] = w
                matched_oc_numbers.add(oc_no)
            elif oc_no:
                failed_reason[oc_no] = "waybillにtrackingNumberなし"
                matched_oc_numbers.add(oc_no)

        for n in batch:
            if n not in matched_oc_numbers and n not in failed_reason:
                failed_reason[n] = "レスポンスに対応するwaybillなし"

    return waybills_map, failed_reason


def resolve_orange_connex_last_mile(oc_tracking_numbers: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """
    OC追跡番号のリストを受け取り、(解決できた{OC番号: ラストマイル番号}, 失敗理由{OC番号: 理由})を返す。
    互換維持用の薄いラッパー。判別ロジックが不要な単純な1対1解決に使う。
    """
    waybills_map, failed_reason = _fetch_orange_connex_waybills(oc_tracking_numbers)
    resolved = {oc_no: w["trackingNumber"] for oc_no, w in waybills_map.items()}
    return resolved, failed_reason


# OrangeConnexのlastStatusが以下のような文言を含む場合、その出荷は
# キャンセル・無効化された旧ラベルとみなし、ラストマイル番号選定の対象から除外する。
_OC_CANCELLED_STATUS_MARKERS = ("cancel", "キャンセル", "void", "invalid")


def _oc_status_is_cancelled(status: str | None) -> bool:
    if not status:
        return False
    s = status.lower()
    return any(marker in s for marker in _OC_CANCELLED_STATUS_MARKERS)


def select_valid_oc_candidate(
    oc_numbers: list[str], waybills_map: dict[str, dict],
) -> tuple[str | None, str]:
    """
    1つの注文に複数のOC追跡番号候補がある場合に、有効な出荷に対応する
    ラストマイル番号を1件だけ判別する。先頭を無条件に採用することはしない。

    除外条件（キャンセル・無効化された旧ラベルとみなす）:
      - lastStatusに"cancel"/"キャンセル"等の語を含む
      - trace（追跡イベント）が1件以下（＝実際の輸送履歴がなく、登録のみで終わっている）

    除外後に候補が1件だけ残れば採用。0件または2件以上残る場合は判別不可とし、
    (None, 理由) を返す（呼び出し側はtracking_numberを更新せず、理由をログに残す）。
    """
    candidates: list[tuple[str, str, str | None, int]] = []
    for oc_no in oc_numbers:
        w = waybills_map.get(oc_no)
        if not w or not w.get("trackingNumber"):
            continue
        status = w.get("lastStatus")
        trace_count = len(w.get("traces") or [])
        if _oc_status_is_cancelled(status):
            continue
        if trace_count <= 1:
            continue
        candidates.append((oc_no, w["trackingNumber"], status, trace_count))

    if len(candidates) == 1:
        oc_no, last_mile, status, trace_count = candidates[0]
        return last_mile, f"候補{len(oc_numbers)}件中1件のみ有効(oc_no={oc_no} status={status} trace={trace_count}件)"
    if len(candidates) == 0:
        return None, f"候補{len(oc_numbers)}件すべて無効（キャンセル扱いまたはtrace不足）: {oc_numbers}"
    return None, f"候補{len(oc_numbers)}件中{len(candidates)}件が有効で判別不可: {[c[0] for c in candidates]}"


# eBay casemanagement APIのcaseStatusEnumは実機で "OPEN"/"CLOSED"/"CS_CLOSED" の
# 3種類の混在を確認済み（2026-09-16）。ホワイトリスト方式にし、未知の値は
# 「要確認」に倒す（ブラックリストで判定すると将来の未知の値を誤って
# 「開いている」扱いしてしまう恐れがあるため）。
_CASE_OPEN_STATUS_VALUES = {"OPEN"}
_CASE_CLOSED_STATUS_VALUES = {"CLOSED", "CS_CLOSED"}


def _line_item_refund_state(
    order_detail: dict | None, order_detail_ok: bool, ebay_id: str, return_: dict | None,
    line_level_only: bool = False,
) -> tuple[str, str]:
    """この明細(ebay_id)が全額返金されたかを ("full" | "not_full" | "unknown", 根拠) で返す。
    2026-09-26追加（ユーザー指示: 返金判定にはPost-OrderのactualRefundAmountだけでなく
    注文側の確定した返金記録も使う。API取得失敗を「返金なし」と解釈しない）。

    実機確認(2026-09-26)に基づく判定:
    - 明細のlineItems[].refunds[].amountはlineItems[].totalと同じ単位（買い手支払額ベース）で
      比較できる（例: 23-14767 明細total 529.99・明細refund 529.99）。一方
      paymentSummary.refunds[].amountは手取りベースで金額が一致しない（同 450.42）。
    - 注文全体のorderPaymentStatusは複数明細注文で1明細だけ返金するとPARTIALLY_REFUNDEDに
      なる（14-14447-68361）ほか、単一明細の全額返金でもPARTIALLY_REFUNDEDの例がある
      （23-14767）ため、全額かどうかは明細単位の返金額で判定する。
    - orderPaymentStatus=FULLY_REFUNDEDは注文全体（=全明細）の全額返金を表すため、
      確定した返金記録（明細/注文のrefunds、またはreturnのactualRefundAmount）を伴う場合のみ全額とする。
    line_level_only=True の場合は明細の返金額による判定のみ行う（未出荷注文の返金判定用）。"""
    if not order_detail_ok or not order_detail:
        return "unknown", "注文詳細(Fulfillment API)を取得できず返金状況を確認できない"
    order_refunds = (order_detail.get("paymentSummary") or {}).get("refunds", []) or []
    unsettled = [r.get("refundStatus") for r in order_refunds if r.get("refundStatus") != "REFUNDED"]
    if unsettled:
        return "unknown", f"確定していない返金記録あり (refundStatus={unsettled})"
    line = next((li for li in order_detail.get("lineItems", []) if li.get("legacyItemId") == ebay_id), None)
    if line is None:
        return "unknown", f"注文詳細に対象明細(legacyItemId={ebay_id})が見つからない"

    line_total = float((line.get("total") or {}).get("value") or 0)
    line_refunded = sum(float((r.get("amount") or {}).get("value") or 0) for r in (line.get("refunds") or []))
    if line_total > 0 and line_refunded >= line_total - 0.005:
        return "full", f"明細返金額{line_refunded:.2f}/明細金額{line_total:.2f}"

    if line_level_only:
        return "not_full", f"明細返金額{line_refunded:.2f}/明細金額{line_total:.2f}"

    return_actual = bool(return_ and (
        (return_.get("sellerTotalRefund") or {}).get("actualRefundAmount")
        or (return_.get("buyerTotalRefund") or {}).get("actualRefundAmount")
    ))
    if order_detail.get("orderPaymentStatus") == "FULLY_REFUNDED":
        if order_refunds or line_refunded or return_actual:
            return "full", "orderPaymentStatus=FULLY_REFUNDEDかつ返金記録あり"
        return "unknown", "orderPaymentStatus=FULLY_REFUNDEDだが返金記録が見つからない"
    return "not_full", (
        f"明細返金額{line_refunded:.2f}/明細金額{line_total:.2f} "
        f"orderPaymentStatus={order_detail.get('orderPaymentStatus')}"
    )


def _outcome_note(case: dict) -> str:
    """ケースの売り手側結果（個別取得したケースにのみ含まれる）を理由文用に整形する。"""
    return f", sellerOutcome={case['sellerOutcome']}" if case.get("sellerOutcome") else ""


def _resolve_closed_outcome(
    context: str, *, order_detail: dict | None, order_detail_ok: bool, ebay_id: str,
    return_: dict | None, delivered_at: datetime | None,
) -> tuple[str | None, str] | None:
    """返品またはケースが終了した注文のstatusを決める（2026-09-26、ユーザー指示のルール）。
    - 全額返金を確認できる                       → 返金済み
    - 一部返金/返金なし ＋ 配達済みの根拠あり      → 配達済み
    - 返金状況を確認できない（API取得失敗等）      → (None, 理由)＝既存statusを維持
    - 一部返金/返金なしで配達済みを確認できない    → None（呼び出し元は後続の通常の配送状態判定へ進む。
      推測で配達済みにしない）"""
    refund_state, refund_basis = _line_item_refund_state(order_detail, order_detail_ok, ebay_id, return_)
    if refund_state == "full":
        return "返金済み", f"{context}・全額返金確認 ({refund_basis})"
    if refund_state == "unknown":
        log.warning(f"[determine_status] {context}だが返金状況を確認できないため判定不可: ebay_id={ebay_id} {refund_basis}")
        return None, f"{context}だが返金状況を確認できないためstatus判定不可（既存statusを維持）: {refund_basis}"
    if delivered_at is not None:
        return "配達済み", f"{context}・全額返金なし({refund_basis})・配達完了 ({delivered_at.isoformat()})"
    return None


# --------------------------------------------------
# ステータス判定エンジン
# --------------------------------------------------
def determine_status(
    *,
    order_detail: dict | None,
    cancellation: dict | None,
    return_: dict | None,
    inquiry_open: bool,
    case: dict | None,
    payment_dispute_open: bool,
    order_date: datetime,
    ebay_id: str = "",
    actual_delivery_time: datetime | None = None,
    estimated_delivery_max: datetime | None = None,
    post_order_ok: bool = True,
    order_detail_ok: bool = True,
    shipped_line_item_ids: set[str] | None = None,
) -> tuple[str | None, str]:
    """
    現在の業務用statusと、通知本文に使う詳細理由(reason_detail)を返す。

    actual_delivery_time / estimated_delivery_max は、Trading APIから取得できない場合
    （注文から約90日超）は呼び出し元でDB保存済みの値を渡す（2026-09-26）。

    shipped_line_item_ids: shipping_fulfillmentに記録された出荷済みlineItemIdの集合。
    Noneは「出荷記録を取得できなかった（不明）」を表す（2026-09-26追加）。
    優先順位はファイル冒頭コメントの対応表を参照。
    不明なAPI値（未定義の組み合わせ）を検知した場合は推測で変換せず、
    "要確認" を返し、ログにraw値を残す（本番切替前に必ず対応表を更新すること）。

    actual_delivery_time / estimated_delivery_max はTrading API(GetOrders)の
    ActualDeliveryTime / EstimatedDeliveryTimeMax（2026-09-09 実機検証で追加）。

    post_order_ok: cancellation/return/inquiry/case/payment_dispute のいずれかの
    API取得が失敗している場合はFalseを渡す。2026-09-11に発生した事故
    （429でPost-Order検索が失敗し「該当なし」と誤認識して「キャンセル済み」を
    「返金済み」へ誤って書き換えた）の再発防止のため、post_order_ok=Falseの場合は
    他の引数の内容に関わらずstatus判定そのものを行わず、status=None
    （「今回は判定できなかった。既存statusを維持する」の意）を返す。

    order_detail_ok: Fulfillment API（GetOrder単体）の取得自体が失敗した場合に
    Falseを渡す（2026-09-15追加）。過去注文の一括backfillで判明: Fulfillment APIの
    注文単体エンドポイントは発注から約2年（実機確認: 718日は成功、751日以降は
    "Invalid Order Id"で失敗）を過ぎると取得できなくなる。この場合
    order_detail=Noneとなり、case/return/inquiry/cancellationのいずれにも
    該当しない注文はorder_detail_ok無しでは「新規受注（未出荷）」に誤判定されて
    しまう（実際には2年以上前の注文が"新規"のはずがない＝推測による誤り）。
    order_detail_ok=Falseの場合、7.の通常配送進捗判定（order_detailに依存する
    唯一の分岐）には進まずstatus=Noneを返す。1〜6のcase/return/inquiry/
    cancellation判定はPost-Order検索由来でorder_detailと無関係のため、
    order_detail_ok=Falseでも通常通り機能する。
    """
    if not post_order_ok:
        return None, "Post-Order API取得失敗のためstatus判定をスキップ（既存statusを維持）"

    now = datetime.now(timezone.utc)
    if order_date is not None and order_date.tzinfo is None:
        # trx.ebay_orders.order_dateはdatetime2(tzなし)でUTC相当の値が入っているため付与する
        order_date = order_date.replace(tzinfo=timezone.utc)

    # 1. eBayケース / 支払い異議申し立て（最優先）
    # 2026-09-16修正: caseは以前「OPEN以外は無条件で通過」だったが、実機で
    # caseStatusEnumが"CLOSED"だけでなく"CS_CLOSED"も使われることを確認した。
    # ホワイトリスト(_CASE_OPEN_STATUS_VALUES)に一致しない値は「要確認」とし、
    # 未知の値を誤って「開いている」または「素通り」扱いしない。
    if case is not None:
        case_status = case.get("caseStatusEnum")
        if case_status in _CASE_OPEN_STATUS_VALUES:
            return "ケース対応中", f"eBayケース対応中 (caseId={case.get('caseId')}, caseType={case.get('caseType')})"
        if case_status not in _CASE_CLOSED_STATUS_VALUES:
            log.warning(f"[determine_status] 未対応のcaseStatusEnum: {case_status} (caseId={case.get('caseId')})")
            return "要確認", f"case未対応のcaseStatusEnum={case_status} (caseId={case.get('caseId')})"
        # CLOSED/CS_CLOSEDの場合は他シグナル（返品/返金）で最終状態を決める。ここでは通過。
    if payment_dispute_open:
        return "ケース対応中", "支払い異議申し立て対応中"

    closed_kwargs = dict(
        order_detail=order_detail, order_detail_ok=order_detail_ok, ebay_id=ebay_id,
        return_=return_, delivered_at=actual_delivery_time,
    )

    # 2. 返品（return）
    if return_ is not None:
        state = return_.get("state")
        status = return_.get("status")
        reason_type = (return_.get("creationInfo") or {}).get("reasonType")

        if status == "ESCALATED":
            # 2026-09-16修正: 以前はreturnのESCALATED履歴だけで無条件に「ケース対応中」と
            # 判定していた。実機調査で、エスカレーション先のケースが実際には既に
            # CS_CLOSEDで解決済み（2ヶ月以上前）にも関わらず「ケース対応中」のまま
            # という誤判定を1件確認した。また、エスカレーションから時間が経過した
            # ケースはcasemanagement/search APIで現在状態を取得できなくなる
            # （実機確認: 検索結果に現れるケースは直近2.5ヶ月以内のみ）。
            # このため、対応するcase(caseStatusEnum)を実際に確認できた場合のみ
            # その現在状態を根拠にし、確認できない場合は推測せずstatus=Noneを返す
            # （既存statusを維持。判定不可として報告する）。
            if case is None:
                escalated_case_id = (return_.get("escalationInfo") or {}).get("caseId")
                log.warning(
                    f"[determine_status] returnがESCALATEDだが対応するケースの現在状態を"
                    f"casemanagement APIで確認できないため判定不可とする: "
                    f"returnId={return_.get('returnId')} caseId={escalated_case_id}"
                )
                return None, (
                    f"returnがケースへエスカレーション済みだが現在のケース状態を確認できないため"
                    f"status判定不可（既存statusを維持）: returnId={return_.get('returnId')} caseId={escalated_case_id}"
                )
            case_status = case.get("caseStatusEnum")
            if case_status in _CASE_OPEN_STATUS_VALUES:
                return "ケース対応中", f"返品がeBayケースへエスカレーション・ケース対応中 (caseId={case.get('caseId')})"
            if case_status not in _CASE_CLOSED_STATUS_VALUES:
                log.warning(f"[determine_status] returnがESCALATEDだがcaseStatusEnum未対応: {case_status}")
                return "要確認", f"return ESCALATED、case未対応のcaseStatusEnum={case_status} (caseId={case.get('caseId')})"
            closed_context = (
                f"returnがケースへエスカレーション後、ケース({case_status}{_outcome_note(case)})で終了"
                f" (returnId={return_.get('returnId')}, reason={reason_type})"
            )

        elif state == "CLOSED" and status == "CLOSED":
            # 2026-09-09実機検証: state/status=CLOSEDでもreturnオブジェクト自体に
            # actualRefundAmountが無いケースがある（返品不成立・撤回等）。返金の有無は
            # _resolve_closed_outcome（注文側の返金記録も使う）で判定する。
            closed_context = f"return CLOSED/CLOSED (returnId={return_.get('returnId')}, reason={reason_type})"

        elif state in ("RETURN_STARTED", "ITEM_READY_TO_SHIP"):
            if reason_type == "SNAD":
                return "INAD申告", f"INAD(商品説明と異なる)申告あり (reason={return_.get('creationInfo', {}).get('reason')})"
            return "返品申請中", f"返品申請あり (reason={return_.get('creationInfo', {}).get('reason')})"

        elif state == "ITEM_SHIPPED":
            return "返品返送中", f"返品商品を返送中 (state={state})"

        elif state == "ITEM_DELIVERED":
            # 2026-09-09: ITEM_SHIPPEDと同一視していたが、ITEM_DELIVEREDは返品商品が
            # 返送先へ到着済みという別の状態なので区別する（返金確定はまだ先）。
            return "返品到着", f"返品商品が返送先に到着 (state={state})"

        elif state == "PARTIAL_REFUND_DECLINED" and status == "RETURN_REQUESTED" and reason_type == "SNAD":
            # 2026-09-26実機確認(18-15144-56405): 売り手の一部返金提案を買い手が拒否し、
            # 返品要求(SNAD)がOPENのまま残っている状態（ユーザー指示でINAD申告とする）
            return "INAD申告", f"INAD申告・一部返金提案を買い手が拒否し返品要求継続中 (state={state}, status={status})"

        else:
            log.warning(f"[determine_status] 未対応のreturn state/status組み合わせ: state={state} status={status}")
            return "要確認", f"return未対応値 state={state} status={status}"

        # 返品終了: 全額返金→返金済み／配達済みの根拠あり→配達済み／返金状況不明→既存status維持。
        # いずれでもなければ後続の通常の判定（未着申告・キャンセル・配送状態）へ進む
        outcome = _resolve_closed_outcome(closed_context, **closed_kwargs)
        if outcome is not None:
            return outcome

    elif case is not None and case.get("caseStatusEnum") in _CASE_CLOSED_STATUS_VALUES:
        # 返品を伴わないケース（未着ケース等）の終了も同じルールで判定する（2026-09-26追加）
        outcome = _resolve_closed_outcome(
            f"ケース({case.get('caseType')}, {case.get('caseStatusEnum')}{_outcome_note(case)})で終了"
            f" (caseId={case.get('caseId')})",
            **closed_kwargs,
        )
        if outcome is not None:
            return outcome

    # 3. 未着申告（Item Not Received Inquiry）
    if inquiry_open:
        return "未着申告", "未着(INR)申告あり"

    # 4. キャンセル
    if cancellation is not None:
        cancel_state = cancellation.get("cancelState")
        cancel_status = cancellation.get("cancelStatus")
        if cancel_state == "CLOSED":
            if cancel_status == "CANCEL_CLOSED_WITH_REFUND":
                return "キャンセル済み", f"キャンセル完了 ({cancel_status})"
            if cancel_status == "CANCEL_REJECTED":
                # 2026-09-09実機検証で3件確認。cancelState=CLOSEDでも
                # 「拒否」＝キャンセルは成立していない。通常の配送フローへ進める
                # （このifブロックを抜けて後続の判定に委ねる。returnせず素通りする）。
                pass
            else:
                log.warning(f"[determine_status] 未対応のcancelStatus: {cancel_status} (cancelState=CLOSED)")
                return "要確認", f"cancellation未対応のcancelStatus={cancel_status}"
        elif cancel_state in ("PENDING", "IN_PROGRESS", "REQUESTED", "APPROVAL_PENDING"):
            # APPROVAL_PENDING: 2026-09-08の実機検証(過去90日604件)で実際に観測。
            # 買い手のキャンセル申請が出され、売り手の承認待ちの状態。
            return "キャンセル申請中", f"キャンセル申請中 ({cancellation.get('cancelReason')})"
        else:
            log.warning(f"[determine_status] 未対応のcancelState: {cancel_state}")
            return "要確認", f"cancellation未対応値 cancelState={cancel_state}"

    # 5. Fulfillment APIのcancelStatus（Post-Order検索に出てこない直後の申請を拾う保険）
    od_cancel = (order_detail or {}).get("cancelStatus") or {}
    if od_cancel.get("cancelState") == "CANCEL_REQUESTED":
        return "キャンセル申請中", "キャンセル申請あり（注文詳細のcancelStatusより検知）"

    # 6. (廃止 2026-09-12) 旧「返品・キャンセル経由でないもの」のpaymentSummary.refunds
    # 汎用フォールバックは、2026-09-11にPost-Order検索が429で失敗した際、
    # 本来「キャンセル済み」であるべき注文を「返金済み」に誤判定する事故の原因となった
    # （キャンセルに伴う返金もpaymentSummary.refundsに現れるため、cancellation検索が
    # 失敗しただけの状態と本当に「返品・キャンセル以外の返金」を区別できなかった）。
    # 「返金済み」は上記2.の返品ベース判定（return CLOSED + actualRefundAmountあり）
    # のみを根拠とし、この汎用フォールバックは使用しない（ユーザー指示）。

    # 7. 通常の配送進捗（明細＝ebay_id単位のlineItemFulfillmentStatusを使う。
    # 注文レベルのorderFulfillmentStatus=IN_PROGRESSは「同一注文内の他の明細が
    # 未出荷」なだけの集約値であり、この明細自体の状態を表さないため使わない）
    if not order_detail_ok:
        return None, "Fulfillment API取得失敗（対象注文が古すぎる等）のためstatus判定不可（既存statusを維持）"

    fulfillment_status = get_line_item_fulfillment_status(order_detail, ebay_id)
    line_item_id = next(
        (li.get("lineItemId") for li in (order_detail or {}).get("lineItems", []) if li.get("legacyItemId") == ebay_id),
        None,
    )
    ship_note = ""
    if fulfillment_status != "FULFILLED" and shipped_line_item_ids and line_item_id in shipped_line_item_ids:
        # 2026-09-26実機確認: 出荷後に返金された注文等で、shipping_fulfillmentに対象明細の
        # 出荷記録があるのに明細のlineItemFulfillmentStatusがNOT_STARTEDのものがある（5件中4件が
        # 出荷後の返金あり）。実際の出荷記録を優先し「未出荷」とは扱わない（ユーザー指示）。
        log.info(
            f"[determine_status] 明細status={fulfillment_status}だがshipping_fulfillmentに出荷記録あり"
            f"のため出荷済みとして判定: ebay_id={ebay_id} lineItemId={line_item_id}"
        )
        fulfillment_status = "FULFILLED"
        ship_note = "（明細statusはNOT_STARTEDだが出荷記録あり）"

    if fulfillment_status == "FULFILLED":
        refund_state, refund_basis = _line_item_refund_state(order_detail, order_detail_ok, ebay_id, None)
        if refund_state == "full":
            return "返金済み", f"出荷後に全額返金 ({refund_basis}){ship_note}"
        if refund_state == "unknown":
            log.warning(f"[determine_status] 出荷済みだが返金状況を確認できないため判定不可: ebay_id={ebay_id} {refund_basis}")
            return None, f"出荷済みだが返金状況を確認できないためstatus判定不可（既存statusを維持）: {refund_basis}"
        if actual_delivery_time is not None:
            # 2026-09-29: 「取引完了」は使わない（ユーザー指示）。配達を確認できたものは経過日数に関わらず配達済み
            return "配達済み", f"配達完了 ({actual_delivery_time.isoformat()}){ship_note}"
        # 2026-09-27: 配達予定日の超過を理由に「配送遅延」とする独自判定は廃止（ユーザー指示）。
        # 配達完了を確認できない出荷済みの明細は、予定日を過ぎていても「出荷済み」とする
        return "出荷済み", f"出荷済み（配達完了の記録なし）{ship_note}"

    if fulfillment_status in ("NOT_STARTED", None):
        if shipped_line_item_ids is not None:
            # 出荷記録を取得できたうえで対象明細の出荷記録が無い＝未出荷が確定している場合のみ、
            # 明細単位の全額返金を「返金済み」とする（キャンセル記録が無いためキャンセル済みにはしない）
            refund_state, refund_basis = _line_item_refund_state(
                order_detail, order_detail_ok, ebay_id, None, line_level_only=True,
            )
            if refund_state == "full":
                return "返金済み", f"未出荷のまま全額返金（キャンセル記録なし） ({refund_basis})"
        # 2026-09-16追加: 実機調査で、注文全体のorderFulfillmentStatus=FULFILLEDなのに
        # 対象明細のlineItemFulfillmentStatusがNOT_STARTEDという矛盾を確認した。
        # 対象明細の出荷記録も無い場合は、どちらが正しいか推測せず「要確認」とする。
        order_level_status = (order_detail or {}).get("orderFulfillmentStatus")
        if order_level_status == "FULFILLED":
            log.warning(
                f"[determine_status] 注文全体はFULFILLEDだが明細のlineItemFulfillmentStatusは"
                f"{fulfillment_status}で出荷記録も確認できないため要確認とする: ebay_id={ebay_id}"
            )
            return "要確認", (
                f"注文全体status=FULFILLEDだが明細status={fulfillment_status}・出荷記録も確認できないため要確認"
            )
        return "新規受注", "未出荷"

    log.warning(f"[determine_status] 未対応のlineItemFulfillmentStatus: {fulfillment_status}")
    return "要確認", f"lineItemFulfillmentStatus未対応値={fulfillment_status}"


# --------------------------------------------------
# ステータス変化の通知（メール・LINE）
# --------------------------------------------------
def _send_plain_mail_safe(subject: str, body: str) -> bool:
    """apps.common.utils.send_mail()を使わない専用のメール送信関数。
    2026-09-09 daily本番実行で判明: utils.send_mail()はSMTP送信自体は成功するが、
    直後のprint("📧...")がpythonw.exe実行時（stderrがリダイレクトされ
    StreamHandlerが有効な環境）にUnicodeEncodeErrorでクラッシュし、例外処理中の
    print("❌...")も同様にクラッシュするため、呼び出し元には常に「送信失敗」として
    伝播してしまう不具合がある（実際にはメールは届いている）。
    共有ファイル(apps/common/utils.py)は影響範囲が広いため変更せず、
    本ファイル内だけで完結する絵文字なしの送信関数を用意して回避する。"""
    import smtplib
    from email.mime.text import MIMEText

    sender_email = os.getenv("GMAIL_SENDER_EMAIL")
    password = os.getenv("GMAIL_APP_PASSWORD")
    if not sender_email or not password:
        log.warning("[_send_plain_mail_safe] GMAIL_SENDER_EMAIL/GMAIL_APP_PASSWORDが未設定です")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = sender_email
    msg["To"] = sender_email
    msg["Subject"] = subject

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        return True
    except Exception:
        log.warning("[_send_plain_mail_safe] メール送信失敗", exc_info=True)
        return False


def notify_status_change(
    *, account: str, order_id: str, ebay_id: str, vendor_item_id: str,
    old_status: str | None, new_status: str, reason_detail: str,
    item_title: str | None = None, buyer: str | None = None, country: str | None = None,
    tracking_number: str | None = None, shipping_carrier: str | None = None,
    estimated_delivery_max_at: datetime | None = None,
) -> None:
    # Windows(cp932)コンソールでのUnicodeEncodeError再発を避けるため、
    # 通知件名には絵文字を使わない（2026-09-09、ユーザー指示により変更）。
    subject = f"【状態変化】{account} {new_status}"

    if new_status == "配送遅延":
        # spec: 配送遅延通知には最低限、アカウント/注文番号/商品名/購入者/国/
        # 追跡番号/配送会社/配達予定日/現在日時を含める
        now_jst = datetime.now(_JST)
        subject = f"【重要・配送遅延】{account} {order_id}"
        body_text = (
            f"配達予定日を過ぎても配達完了が確認できていません。\n"
            f"\n"
            f"eBayアカウント: {account}\n"
            f"注文番号: {order_id}\n"
            f"商品名: {item_title or '(不明)'}\n"
            f"購入者: {buyer or '(不明)'}\n"
            f"国: {country or '(不明)'}\n"
            f"追跡番号: {tracking_number or '(不明)'}\n"
            f"配送会社: {shipping_carrier or '(不明)'}\n"
            f"配達予定日: {estimated_delivery_max_at.astimezone(_JST).strftime('%Y-%m-%d %H:%M JST') if estimated_delivery_max_at else '(不明)'}\n"
            f"現在日時: {now_jst.strftime('%Y-%m-%d %H:%M JST')}\n"
            f"\n"
            f"eBay: https://www.ebay.com/itm/{ebay_id}\n"
        )
    else:
        body_text = (
            f"注文: {order_id}\n"
            f"eBay ItemID: {ebay_id}\n"
            f"SKU: {vendor_item_id}\n"
            f"商品名: {item_title or '(不明)'}\n"
            f"購入者: {buyer or '(不明)'}\n"
            f"国: {country or '(不明)'}\n"
            f"旧status: {old_status or '(未設定)'}\n"
            f"新status: {new_status}\n"
            f"詳細: {reason_detail}\n"
            f"eBay: https://www.ebay.com/itm/{ebay_id}\n"
        )

    if not NOTIFICATION_ENABLED:
        log.info(
            f"[通知抑止] NOTIFICATION_ENABLED=Falseのためメール・LINEを送信しません "
            f"order_id={order_id} ebay_id={ebay_id} old_status={old_status} new_status={new_status}\n"
            f"件名: {subject}\n{body_text}"
        )
        return

    if TEST_MODE:
        log.info(f"[TEST_MODE] メール/LINE送信予定（実送信なし）\n件名: {subject}\n{body_text}")
        return

    mail_ok = _send_plain_mail_safe(subject, body_text)
    if mail_ok:
        log.info(f"[notify_status_change] メール送信成功 order_id={order_id}")
    else:
        log.warning(f"[notify_status_change] メール送信失敗 order_id={order_id}")

    try:
        token = _get_line_token(account)
        send_line_broadcast(token, text=f"{subject}\n\n{body_text}")
    except Exception:
        log.warning(f"[notify_status_change] LINE送信失敗 order_id={order_id}", exc_info=True)


# --------------------------------------------------
# DB更新（statusカラム）
# --------------------------------------------------
def update_order_status(
    cn, *, order_id: str, ebay_id: str, status: str | None, tracking_number: str | None,
    shipping_carrier: str | None, shipping_service_code: str | None = None,
    estimated_delivery_max_at: datetime | None = None, delivered_at: datetime | None = None,
    status_changed: bool, dry_run: bool, skip_status: bool = False,
) -> None:
    """trx.ebay_ordersを更新する。
    skip_status=True の場合（2026-09-12追加）: Post-Order API取得失敗のため
    status/status_updated_at/status_checked_atは一切更新しない
    （＝既存statusを維持し、成功扱いにもしない）。tracking_number等の
    Fulfillment/Trading/OC由来の項目のみ、取得できていれば更新する。
    tracking_number等はいずれもCOALESCE(?, 既存値)で書き込むため、
    該当APIが失敗してNoneのまま渡された場合に既存の正しい値を消してしまうことはない。"""
    # 既存カラム(order_date/ship_by_date)と同じくUTCのnaive datetime2として保存する
    # （created_atのみ例外でサーバーローカル時刻=JSTのsysdatetime()既定値を使っているが、
    # 業務ロジックで比較するorder_date系はUTCで統一されているため、新カラムもそれに合わせる。
    # 2026-09-09、GETUTCDATE()とGETDATE()の差分で実機確認済み）。
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    est_max_naive = estimated_delivery_max_at.astimezone(timezone.utc).replace(tzinfo=None) if estimated_delivery_max_at else None
    delivered_at_naive = delivered_at.astimezone(timezone.utc).replace(tzinfo=None) if delivered_at else None

    if not _new_columns_available(cn):
        log.info(
            f"[SKIP:カラム未追加] order_id={order_id} ebay_id={ebay_id} "
            f"status={status} tracking={tracking_number} carrier={shipping_carrier} "
            f"service_code={shipping_service_code} est_max={est_max_naive} delivered_at={delivered_at_naive} "
            f"（ALTER TABLE未実施のためDB書き込みは行わずログのみ）"
        )
        return

    if skip_status:
        if dry_run:
            log.info(
                f"[DRY-RUN][status維持] order_id={order_id} ebay_id={ebay_id} "
                f"tracking={tracking_number} carrier={shipping_carrier} service_code={shipping_service_code} "
                f"est_max={est_max_naive} delivered_at={delivered_at_naive} "
                f"（判定不可のためstatus/status_updated_at/status_checked_atは変更しない）"
            )
            return
        cur = cn.cursor()
        cur.execute("""
            UPDATE trx.ebay_orders
            SET tracking_number = COALESCE(?, tracking_number),
                shipping_carrier = COALESCE(?, shipping_carrier),
                shipping_service_code = COALESCE(?, shipping_service_code),
                estimated_delivery_max_at = COALESCE(?, estimated_delivery_max_at),
                delivered_at = COALESCE(?, delivered_at)
            WHERE order_id = ? AND ebay_id = ?
        """, tracking_number, shipping_carrier, shipping_service_code,
            est_max_naive, delivered_at_naive, order_id, ebay_id)
        cn.commit()
        log.info(
            f"[UPDATE:tracking-only] order_id={order_id} ebay_id={ebay_id} "
            f"tracking={tracking_number} carrier={shipping_carrier} "
            f"（判定不可のためstatusは維持・status_checked_atも更新せず）"
        )
        return

    if dry_run:
        log.info(
            f"[DRY-RUN] UPDATE予定: order_id={order_id} ebay_id={ebay_id} "
            f"status={status} tracking={tracking_number} carrier={shipping_carrier} "
            f"service_code={shipping_service_code} est_max={est_max_naive} delivered_at={delivered_at_naive} "
            f"status_changed={status_changed}"
        )
        return

    cur = cn.cursor()
    if status_changed:
        cur.execute("""
            UPDATE trx.ebay_orders
            SET status = ?, tracking_number = COALESCE(?, tracking_number),
                shipping_carrier = COALESCE(?, shipping_carrier),
                shipping_service_code = COALESCE(?, shipping_service_code),
                estimated_delivery_max_at = COALESCE(?, estimated_delivery_max_at),
                delivered_at = COALESCE(?, delivered_at),
                status_updated_at = ?, status_checked_at = ?
            WHERE order_id = ? AND ebay_id = ?
        """, status, tracking_number, shipping_carrier, shipping_service_code,
            est_max_naive, delivered_at_naive, now, now, order_id, ebay_id)
    else:
        cur.execute("""
            UPDATE trx.ebay_orders
            SET tracking_number = COALESCE(?, tracking_number),
                shipping_carrier = COALESCE(?, shipping_carrier),
                shipping_service_code = COALESCE(?, shipping_service_code),
                estimated_delivery_max_at = COALESCE(?, estimated_delivery_max_at),
                delivered_at = COALESCE(?, delivered_at),
                status_checked_at = ?
            WHERE order_id = ? AND ebay_id = ?
        """, tracking_number, shipping_carrier, shipping_service_code,
            est_max_naive, delivered_at_naive, now, order_id, ebay_id)
    cn.commit()
    log.info(f"[UPDATE] order_id={order_id} ebay_id={ebay_id} status={status} status_changed={status_changed}")


def _get_current_status(cn, order_id: str, ebay_id: str) -> str | None:
    if not _new_columns_available(cn):
        return None
    cur = cn.cursor()
    cur.execute(
        "SELECT status FROM trx.ebay_orders WHERE order_id = ? AND ebay_id = ?",
        order_id, ebay_id,
    )
    row = cur.fetchone()
    return row[0] if row else None


def _get_monitored_orders(cn) -> list[tuple]:
    """daily/backfillの対象注文一覧
    （90日以内の全注文 + 90日超でも未解決ステータスのもの）。
    item_title/buyer/countryは通知本文用にあわせて取得する（既存カラム）。"""
    if _new_columns_available(cn):
        placeholders = ",".join("?" for _ in UNRESOLVED_STATUSES)
        cur = cn.cursor()
        cur.execute(f"""
            SELECT account, order_id, ebay_id, vendor_item_id, order_date, status,
                   item_title, buyer, country, delivered_at, estimated_delivery_max_at
            FROM trx.ebay_orders
            WHERE order_date >= DATEADD(day, -{MONITOR_WINDOW_DAYS}, GETUTCDATE())
               OR status IN ({placeholders})
            ORDER BY order_date DESC
        """, *UNRESOLVED_STATUSES)
    else:
        # 新カラム未追加の環境（検証初期）では90日以内のみを対象にする
        cur = cn.cursor()
        cur.execute(f"""
            SELECT account, order_id, ebay_id, vendor_item_id, order_date, NULL,
                   item_title, buyer, country, NULL, NULL
            FROM trx.ebay_orders
            WHERE order_date >= DATEADD(day, -{MONITOR_WINDOW_DAYS}, GETUTCDATE())
            ORDER BY order_date DESC
        """)
    return cur.fetchall()


def _check_one_order(account: str, order_id: str, ebay_id: str, order_date: datetime,
                      cancellations_by_order: dict, returns_by_order: dict,
                      inquiries_by_item: dict, cases_by_item: dict,
                      payment_disputes_by_order: dict,
                      post_order_ok: bool = True,
                      known_delivered_at: datetime | None = None,
                      known_est_max_at: datetime | None = None) -> dict:
    """1注文(1明細)分のAPI詳細取得 + ステータス判定。
    Fulfillment APIとTrading API(GetOrders)を併用する
    （2026-09-09: 配達済み/配送遅延判定にはTrading APIのActualDeliveryTime/
    EstimatedDeliveryTimeMaxが必須なため追加）。
    戻り値はdict: status, reason_detail, tracking_number, shipping_carrier,
    shipping_service_code, estimated_delivery_max_at, delivered_at

    post_order_ok=False の場合、status="None"（既存statusを維持する指示）を返す。
    tracking_number等（Fulfillment/Trading API由来）はpost_order_okに関係なく
    そのAPI自体が成功していれば取得・返却する（2026-09-12、ユーザー指示）。

    known_delivered_at / known_est_max_at: DB保存済みの配達完了日時・配達予定日。
    Trading APIは注文から約90日を過ぎると注文を返さなくなるため、Trading APIから
    取得できない場合はこれらを判定に使う（情報が減っただけでstatusを後退させない。2026-09-26）。"""
    order_detail = get_order_detail(account, order_id)
    order_detail_ok = order_detail is not None

    # Fulfillment API: shipping_service_code（参考値。真の配送会社名はTrading API側）
    fulfillments = get_shipping_fulfillments(account, order_id)
    shipped_line_item_ids = (
        None if fulfillments is None
        else {li.get("lineItemId") for f in fulfillments for li in f.get("lineItems", []) if li.get("lineItemId")}
    )
    fulfillments = fulfillments or []
    service_codes = [f.get("shippingServiceCode") for f in fulfillments if f.get("shippingServiceCode")]
    shipping_service_code = "; ".join(dict.fromkeys(service_codes)) or None

    # Trading API: 実配送会社名・複数区間の追跡番号・配達完了日時・配達予定日
    trading_by_item = get_order_trading_detail(account, order_id)
    trading_detail = trading_by_item.get(ebay_id, {})
    tracking_pairs = trading_detail.get("tracking_pairs", [])
    if tracking_pairs:
        tracking_number = "; ".join(t for _, t in tracking_pairs)
        shipping_carrier = "; ".join(c for c, _ in tracking_pairs)
    else:
        tracking_number = None
        shipping_carrier = None
    if len(tracking_pairs) > 1:
        log.info(
            f"[複数区間追跡] order_id={order_id} ebay_id={ebay_id} "
            f"tracking_pairs={tracking_pairs} → 順序を保ったまま\"; \"区切りで連結保存"
        )
    actual_delivery_time = trading_detail.get("actual_delivery_time") or _as_utc(known_delivered_at)
    estimated_delivery_max = trading_detail.get("estimated_delivery_max") or _as_utc(known_est_max_at)

    # OC(Orange Connex)の配送状態確認に使う番号: Trading APIのSpeedPAK区間の追跡番号（OC番号）。
    # Trading APIから取得できない古い注文はshipping_fulfillmentの追跡番号を使う
    # （OC APIはOCが管理する番号のみ結果を返すため、他社番号が混ざっても誤判定しない）
    if tracking_pairs:
        oc_candidates = [t for c, t in tracking_pairs if c == "SpeedPAK"]
    else:
        oc_candidates = [f.get("shipmentTrackingNumber") for f in fulfillments if f.get("shipmentTrackingNumber")]

    cancellation = cancellations_by_order.get(order_id)
    return_ = returns_by_order.get(order_id)
    inquiry_open = ebay_id in inquiries_by_item
    case = cases_by_item.get(ebay_id)
    # returnがケースへエスカレーション済みでも、古いケースはcasemanagement/searchに
    # 出てこないため、escalationInfo.caseIdで個別取得する（2026-09-26追加）
    if case is None and return_ is not None and return_.get("status") == "ESCALATED":
        escalated_case_id = (return_.get("escalationInfo") or {}).get("caseId")
        if escalated_case_id:
            case = get_case_detail(account, escalated_case_id)
    payment_dispute_open = order_id in payment_disputes_by_order

    status, reason_detail = determine_status(
        order_detail=order_detail,
        cancellation=cancellation,
        return_=return_,
        inquiry_open=inquiry_open,
        case=case,
        payment_dispute_open=payment_dispute_open,
        order_date=order_date,
        ebay_id=ebay_id,
        actual_delivery_time=actual_delivery_time,
        estimated_delivery_max=estimated_delivery_max,
        post_order_ok=post_order_ok,
        order_detail_ok=order_detail_ok,
        shipped_line_item_ids=shipped_line_item_ids,
    )
    return {
        "status": status,
        "reason_detail": reason_detail,
        "tracking_number": tracking_number,
        "shipping_carrier": shipping_carrier,
        "shipping_service_code": shipping_service_code,
        "estimated_delivery_max_at": estimated_delivery_max,
        "delivered_at": actual_delivery_time,
        "oc_candidates": oc_candidates,
        "shipped_at": _line_shipped_at(order_detail, fulfillments, ebay_id, trading_detail.get("shipped_time")),
    }


def _line_shipped_at(
    order_detail: dict | None, fulfillments: list[dict] | None, ebay_id: str,
    trading_shipped_time: datetime | None = None,
) -> datetime | None:
    """対象明細(ebay_id)の実際の出荷日時。Fulfillment APIのshipping_fulfillmentのうち
    対象明細のlineItemIdを含む記録のshippedDate（複数あれば最新＝みなし配達の判定が控えめになる側）。
    取得できない場合はTrading APIのShippedTime（注文から約90日以内のみ取得可）。
    どちらも無ければNone（出荷日不明。注文日・ship_by_date等で代用しない）。"""
    line_item_id = next(
        (li.get("lineItemId") for li in (order_detail or {}).get("lineItems", []) if li.get("legacyItemId") == ebay_id),
        None,
    )
    dates = []
    if line_item_id:
        for f in fulfillments or []:
            if line_item_id in [li.get("lineItemId") for li in f.get("lineItems", [])]:
                dt = _parse_trading_dt(f.get("shippedDate"))
                if dt is not None:
                    dates.append(dt)
    if dates:
        return max(dates)
    return trading_shipped_time


def _is_deemed_delivered(old_status: str | None, shipped_at: datetime | None, now: datetime) -> bool:
    """みなし配達済みの条件（2026-09-27、ユーザー指示）: 現在のstatusが「出荷済み」で、
    実際の出荷日時が分かり、出荷から DEEMED_DELIVERED_DAYS 日を超えていること。
    出荷済み以外（配送遅延・未着申告・要確認等の問題status、手動設定のstatus）は対象外。"""
    return old_status == "出荷済み" and shipped_at is not None and (now - shipped_at).days > DEEMED_DELIVERED_DAYS


def _as_utc(dt: datetime | None) -> datetime | None:
    """DBのdatetime2(UTC相当のnaive値)をUTC awareにする。"""
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


# --------------------------------------------------
# メイン処理
# --------------------------------------------------

# --------------------------------------------------
# realtimeのキャンセル確認: 429(日次上限)時のアカウント別一時停止
# --------------------------------------------------
# 2026-09-11 実機調査で判明: cancellation/searchの429は「米国太平洋時間の日付変更」で
# リセットされる日次上限で、アカウント単位（App共通ではない）。JSTの固定時刻ではなく
# zoneinfoでPT基準の次回リセット時刻を都度計算する（DST自動対応）。
from zoneinfo import ZoneInfo

_PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
# account -> 再開予定時刻(UTC aware)。エントリが無い/現在時刻がこれを過ぎていれば停止していない。
_cancellation_paused_until: dict[str, datetime] = {}


def _next_pacific_midnight_utc(now_utc: datetime) -> datetime:
    """現在時刻(UTC)から見て、次に到来する米国太平洋時間の日付変更時刻をUTCで返す。"""
    now_pacific = now_utc.astimezone(_PACIFIC_TZ)
    next_midnight_pacific = (now_pacific + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return next_midnight_pacific.astimezone(timezone.utc)


def _search_cancellations_realtime(account: str, date_from: datetime, date_to: datetime) -> tuple[list[dict], bool]:
    """realtimeのキャンセル確認専用。既存のアカウント単位一括取得方式を維持しつつ、
    429(日次上限)を検知して呼び出し元へ伝える。戻り値: (cancellations, is_rate_limited)。
    注文単位のAPI呼び出しには変更しない。"""
    token = get_access_token_new(account)
    if not token:
        return [], False
    headers = _iaf_headers(token)
    params = {
        "creation_date_range_from": date_from.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "creation_date_range_to": date_to.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "limit": 200, "offset": 0,
    }
    try:
        r = requests.get(
            "https://api.ebay.com/post-order/v2/cancellation/search",
            headers=headers, params=params, timeout=30,
        )
    except Exception:
        log.warning(f"[cancel-check] {account} 通信エラー", exc_info=True)
        return [], False
    if r.status_code == 429:
        return [], True
    if r.status_code != 200:
        log.warning(f"[cancel-check] {account}: {r.status_code} {r.text[:300]}")
        return [], False
    try:
        data = r.json()
    except Exception:
        log.warning(f"[cancel-check] {account} JSON解析失敗")
        return [], False
    return data.get("cancellations", []) or [], False


def _check_unshipped_cancellations(cn, account: str, dry_run: bool) -> None:
    """realtime処理の追加ステップ：DB上の未出荷注文について、
    キャンセル申請・キャンセル完了が発生していないかを確認し、
    変化があった場合のみstatusを更新・通知する。
    （spec: 「DB上の未出荷注文について、キャンセル申請・キャンセル完了がないか確認する」）
    2026-09-11: 5分間隔での呼び出しを前提に、429(日次上限)発生時はアカウント単位で
    次回の米国太平洋時間の日付変更まで一時停止する。"""
    now = datetime.now(timezone.utc)

    resume_at = _cancellation_paused_until.get(account)
    if resume_at is not None:
        if now < resume_at:
            return  # 停止中: このアカウントは今回スキップ（ログも出さない＝繰り返し記録しない）
        del _cancellation_paused_until[account]
        log.info(
            f"[cancel-check] {account}: キャンセル確認を再開します"
            f"（前回429による一時停止を解除、resume_at={resume_at.isoformat()}）"
        )

    cur = cn.cursor()
    if _new_columns_available(cn):
        cur.execute("""
            SELECT order_id, ebay_id, vendor_item_id, order_date, status,
                   item_title, buyer, country
            FROM trx.ebay_orders
            WHERE account = ?
              AND (status IS NULL OR status IN ('新規受注'))
              AND order_date >= DATEADD(day, -30, GETUTCDATE())
        """, account)
    else:
        cur.execute("""
            SELECT order_id, ebay_id, vendor_item_id, order_date, NULL,
                   item_title, buyer, country
            FROM trx.ebay_orders
            WHERE account = ?
              AND order_date >= DATEADD(day, -30, GETUTCDATE())
        """, account)
    targets = cur.fetchall()
    if not targets:
        return

    # 5分間隔で実際に呼ばれていることをログで確認できるようにする（成功時は他に出力がないため）
    log.info(f"[cancel-check] {account}: 実行（対象={len(targets)}件）")

    date_from = now - timedelta(days=30)
    cancellations_list, rate_limited = _search_cancellations_realtime(account, date_from, now)
    if rate_limited:
        resume_at = _next_pacific_midnight_utc(now)
        _cancellation_paused_until[account] = resume_at
        log.warning(
            f"[cancel-check] {account}: cancellation/searchが日次上限(429)のため、"
            f"このアカウントのキャンセル確認を一時停止します。"
            f"再開予定(米国太平洋時間の日付変更後): {resume_at.isoformat()}"
        )
        return
    cancellations = {c.get("legacyOrderId"): c for c in cancellations_list}

    for order_id, ebay_id, vendor_item_id, order_date, old_status, item_title, buyer, country in targets:
        cancellation = cancellations.get(order_id)
        if cancellation is None:
            continue  # キャンセル関連の変化なし

        order_detail = get_order_detail(account, order_id)
        status, reason_detail = determine_status(
            order_detail=order_detail, cancellation=cancellation, return_=None,
            inquiry_open=False, case=None, payment_dispute_open=False,
            order_date=order_date, ebay_id=ebay_id,
        )
        status_changed = (status != (old_status or "新規受注"))
        update_order_status(
            cn, order_id=order_id, ebay_id=ebay_id, status=status,
            tracking_number=None, shipping_carrier=None,
            status_changed=status_changed, dry_run=dry_run,
        )
        if status_changed and status in NOTIFY_ON_STATUS:
            notify_status_change(
                account=account, order_id=order_id, ebay_id=ebay_id,
                vendor_item_id=vendor_item_id, old_status=old_status,
                new_status=status, reason_detail=reason_detail,
                item_title=item_title, buyer=buyer, country=country,
            )


def run_realtime(once: bool = False, dry_run: bool = False):
    """realtimeモード：新規受注取得 + 出荷前キャンセル確認。
    TEST_MODE=Trueの間は新規受注に伴う実際の後続処理（メール/LINE/サンキュー/
    Access登録/DB INSERT）を実行せず、ログにのみ記録する。"""
    log.info(
        f"START(realtime): {datetime.now():%Y-%m-%d %H:%M:%S} "
        f"TEST_MODE={TEST_MODE} NEW_ORDER_NOTIFICATION_ENABLED={NEW_ORDER_NOTIFICATION_ENABLED} "
        f"NOTIFICATION_ENABLED={NOTIFICATION_ENABLED} once={once}"
    )

    # 2026-09-11: 新規受注確認(60秒)とキャンセル確認(5分)を独立したタイマーで管理する。
    # None起動時は「まだ一度も実行していない」を表し、起動直後の1回目のループで
    # 必ずキャンセル確認も実行される（spec: 起動直後は両方実行する）。
    CANCEL_CHECK_INTERVAL = timedelta(minutes=5)
    last_cancel_check_at: datetime | None = None

    while True:
        _update_heartbeat(last_loop_start=datetime.now(timezone.utc).isoformat())
        cn = cur = None
        try:
            now = datetime.now(timezone.utc)

            # キャンセル確認は5分間隔（新規受注確認は毎ループ=60秒間隔のまま）
            do_cancel_check = (
                last_cancel_check_at is None
                or now - last_cancel_check_at >= CANCEL_CHECK_INTERVAL
            )

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

                for order in (orders or []):
                    order_id   = order.get("orderId")
                    order_date = order.get("creationDate")
                    buyer      = order.get("buyer", {}).get("username")

                    # 初回APIレスポンスの country / is_ag（メール送信用）
                    initial_country, initial_is_ag = _order_ship_to(order)

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

                        if TEST_MODE:
                            log.info(
                                f"[TEST_MODE] 新規受注検知（実処理はスキップ）: "
                                f"order_id={order_id} ebay_id={ebay_id} sku={vendor_item_id} "
                                f"buyer={buyer} price_usd={price_usd} country={initial_country} "
                                f"→ メール送信/LINE通知/サンキュー/DB INSERT/Access登録は"
                                f"TEST_MODEのため実行しない"
                            )
                            continue

                        # ① メール送信（最優先・即時）
                        # 2026-09-12: 新規受注通知は本番機能のため常時有効
                        # （NEW_ORDER_NOTIFICATION_ENABLED、status変化通知とは独立制御）。
                        # DB INSERT/Access登録/サンキューメッセージは元々別枠で常時実行。
                        if NEW_ORDER_NOTIFICATION_ENABLED:
                            send_new_order_mail(
                                account=account,
                                order_id=order_id,
                                buyer=buyer,
                                vendor_item_id=vendor_item_id,
                                ebay_id=ebay_id,
                                price_usd=float(price_usd),
                                country=initial_country,
                            )
                        else:
                            log.info(
                                f"[通知抑止] NEW_ORDER_NOTIFICATION_ENABLED=Falseのため新規受注メールを"
                                f"送信しません: order_id={order_id} ebay_id={ebay_id}"
                            )

                        # ① LINE通知（追加 / メール送信に続いて即時送信）
                        if NEW_ORDER_NOTIFICATION_ENABLED:
                            send_line_new_order(
                                account=account,
                                vendor_item_id=vendor_item_id,
                                ebay_id=ebay_id,
                                price_usd=float(price_usd),
                                country=initial_country,
                                buyer=buyer,
                            )
                        else:
                            log.info(
                                f"[通知抑止] NEW_ORDER_NOTIFICATION_ENABLED=Falseのため新規受注LINE通知を"
                                f"送信しません: order_id={order_id} ebay_id={ebay_id}"
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
                        _insert_trx_ebay_order(
                            cur, account=account, order_id=order_id, buyer=buyer, ebay_id=ebay_id,
                            vendor_item_id=vendor_item_id, qty=qty, order_date=order_date,
                            price_usd=price_usd, country=country, is_ag=is_ag,
                            item_title=item_title, ship_by_date=ship_by_date,
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

                # ---- realtime追加ステップ：出荷前キャンセル確認（5分間隔） ----
                # spec: 「DB上の未出荷注文について、キャンセル申請・キャンセル完了が
                # ないか確認する」「状態が変化した場合だけstatusを更新して通知する」
                if do_cancel_check:
                    try:
                        _check_unshipped_cancellations(cn, account, dry_run=(dry_run or TEST_MODE))
                    except Exception:
                        log.warning(f"[cancel-check] {account} でエラー", exc_info=True)

            if do_cancel_check:
                last_cancel_check_at = now

            _update_heartbeat(last_success=datetime.now(timezone.utc).isoformat())

        except Exception as e:
            log.error(f"run_realtime()ループでエラー: {e}", exc_info=True)
        finally:
            # Access DB接続障害等、途中で例外が発生した場合でも
            # SQL Serverコネクションは必ず閉じる（コネクションリーク防止）
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            if cn is not None:
                try:
                    cn.close()
                except Exception:
                    pass

        if once:
            log.info("--once指定のため1回で終了します")
            break

        # 正常時・エラー時のいずれも、次サイクルまで60秒待機する
        time.sleep(60)


# --------------------------------------------------
# dailyモード：出荷・配達・返品・ケース・返金状態の確認（1日1回想定）
# --------------------------------------------------
def resolve_speedpak_tracking(pending: list[dict]) -> dict:
    """SpeedPAK経由の注文だけOrange Connexでラストマイル番号へ解決する。
    pendingの各要素のp["result"]["tracking_number"]をin-placeで書き換え、集計をdictで返す。
    daily/backfill共通（2026-09-26: backfillにこの処理が無く、SpeedPAK注文にOC番号が
    そのまま書き込まれていたため、dailyから切り出して共通化）。"""
    # spec: shipping_carrierはSpeedPAKのまま。tracking_numberだけラストマイル番号に置き換える。
    # 解決できない場合はeBay API由来のOC番号をそのままtracking_numberに残す。
    # tracking_numberは複数のTradingAPI ShipmentTrackingDetailsを"; "で連結した
    # 文字列になっている場合がある（1注文に複数の出荷記録が存在するケース）。
    # 全区間がSpeedPAKであれば対象とし、OC番号ごとに個別照会したうえで、
    # 複数候補が残る場合はlastStatus/trace件数から有効な1件だけを判別する。
    # 先頭を無条件に採用することはせず、判別できない場合は更新しない。
    def _is_speedpak_only(shipping_carrier: str | None) -> bool:
        if not shipping_carrier:
            return False
        segments = [s.strip() for s in shipping_carrier.split(";") if s.strip()]
        return bool(segments) and all(seg == "SpeedPAK" for seg in segments)

    oc_targets = [
        p for p in pending
        if _is_speedpak_only(p["result"]["shipping_carrier"]) and p["result"]["tracking_number"]
    ]
    oc_speedpak_count = len(oc_targets)
    oc_resolved_count = 0
    oc_unresolved_count = 0
    oc_api_error_count = 0
    oc_multi_candidate_count = 0
    oc_multi_undetermined_count = 0
    oc_samples: list[tuple[str, str]] = []

    if oc_targets:
        all_oc_numbers: list[str] = []
        for p in oc_targets:
            all_oc_numbers.extend(s.strip() for s in p["result"]["tracking_number"].split(";") if s.strip())
        waybills_map, failed_reason = _fetch_orange_connex_waybills(all_oc_numbers)

        for p in oc_targets:
            oc_list = [s.strip() for s in p["result"]["tracking_number"].split(";") if s.strip()]
            if len(oc_list) == 1:
                oc_no = oc_list[0]
                w = waybills_map.get(oc_no)
                last_mile = w.get("trackingNumber") if w else None
                if last_mile:
                    p["result"]["tracking_number"] = last_mile
                    oc_resolved_count += 1
                    if len(oc_samples) < 10:
                        oc_samples.append((oc_no, last_mile))
                else:
                    oc_unresolved_count += 1
                    reason = failed_reason.get(oc_no, "不明")
                    if "エラー" in reason or "解析失敗" in reason or "success=false" in reason:
                        oc_api_error_count += 1
                    log.info(
                        f"[orange_connex] 未解決のためOC番号のまま保持: order_id={p['order_id']} "
                        f"ebay_id={p['ebay_id']} oc_no={oc_no} reason={reason}"
                    )
            else:
                oc_multi_candidate_count += 1
                last_mile, reason = select_valid_oc_candidate(oc_list, waybills_map)
                if last_mile:
                    p["result"]["tracking_number"] = last_mile
                    oc_resolved_count += 1
                    if len(oc_samples) < 10:
                        oc_samples.append((";".join(oc_list), last_mile))
                    log.info(
                        f"[orange_connex][複数候補から判別] order_id={p['order_id']} "
                        f"ebay_id={p['ebay_id']} 候補={oc_list} 採用={last_mile} 理由={reason}"
                    )
                else:
                    oc_unresolved_count += 1
                    oc_multi_undetermined_count += 1
                    log.warning(
                        f"[orange_connex][複数候補・判別不可のため更新せず保持] order_id={p['order_id']} "
                        f"ebay_id={p['ebay_id']} 候補={oc_list} 理由={reason}"
                    )
        log.info(
            f"[orange_connex] SpeedPAK対象={oc_speedpak_count} "
            f"解決={oc_resolved_count} 未解決={oc_unresolved_count} "
            f"(うちAPIエラー起因={oc_api_error_count}, 複数候補={oc_multi_candidate_count}件"
            f"[うち判別不可={oc_multi_undetermined_count}])"
        )
    else:
        log.info("[orange_connex] SpeedPAK対象の注文なし")
    return {
        "speedpak": oc_speedpak_count, "resolved": oc_resolved_count,
        "unresolved": oc_unresolved_count, "api_error": oc_api_error_count,
        "samples": oc_samples,
    }


def _apply_oc_delivery_status(pending: list[dict]) -> int:
    """SpeedPAK等でOC(Orange Connex)の最新状態がDeliveredの注文を「配達済み」にする（2026-09-26追加、
    ユーザー指示）。Trading APIに配達完了が記録されない/約90日超で取得できない注文の配達を補う。
    対象は判定結果が出荷済み/配送遅延のものだけ。OCの返送中/返送済みは対応する既存statusが
    無いため変更しない（ユーザーへ報告済みの保留事項）。戻り値は配達済みにした件数。"""
    targets = [
        p for p in pending
        if p["result"]["status"] in ("出荷済み", "配送遅延") and p["result"].get("oc_candidates")
    ]
    if not targets:
        return 0
    numbers = sorted({n for p in targets for n in p["result"]["oc_candidates"]})
    waybills_map, _failed = _fetch_orange_connex_waybills(numbers)
    delivered_count = 0
    for p in targets:
        found = [n for n in p["result"]["oc_candidates"] if n in waybills_map]
        if not found:
            continue
        if len(found) == 1:
            waybill = waybills_map[found[0]]
        else:
            last_mile, _reason = select_valid_oc_candidate(found, waybills_map)
            waybill = next(
                (waybills_map[n] for n in found if last_mile and waybills_map[n].get("trackingNumber") == last_mile),
                None,
            )
            if waybill is None:
                continue
        last_status = waybill.get("lastStatus")
        p["result"]["oc_last_status"] = last_status
        if last_status == "Delivered":
            p["result"]["status"] = "配達済み"
            p["result"]["reason_detail"] = f"OC配送状態=Delivered (lastMile={waybill.get('trackingNumber')})"
            delivered_count += 1
    log.info(f"[orange_connex] 配送状態確認 対象={len(targets)} OC配達済み={delivered_count}")
    return delivered_count


def _guard_status_regression(
    row: dict, result: dict, has_post_order_record: bool, keep_manual_delay: bool = True,
) -> None:
    """取得できる情報が減っただけで既存statusを後退させない（2026-09-26追加、ユーザー指示）。
    該当する場合はresult["status"]をNone（＝既存statusを維持）にする。
    - Post-Order由来の既存statusなのに、今回そのPost-Order記録を1件も取得できなかった
      （＝「存在しない」ではなく「取得できなかった」）
    - 配送進捗が後退する判定結果（例: 配達済み→出荷済み、配送遅延→新規受注）
    - 配送遅延→出荷済み（2026-09-27追加）: 配送遅延は人間が手動で設定するstatusのため、
      配達完了・返金等の進展がない限り維持する。keep_manual_delay=Falseは、自動判定廃止前に
      プログラムが設定した既存の配送遅延を一度だけ再判定する移行用（backfillの--recheck-delay）"""
    old_status = row.get("old_status")
    new_status = result["status"]
    if new_status is None or old_status is None or new_status == old_status:
        return
    if keep_manual_delay and old_status == "配送遅延" and new_status == "出荷済み":
        result["reason_detail"] = f"配送遅延は手動設定のstatusのため出荷済みへ戻さず維持（{result['reason_detail']}）"
        result["status"] = None
        return
    if old_status in POST_ORDER_DERIVED_STATUSES and not has_post_order_record:
        log.warning(
            f"[status維持] order_id={row['order_id']} ebay_id={row['ebay_id']}: 既存status={old_status}の根拠となる"
            f"Post-Order記録を今回取得できなかったため維持（判定結果={new_status}）"
        )
        result["reason_detail"] = (
            f"既存status={old_status}の根拠となるPost-Order記録を今回取得できなかったため既存statusを維持"
            f"（判定結果={new_status}: {result['reason_detail']}）"
        )
        result["status"] = None
        return
    if (old_status in _DELIVERY_RANK and new_status in _DELIVERY_RANK
            and _DELIVERY_RANK[new_status] < _DELIVERY_RANK[old_status]):
        log.warning(
            f"[status維持] order_id={row['order_id']} ebay_id={row['ebay_id']}: 配送進捗が後退する判定結果"
            f"({old_status}→{new_status})のため既存statusを維持"
        )
        result["reason_detail"] = (
            f"配送進捗が後退する判定結果（{old_status}→{new_status}）のため既存statusを維持"
            f"（{result['reason_detail']}）"
        )
        result["status"] = None


def evaluate_orders(rows: list[dict], log_prefix: str, keep_manual_delay: bool = True) -> tuple[list[dict], dict]:
    """daily/backfill共通の判定処理（2026-09-26: 両モードから切り出して共通化）。DB更新は行わない。
    rows: account, order_id, ebay_id, vendor_item_id, order_date, old_status, delivered_at, est_max_at
          を持つdict（その他のキーは戻り値にそのまま引き継ぐ）。
    戻り値: (pending, stats)。pendingは各rowに判定結果"result"を加えたdict。
    keep_manual_delay: Falseの場合のみ既存の配送遅延を出荷済みへ変更できる（移行用。_guard_status_regression参照）。

    Post-Order検索の開始日は、アカウントごとに対象注文の最古のorder_dateの30日前とする。
    以前は「今日から120日前」固定で、90日超の未解決statusの注文の返品・ケースが検索に出ず、
    「返品なし」と判定されて要確認→出荷済みへ逆戻りした（2026-09-26 daily本番実行で20件）。

    2026-09-12追加: cancellation/return/inquiry/caseのいずれか1つでもAPI取得に
    失敗した場合、そのアカウントの全注文についてstatus判定そのものをスキップする
    （post_order_ok=False）。2026-09-11に429でこれらのAPIが失敗した際、
    「該当なし」と誤認識して「キャンセル済み」を「返金済み」に誤って書き換えた
    事故の再発防止（ユーザー指示）。payment_disputeは注文単位のAPIのため、
    アカウント単位の成否とその注文自体の成否の両方を満たす場合のみpost_order_ok=True。"""
    now = datetime.now(timezone.utc)
    by_account: dict[str, list[dict]] = {}
    for row in rows:
        by_account.setdefault(row["account"], []).append(row)

    pending: list[dict] = []
    account_post_order_fail_count = 0
    for account, acct_rows in by_account.items():
        date_from = min(_as_utc(r["order_date"]) for r in acct_rows) - timedelta(days=30)
        log.info(f"{log_prefix}[ACCOUNT] {account} 対象={len(acct_rows)}件 Post-Order検索開始日={date_from:%Y-%m-%d}")
        cancellations_list, cancel_ok = search_cancellations(account, date_from, now)
        returns_list, return_ok = search_returns(account, date_from, now)
        inquiries_list, inquiry_ok = search_inquiries(account, date_from, now)
        cases_list, case_ok = search_cases(account, date_from, now)

        account_ok = cancel_ok and return_ok and inquiry_ok and case_ok
        if not account_ok:
            account_post_order_fail_count += 1
            failed_apis = [
                name for name, ok in [
                    ("cancellation/search", cancel_ok), ("return/search", return_ok),
                    ("inquiry/search", inquiry_ok), ("casemanagement/search", case_ok),
                ] if not ok
            ]
            log.warning(
                f"{log_prefix}[ACCOUNT] {account}: Post-Order API取得失敗({', '.join(failed_apis)})のため、"
                f"このアカウントの{len(acct_rows)}件はstatus判定をスキップします"
                f"（既存statusを維持。tracking_number等は取得できた分のみ更新）"
            )

        cancellations = {c.get("legacyOrderId"): c for c in cancellations_list}
        returns_by_order = {r.get("orderId"): r for r in returns_list if r.get("orderId")}
        inquiries_by_item = {}
        inquiry_items_any = set()
        for iq in inquiries_list:
            item_id = str(iq.get("itemId") or "")
            if not item_id:
                continue
            inquiry_items_any.add(item_id)
            # inquiryStatusEnumがCLOSEDのもの（実機観測済み）は解決済みのため未着申告の判定対象外
            if iq.get("inquiryStatusEnum") != "CLOSED":
                inquiries_by_item[item_id] = iq
        # 2026-09-16修正: caseStatusEnumは事前フィルタせず全件を渡す
        # （determine_status側でOPEN/CLOSED/CS_CLOSED/未知の値を明示的に判定する）
        cases_by_item = {}
        for c in cases_list:
            item_id = str(c.get("itemId") or "")
            if item_id:
                cases_by_item[item_id] = c

        for row in acct_rows:
            order_id, ebay_id = row["order_id"], row["ebay_id"]
            payment_disputes, payment_dispute_ok = get_payment_dispute_summary(account, order_id)
            payment_disputes_by_order = {order_id: True} if payment_disputes else {}
            order_post_order_ok = account_ok and payment_dispute_ok
            if account_ok and not payment_dispute_ok:
                log.warning(
                    f"{log_prefix} {order_id} ({account}): payment_dispute_summary取得失敗のため、"
                    f"この注文のみstatus判定をスキップします"
                )

            result = _check_one_order(
                account, order_id, ebay_id, row["order_date"],
                cancellations_by_order=cancellations,
                returns_by_order=returns_by_order,
                inquiries_by_item=inquiries_by_item,
                cases_by_item=cases_by_item,
                payment_disputes_by_order=payment_disputes_by_order,
                post_order_ok=order_post_order_ok,
                known_delivered_at=row.get("delivered_at"),
                known_est_max_at=row.get("est_max_at"),
            )
            has_post_order_record = (
                order_id in cancellations or order_id in returns_by_order
                or ebay_id in inquiry_items_any or ebay_id in cases_by_item or bool(payment_disputes)
            )
            _guard_status_regression(row, result, has_post_order_record, keep_manual_delay)
            pending.append({**row, "result": result})

    if account_post_order_fail_count:
        log.warning(
            f"{log_prefix} Post-Order API取得失敗によりstatus判定をスキップしたアカウント数: "
            f"{account_post_order_fail_count}"
        )

    # SpeedPAK経由の注文はOC番号をラストマイル番号へ解決し、OCの配送状態も判定に使う
    oc_stats = resolve_speedpak_tracking(pending)
    oc_delivered = _apply_oc_delivery_status(pending)

    # 実配達確認なし・出荷から90日超・現在出荷済み → 配達済み（みなし）
    deemed_count = 0
    for p in pending:
        result = p["result"]
        if result["status"] == "出荷済み" and _is_deemed_delivered(p.get("old_status"), result.get("shipped_at"), now):
            result["status"] = "配達済み"
            result["reason_detail"] = (
                f"みなし配達済み: 出荷({result['shipped_at'].isoformat()})から{DEEMED_DELIVERED_DAYS}日超・"
                f"配達完了の記録なし・現在出荷済み"
            )
            deemed_count += 1
    if deemed_count:
        log.info(f"{log_prefix} みなし配達済み(出荷から{DEEMED_DELIVERED_DAYS}日超): {deemed_count}件")
    return pending, {"oc": oc_stats, "oc_delivered": oc_delivered, "deemed_delivered": deemed_count,
                     "post_order_fail_accounts": account_post_order_fail_count}


def run_deemed_delivery(dry_run: bool = True) -> None:
    """現在「出荷済み」の注文のうち、出荷から DEEMED_DELIVERED_DAYS 日を超えたものを「配達済み」にし、
    Access「日常」.eBayステータスへ同期する（2026-09-27追加。既存データの初回整理用）。
    出荷日はFulfillment APIの注文詳細・shipping_fulfillmentから取得する（dailyの判定と同じ_line_shipped_at）。
    返品・ケース等のstatus判定は行わない（出荷済み→配達済みの変更のみ）。"""
    log.info(f"START(deemed-delivery): {datetime.now():%Y-%m-%d %H:%M:%S} dry_run={dry_run}")
    now = datetime.now(timezone.utc)
    cn = get_sql_server_connection()
    try:
        cur = cn.cursor()
        cur.execute("SELECT account, order_id, ebay_id FROM trx.ebay_orders WHERE status = N'出荷済み'")
        rows = cur.fetchall()
        counts = {"target": len(rows), "shipped_known": 0, "within": 0, "over": 0, "unknown": 0, "updated": 0}
        changed_keys = []
        for account, order_id, ebay_id in rows:
            order_detail = get_order_detail(account, order_id)
            fulfillments = get_shipping_fulfillments(account, order_id)
            shipped_at = _line_shipped_at(order_detail, fulfillments, ebay_id)
            if shipped_at is None:
                counts["unknown"] += 1
                log.info(f"[deemed-delivery] 出荷日不明のため対象外: order_id={order_id} ebay_id={ebay_id}")
                continue
            counts["shipped_known"] += 1
            if not _is_deemed_delivered("出荷済み", shipped_at, now):
                counts["within"] += 1
                continue
            counts["over"] += 1
            if dry_run:
                log.info(f"[deemed-delivery][DRY-RUN] 配達済みへ変更予定: order_id={order_id} ebay_id={ebay_id} 出荷={shipped_at.isoformat()}")
                continue
            now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
            cur.execute("""
                UPDATE trx.ebay_orders
                SET status = N'配達済み', status_updated_at = ?, status_checked_at = ?
                WHERE order_id = ? AND ebay_id = ? AND status = N'出荷済み'
            """, now_naive, now_naive, order_id, ebay_id)
            if cur.rowcount == 1:
                cn.commit()
                counts["updated"] += 1
                changed_keys.append((order_id, ebay_id))
                log.info(f"[deemed-delivery] 出荷済み→配達済み: order_id={order_id} ebay_id={ebay_id} 出荷={shipped_at.isoformat()}")
            else:
                cn.rollback()
                log.warning(f"[deemed-delivery] 更新行数が1行ではないため取り消し(rowcount={cur.rowcount}): order_id={order_id} ebay_id={ebay_id}")
        log.info(f"[deemed-delivery][サマリ] dry_run={dry_run} {counts}")
        if changed_keys:
            sync_ebay_status_to_access(cn, keys=changed_keys, dry_run=dry_run)
    finally:
        cn.close()
    log.info("deemed-delivery処理 完了")


def run_daily(dry_run: bool = True):
    log.info(
        f"START(daily): {datetime.now():%Y-%m-%d %H:%M:%S} "
        f"TEST_MODE={TEST_MODE} NOTIFICATION_ENABLED={NOTIFICATION_ENABLED} dry_run={dry_run}"
    )
    cn = get_sql_server_connection()
    pending: list[dict] = []
    try:
        targets = _get_monitored_orders(cn)
        log.info(f"[daily] 監視対象注文数: {len(targets)}")
        rows = [
            {"account": acct, "order_id": order_id, "ebay_id": ebay_id, "vendor_item_id": vendor_item_id,
             "order_date": order_date, "old_status": old_status, "item_title": item_title,
             "buyer": buyer, "country": country, "delivered_at": delivered_at, "est_max_at": est_max_at}
            for (acct, order_id, ebay_id, vendor_item_id, order_date, old_status,
                 item_title, buyer, country, delivered_at, est_max_at) in targets
        ]
        # ---- 第1・第2パス: API取得・status判定・SpeedPAK(OC)解決（DB更新なし） ----
        pending, eval_stats = evaluate_orders(rows, "[daily]")
        oc_stats = eval_stats["oc"]

        # ---- 第3パス: DB更新・通知 ----
        # result["status"] is None は「判定不可（API取得失敗・情報減少等）のため既存statusを維持」を意味する。
        # この場合は既存statusを維持し（skip_status=True）、status_changed判定も通知も行わない。
        db_update_count = 0
        status_skipped_count = 0
        for p in pending:
            result = p["result"]
            status = result["status"]

            if status is None:
                status_skipped_count += 1
                update_order_status(
                    cn, order_id=p["order_id"], ebay_id=p["ebay_id"], status=None,
                    tracking_number=result["tracking_number"], shipping_carrier=result["shipping_carrier"],
                    shipping_service_code=result["shipping_service_code"],
                    estimated_delivery_max_at=result["estimated_delivery_max_at"],
                    delivered_at=result["delivered_at"],
                    status_changed=False, dry_run=dry_run, skip_status=True,
                )
                sync_ebay_tracking_to_access(p["order_id"], p["vendor_item_id"], result["tracking_number"], dry_run=dry_run)
                continue

            # 2026-09-27修正: 以前は旧status=NULLを「新規受注」とみなして比較していたため、
            # NULL→新規受注の注文でstatusが書き込まれずNULLのまま残っていた
            status_changed = (status != p["old_status"])
            update_order_status(
                cn, order_id=p["order_id"], ebay_id=p["ebay_id"], status=status,
                tracking_number=result["tracking_number"], shipping_carrier=result["shipping_carrier"],
                shipping_service_code=result["shipping_service_code"],
                estimated_delivery_max_at=result["estimated_delivery_max_at"],
                delivered_at=result["delivered_at"],
                status_changed=status_changed, dry_run=dry_run,
            )
            sync_ebay_tracking_to_access(p["order_id"], p["vendor_item_id"], result["tracking_number"], dry_run=dry_run)
            db_update_count += 1

            if status_changed and status in NOTIFY_ON_STATUS:
                notify_status_change(
                    account=p["account"], order_id=p["order_id"], ebay_id=p["ebay_id"],
                    vendor_item_id=p["vendor_item_id"], old_status=p["old_status"],
                    new_status=status, reason_detail=result["reason_detail"],
                    item_title=p["item_title"], buyer=p["buyer"], country=p["country"],
                    tracking_number=result["tracking_number"], shipping_carrier=result["shipping_carrier"],
                    estimated_delivery_max_at=result["estimated_delivery_max_at"],
                )

        log.info(
            f"[daily][サマリ] 対象={len(pending)} DB更新予定={db_update_count} "
            f"status判定スキップ(判定不可・既存status維持)={status_skipped_count} "
            f"OrangeConnex対象={oc_stats['speedpak']} ラストマイル解決={oc_stats['resolved']} "
            f"未解決={oc_stats['unresolved']} APIエラー={oc_stats['api_error']} "
            f"サンプル(OC番号->ラストマイル)={oc_stats['samples']}"
        )

        # ---- 最後: Access「日常」.eBayステータスへ同期（対象はこのdailyで確認した注文） ----
        if ACCESS_EBAY_STATUS_SYNC_ENABLED:
            sync_ebay_status_to_access(cn, keys=[(p["order_id"], p["ebay_id"]) for p in pending], dry_run=dry_run)
    finally:
        cn.close()
    log.info("daily処理 完了")


# --------------------------------------------------
# backfillモード：初回補完（過去90日分。通知は絶対に送らない）
# --------------------------------------------------
def import_missing_orders_from_api(
    cn, accounts: list[str], date_from: datetime | None, date_to: datetime | None,
    order_ids: list[str] | None, dry_run: bool,
) -> tuple[list[dict], dict]:
    """eBay APIから注文を取得し、trx.ebay_ordersに無い明細だけを追加する（2026-09-27追加）。
    realtimeは取得範囲を全アカウント共通のMAX(order_date)から決めるため、あるアカウントの取得が
    APIエラーで失敗するとその注文を取りこぼしたまま範囲が進み、daily/backfillは既存行しか
    扱わないため回収できなかった。その取りこぼしをAPIの実データから補完する。
    新規受注の通知・サンキュー送信・Access「日常」の新規登録は行わない（DB補完のみ）。
    order_ids指定時は各アカウントで注文単体取得、それ以外はdate_from〜date_toの作成日で検索。
    戻り値: (追加した明細のrow dict一覧（evaluate_orders用）, 集計)"""
    cur = cn.cursor()
    stats = {"api_orders": 0, "api_lines": 0, "existing": 0, "inserted": 0, "api_errors": 0}
    new_rows: list[dict] = []
    for account in accounts:
        if order_ids:
            orders = []
            for oid in order_ids:
                od = get_order_detail(account, oid)
                if od:
                    orders.append(od)
        else:
            orders = fetch_paid_orders(account, date_from, date_to)
        log.info(f"[backfill-api][ACCOUNT] {account} API取得注文数={len(orders or [])}")
        for order in orders or []:
            stats["api_orders"] += 1
            order_id = order.get("orderId")
            order_date = order.get("creationDate")
            buyer = (order.get("buyer") or {}).get("username")
            country, is_ag = _order_ship_to(order)
            for item in order.get("lineItems", []):
                stats["api_lines"] += 1
                ebay_id = item.get("legacyItemId")
                cur.execute("SELECT 1 FROM trx.ebay_orders WHERE order_id = ? AND ebay_id = ?", order_id, ebay_id)
                if cur.fetchone():
                    stats["existing"] += 1
                    continue
                fields = dict(
                    account=account, order_id=order_id, buyer=buyer, ebay_id=ebay_id,
                    vendor_item_id=item.get("sku"), qty=item.get("quantity"), order_date=order_date,
                    price_usd=Decimal((item.get("lineItemCost") or {}).get("value", "0")),
                    country=country, is_ag=is_ag, item_title=item.get("title"),
                    ship_by_date=(item.get("lineItemFulfillmentInstructions") or {}).get("shipByDate"),
                )
                if dry_run:
                    log.info(f"[backfill-api][DRY-RUN] 追加予定: account={account} order_id={order_id} ebay_id={ebay_id} order_date={order_date}")
                else:
                    _insert_trx_ebay_order(cur, **fields)
                    cn.commit()
                    log.info(f"[backfill-api] trx.ebay_ordersへ追加: account={account} order_id={order_id} ebay_id={ebay_id} order_date={order_date}")
                stats["inserted"] += 1
                new_rows.append({
                    "account": account, "order_id": order_id, "ebay_id": ebay_id,
                    "vendor_item_id": fields["vendor_item_id"],
                    "order_date": _parse_trading_dt(order_date).replace(tzinfo=None),
                    "old_status": None, "delivered_at": None, "est_max_at": None,
                })
    log.info(f"[backfill-api][サマリ] dry_run={dry_run} {stats}")
    return new_rows, stats


def run_backfill(
    dry_run: bool = True, target: str = "recent", sync_to_access: bool = True,
    accounts: list[str] | None = None, order_ids: list[str] | None = None,
    recheck_delay: bool = False,
    api_accounts: list[str] | None = None, api_from: datetime | None = None, api_to: datetime | None = None,
):
    """
    target="orders"（2026-09-26追加）: order_idsで指定した注文（全明細）のみを対象とする。
      90日を超えてdailyの対象外になった注文を、必要なときにdailyと同じevaluate_orders()で再判定する用途。
    target="recent"（既定・従来動作）: order_date >= 過去MONITOR_WINDOW_DAYS日のみ対象。
    target="null_status"（2026-09-15追加）: order_dateに関わらずstatus IS NULLの全注文を対象とする。
      過去注文の一括ステータス補完用。Post-Order検索のdate_fromは対象注文の最古のorder_date基準で
      動的に計算する（固定90日窓だとこのモードの目的である古い注文を取りこぼすため）。
    sync_to_access: Falseの場合、日常.ebay_tracking_numberへの同期を行わない
      （2026-09-15追加。過去ステータス補完のみを目的とする実行で、Access側への
      影響を完全に避けたい場合に使う）。
    target="api"（2026-09-27追加）: eBay APIから注文を取得し、trx.ebay_ordersに無い明細を追加したうえで
      追加分をdailyと同じevaluate_orders()で判定・更新・日常同期する（import_missing_orders_from_api参照）。
      api_accounts省略時はis_excluded=0の全アカウント。order_ids指定時はその注文のみ、
      それ以外はapi_from〜api_to（省略時は検索可能な最大範囲＝過去2年弱〜現在）。
    recheck_delay: Trueの場合、既存の配送遅延も出荷済みへ変更できる（2026-09-27追加。
      自動判定廃止前にプログラムが設定した配送遅延を一度だけ再判定する移行用）。
    accounts: 指定した場合、そのアカウント一覧のみを対象にする（2026-09-15追加。
      全件実行前の少数件検証用。CLIには公開せず、関数呼び出し専用）。
    """
    log.info(f"START(backfill): {datetime.now():%Y-%m-%d %H:%M:%S} dry_run={dry_run} target={target} sync_to_access={sync_to_access}")
    cn = get_sql_server_connection()
    rows_report = []
    try:
        cur = cn.cursor()
        if target == "api":
            now_utc = datetime.now(timezone.utc)
            rows, _api_stats = import_missing_orders_from_api(
                cn, api_accounts or load_accounts(),
                api_from or (now_utc - timedelta(days=729)), api_to or now_utc,
                order_ids, dry_run,
            )
            targets = None
        elif target == "null_status":
            cur.execute("""
                SELECT account, order_id, ebay_id, vendor_item_id, order_date, status,
                       delivered_at, estimated_delivery_max_at
                FROM trx.ebay_orders
                WHERE status IS NULL
                ORDER BY order_date DESC
            """)
        elif target == "delay":
            cur.execute("""
                SELECT account, order_id, ebay_id, vendor_item_id, order_date, status,
                       delivered_at, estimated_delivery_max_at
                FROM trx.ebay_orders
                WHERE status = N'配送遅延'
                ORDER BY order_date DESC
            """)
        elif target == "orders":
            if not order_ids:
                raise ValueError("target=orders には order_ids の指定が必要です")
            placeholders = ",".join("?" for _ in order_ids)
            cur.execute(f"""
                SELECT account, order_id, ebay_id, vendor_item_id, order_date, status,
                       delivered_at, estimated_delivery_max_at
                FROM trx.ebay_orders
                WHERE order_id IN ({placeholders})
                ORDER BY order_date DESC
            """, *order_ids)
        else:
            cur.execute(f"""
                SELECT account, order_id, ebay_id, vendor_item_id, order_date, status,
                       delivered_at, estimated_delivery_max_at
                FROM trx.ebay_orders
                WHERE order_date >= DATEADD(day, -{MONITOR_WINDOW_DAYS}, GETUTCDATE())
                ORDER BY order_date DESC
            """)  # backfillは通知しないためitem_title/buyer/countryの取得は不要
        targets = cur.fetchall() if target != "api" else []
        if accounts:
            targets = [row for row in targets if row[0] in accounts]
            log.info(f"[backfill] accountsフィルタ適用: {accounts}")
        if target != "api":
            log.info(f"[backfill] 対象注文数(target={target}): {len(targets)}")

        if target != "api":
            rows = [
                {"account": acct, "order_id": order_id, "ebay_id": ebay_id, "vendor_item_id": vendor_item_id,
                 "order_date": order_date, "old_status": old_status, "delivered_at": delivered_at, "est_max_at": est_max_at}
                for (acct, order_id, ebay_id, vendor_item_id, order_date, old_status, delivered_at, est_max_at) in targets
            ]
        log.info(f"[backfill] 判定対象の明細数(target={target}): {len(rows)}")
        # daily と共通の判定処理（Post-Order検索期間は対象注文の最古日基準・SpeedPAK(OC)解決を含む）
        pending, _eval_stats = evaluate_orders(rows, "[backfill]", keep_manual_delay=not recheck_delay)

        for p in pending:
            result = p["result"]
            status = result["status"]
            tracking_number = result["tracking_number"]
            shipping_carrier = result["shipping_carrier"]

            # backfillは通知を絶対に送らない（過去分の変化を通知対象にしないため）
            # status is None（判定不可）の場合はstatusを書かない（skip_status）
            update_order_status(
                cn, order_id=p["order_id"], ebay_id=p["ebay_id"], status=status,
                tracking_number=tracking_number, shipping_carrier=shipping_carrier,
                shipping_service_code=result["shipping_service_code"],
                estimated_delivery_max_at=result["estimated_delivery_max_at"],
                delivered_at=result["delivered_at"],
                status_changed=(status is not None), dry_run=dry_run,
                skip_status=(status is None),
            )
            if sync_to_access:
                sync_ebay_tracking_to_access(p["order_id"], p["vendor_item_id"], tracking_number, dry_run=dry_run)
            rows_report.append({
                "account": p["account"], "order_id": p["order_id"], "ebay_id": p["ebay_id"],
                "vendor_item_id": p["vendor_item_id"], "order_date": str(p["order_date"]),
                "status": status, "reason_detail": result["reason_detail"],
                "shipping_service_code": result["shipping_service_code"] or "",
                "estimated_delivery_max_at": str(result["estimated_delivery_max_at"] or ""),
                "delivered_at": str(result["delivered_at"] or ""),
                "tracking_number": tracking_number or "",
                "shipping_carrier": shipping_carrier or "",
            })

        # 最後: Access「日常」.eBayステータスへ同期（dailyと共通の関数）
        if sync_to_access and ACCESS_EBAY_STATUS_SYNC_ENABLED:
            sync_ebay_status_to_access(cn, keys=[(p["order_id"], p["ebay_id"]) for p in pending], dry_run=dry_run)
    finally:
        cn.close()

    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(_BACKFILL_REPORT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "account", "order_id", "ebay_id", "vendor_item_id", "order_date",
                "status", "reason_detail", "tracking_number", "shipping_carrier",
                "shipping_service_code", "estimated_delivery_max_at", "delivered_at",
            ])
            writer.writeheader()
            writer.writerows(rows_report)
        log.info(f"[backfill] レポート出力: {_BACKFILL_REPORT_CSV} ({len(rows_report)}件)")
    except Exception:
        log.warning("[backfill] レポートCSV出力に失敗しました", exc_info=True)

    determined_count = sum(1 for r in rows_report if r["status"] is not None)
    skipped_post_order = sum(
        1 for r in rows_report if r["status"] is None and "Post-Order API取得失敗" in r["reason_detail"]
    )
    skipped_order_detail = sum(
        1 for r in rows_report if r["status"] is None and "Fulfillment API取得失敗" in r["reason_detail"]
    )
    skipped_other = sum(1 for r in rows_report if r["status"] is None) - skipped_post_order - skipped_order_detail
    status_counter: dict[str, int] = {}
    for r in rows_report:
        if r["status"] is not None:
            status_counter[r["status"]] = status_counter.get(r["status"], 0) + 1
    log.info(
        f"[backfill][サマリ] 処理対象={len(rows_report)} 判定成功(DB反映予定)={determined_count} "
        f"判定不可合計={len(rows_report) - determined_count} "
        f"(内訳: Post-Order API取得失敗={skipped_post_order}, "
        f"Fulfillment API取得失敗(古い注文等)={skipped_order_detail}, その他={skipped_other}) "
        f"status別内訳={status_counter}"
    )
    log.info("backfill処理 完了（通知は送信していません）")


def _parse_args():
    p = argparse.ArgumentParser(description="fetch_orders_ebay: 新規受注 + 受注後ステータス継続取得")
    p.add_argument("--mode", choices=["realtime", "daily", "backfill", "status-sync", "deemed-delivery"], default="realtime",
                    help="realtime=新規受注取得+出荷前キャンセル確認（既定・無限ループ）, "
                         "daily=既存注文の配送/返品/ケース/返金状態確認（1回で終了）, "
                         "backfill=過去90日分の初回補完（1回で終了・通知なし）, "
                         "status-sync=trx.ebay_ordersの現在のstatusをAccess日常.eBayステータスへ一括同期"
                         "（eBay APIは呼ばない。--live未指定時はdry-run）, "
                         "deemed-delivery=出荷から90日超の出荷済みを配達済みにする既存データ整理（--live未指定時はdry-run）")
    p.add_argument("--once", action="store_true",
                    help="realtimeモードを無限ループさせず1回だけ実行して終了する（検証用）")
    p.add_argument("--live", action="store_true",
                    help="指定した場合、daily/backfillでtrx.ebay_ordersへの実UPDATEを実行する。"
                         "未指定時は既定でdry-run（予定内容の表示のみ）。")
    p.add_argument("--target", choices=["recent", "null_status", "orders", "delay", "api"], default="recent",
                    help="backfillモード専用（2026-09-15追加）。recent=従来通り過去"
                         "MONITOR_WINDOW_DAYS日分のみ対象（既定）。null_status="
                         "order_dateに関わらずtrx.ebay_orders.status IS NULLの全注文を対象とする"
                         "（過去分の一括ステータス補完用）。orders=--order-idsで指定した注文のみ"
                         "（90日超でdaily対象外の注文の再判定用。2026-09-26追加）。"
                         "delay=status=配送遅延の注文（2026-09-27追加。--recheck-delayと併用する移行用）。")
    p.add_argument("--api-accounts", default="",
                    help="--target api 用。カンマ区切りのアカウント名（省略時はis_excluded=0の全アカウント）")
    p.add_argument("--api-from", default="", help="--target api 用。注文作成日の開始 YYYY-MM-DD（省略時は検索可能な最大範囲）")
    p.add_argument("--api-to", default="", help="--target api 用。注文作成日の終了 YYYY-MM-DD（省略時は現在）")
    p.add_argument("--recheck-delay", action="store_true",
                    help="backfillモード専用（2026-09-27追加）。既存の配送遅延を出荷済みへ変更できるようにする"
                         "（配送遅延の自動判定廃止前にプログラムが設定したものの一度きりの再判定用）。")
    p.add_argument("--order-ids", default="",
                    help="--target orders 用。カンマ区切りのorder_id（例: 12-34567-89012,23-45678-90123）")
    p.add_argument("--no-access-sync", action="store_true",
                    help="backfillモード専用（2026-09-15追加）。指定した場合、"
                         "Access「日常」のebay_tracking_numberへの同期を行わない。")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.mode == "realtime":
        run_realtime(once=args.once, dry_run=not args.live)
    elif args.mode == "daily":
        run_daily(dry_run=not args.live)
    elif args.mode == "backfill":
        run_backfill(
            dry_run=not args.live, target=args.target, sync_to_access=not args.no_access_sync,
            order_ids=[x.strip() for x in args.order_ids.split(",") if x.strip()] or None,
            recheck_delay=args.recheck_delay,
            api_accounts=[x.strip() for x in args.api_accounts.split(",") if x.strip()] or None,
            api_from=datetime.strptime(args.api_from, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.api_from else None,
            api_to=datetime.strptime(args.api_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.api_to else None,
        )
    elif args.mode == "deemed-delivery":
        run_deemed_delivery(dry_run=not args.live)
    elif args.mode == "status-sync":
        log.info(f"START(status-sync): dry_run={not args.live}")
        _cn = get_sql_server_connection()
        try:
            sync_ebay_status_to_access(_cn, keys=None, dry_run=not args.live)
        finally:
            _cn.close()

