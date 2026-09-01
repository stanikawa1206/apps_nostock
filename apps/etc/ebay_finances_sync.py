# ==============================================================================
# apps/etc/ebay_finances_sync.py
#
# 【処理概要】
# eBay Finances API を使って資金の動き（売上・返金・手数料・Payoutなど）を取得し、
# セラーハブの "Transaction report (CSV)" と同じ構造の1フラットテーブル
# trx.ebay_transactions へ UPSERT する。
#
# 【データ取得元】
#   1) GET /sell/finances/v1/transaction  … Order/Refund/手数料などの個々の資金移動
#   2) GET /sell/finances/v1/payout       … Payout本体（銀行振込イベント）
#   3) GET /sell/fulfillment/v1/order/{orderId} … (1)にSKU/タイトルが無い場合の補完取得
#
# 【実行方式】
# 1回実行して終了するバッチ型。数週間に1回程度の実行を想定。
# アカウントごとにDB上の MAX(transaction_date) から差分のみ取得するが、
# Order直後はpayoutId未確定(NULL)のまま記録され、確定まで時間がかかるため、
# 直近 PAYOUT_SETTLE_WINDOW（既定45日）は毎回再取得してUPSERTで上書きする。
# PayoutIDが一度確定した取引データは変更されない前提のため、それより古い期間を
# 通常運用で再走査する必要はない（初回の大量バックフィル時のみ全期間を取得）。
# Windowsタスクスケジューラ等で定期実行する想定。
#
# 【APIのデータ保持期限（重要・実測済み）】
# - Finances API (transaction/payoutとも): 開始日が5年より古いとリクエスト自体が
#   エラーになる（明示的なAPIエラーメッセージあり）。
# - Fulfillment API (SKU/タイトル補完用): 実測で730日（2年）が境界。エラーメッセージは
#   親切でなく "400 Invalid Order Id" になるだけなので気付きにくい。
#   → Finances(5年)より大幅に短いため、700日より古いtransactionはFulfillment API
#     呼び出し自体をスキップし、custom_label/item_titleはNULLのまま確定させる
#     （2年より古いSKU/タイトルの補完は本スクリプトの対象外。別手段で行う運用）。
#
# 【配置場所】
# X:\apps\etc\ebay_finances_sync.py 固定。ログは X:\logs\ 配下に出力する。
# ==============================================================================

import sys
import time
import json
import csv
import argparse
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional, Any

import requests

# ==== 'apps' パッケージをインポートできるようにする ====
# X:\apps\etc\ebay_finances_sync.py から見て parents[2] が X:\ (プロジェクトルート)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import get_sql_server_connection


# --------------------------------------------------
# ロギング設定（ファイルのみ。コンソール依存にしない）
# --------------------------------------------------
_LOG_DIR = _PROJECT_ROOT / "logs"
_LOG_FILE = _LOG_DIR / "ebay_finances_sync.log"


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("ebay_finances_sync")
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
        logger.addHandler(logging.NullHandler())
    return logger


log = _setup_logging()

# ==== エンドポイント ====
TRANSACTION_URL = "https://apiz.ebay.com/sell/finances/v1/transaction"
PAYOUT_URL = "https://apiz.ebay.com/sell/finances/v1/payout"
ORDER_URL_TMPL = "https://api.ebay.com/sell/fulfillment/v1/order/{order_id}"

TRANSACTION_PAGE_LIMIT = 200   # eBay仕様上の上限は1000だが、負荷を抑えて200に設定
PAYOUT_PAGE_LIMIT = 200

# Fulfillment API (/sell/fulfillment/v1/order/{orderId}) の実測データ保持期限。
# Finances API（transaction/payoutとも「5年より古い開始日は不可」という明示エラー）とは別物で、
# Fulfillment APIはエラーメッセージも無く"400 Invalid Order Id"を返すだけなので気付きにくい。
# 2026-08-17時点でage=730日はNG・728日はOKと実測で確定した（1日単位で境界を特定済み）。
# 安全マージンを見て700日を閾値とし、これより古いtransactionはAPI呼び出し自体を
# スキップしてcustom_label/item_titleをNULLのまま確定させる
# （2年より古いデータのSKU/タイトルは別手段で補完する運用のため、ここでは追わない）。
FULFILLMENT_RETENTION_DAYS = 700

# 初回（DBに何もない場合）の取得開始日
DEFAULT_START_DATE = datetime(2024, 4, 1, tzinfo=timezone.utc)

# 差分取得時に遡るバッファ（境界のズレでデータが漏れないように少し重複させる）
OVERLAP_WINDOW = timedelta(minutes=5)

# Order発生直後はpayoutId/transaction_statusが未確定（FUNDS_PROCESSING等）で、
# 実際にPayoutへ組み込まれるまで数日〜数週間かかることがある。
# MAX(transaction_date)だけを差分カーソルにすると、後から確定したpayoutIdを
# 二度と拾えなくなるため、直近N日は毎回再取得してUPSERTで上書きする
# （UPSERTなので重複は発生しない）。
PAYOUT_SETTLE_WINDOW = timedelta(days=45)

# 稀にサブスク費用(Charge/Other fee)などがpayoutに組み込まれるまで
# 45日を超えて未確定のままになるケースが実データで確認されている
# （実測で60〜90日程度のケースあり）。そのため、payout_id未確定の
# レコードがある場合は、その最古のtransaction_dateまで遡って再取得する。
# ただし何らかの理由で恒久的に未確定のレコードによってウィンドウが
# 際限なく広がらないよう、探索対象は直近UNRESOLVED_LOOKBACK_CAP以内に限定する。
UNRESOLVED_LOOKBACK_CAP = timedelta(days=180)

# 本スクリプトだけで対象外にしたいアカウント（Fulfillment API保持期間切れで
# SKU補完がほぼ機能しないなど）。mst.ebay_accounts.is_excluded は
# fetch_orders_ebay.py（受注通知の本番デーモン）とも共有されているため、
# ここでは触らずスクリプトローカルに除外する。
SKIP_ACCOUNTS: set[str] = {"貴文"}


# --------------------------------------------------
# アカウント一覧
# --------------------------------------------------
def load_accounts() -> list[str]:
    cn = get_sql_server_connection()
    try:
        cur = cn.cursor()
        cur.execute("""
            SELECT account
            FROM mst.ebay_accounts
            WHERE is_excluded = 0
            ORDER BY account
        """)
        accounts = [row[0] for row in cur.fetchall()]
    finally:
        cn.close()

    skipped = [a for a in accounts if a in SKIP_ACCOUNTS]
    if skipped:
        log.info(f"SKIP_ACCOUNTS指定によりスキップ: {skipped}")
    return [a for a in accounts if a not in SKIP_ACCOUNTS]


# --------------------------------------------------
# 差分取得の開始日時
# --------------------------------------------------
def get_unresolved_floor(cn, account: str) -> Optional[datetime]:
    """
    payout_id未確定のTRANSACTIONのうち最も古いtransaction_dateを返す。
    UNRESOLVED_LOOKBACK_CAPより古いものは恒久的な異常値の可能性があるため対象外とする。
    """
    cur = cn.cursor()
    cur.execute("""
        SELECT MIN(transaction_date)
        FROM trx.ebay_transactions
        WHERE account = ? AND record_type = 'TRANSACTION'
          AND payout_id IS NULL
          AND transaction_date >= ?
    """, account, datetime.now(timezone.utc) - UNRESOLVED_LOOKBACK_CAP)
    val = cur.fetchone()[0]
    if val is None:
        return None
    if val.tzinfo is None:
        val = val.replace(tzinfo=timezone.utc)
    return val


def get_sync_start(cn, account: str, record_type: str) -> datetime:
    cur = cn.cursor()
    cur.execute("""
        SELECT MAX(transaction_date)
        FROM trx.ebay_transactions
        WHERE account = ? AND record_type = ?
    """, account, record_type)
    val = cur.fetchone()[0]
    if val is None:
        return DEFAULT_START_DATE
    if val.tzinfo is None:
        val = val.replace(tzinfo=timezone.utc)

    rolling_start = datetime.now(timezone.utc) - PAYOUT_SETTLE_WINDOW
    # 「前回からの純粋な差分」「payoutId確定待ちの再走査ウィンドウ(45日)」
    # 「45日を超えて未確定のまま残っているレコード」の3つのうち、
    # 最も過去の日付を開始日にすることで取りこぼしを防ぐ。
    candidates = [val - OVERLAP_WINDOW, rolling_start]
    if record_type == "TRANSACTION":
        unresolved_floor = get_unresolved_floor(cn, account)
        if unresolved_floor is not None:
            candidates.append(unresolved_floor - OVERLAP_WINDOW)
    return min(candidates)


def _fmt_ebay_datetime(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# --------------------------------------------------
# eBay Finances API: /transaction 全件取得（ページネーション）
# --------------------------------------------------
def fetch_transactions(token: str, start: datetime, end: datetime) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    url = TRANSACTION_URL
    params = {
        "filter": f"transactionDate:[{_fmt_ebay_datetime(start)}..{_fmt_ebay_datetime(end)}]",
        "limit": TRANSACTION_PAGE_LIMIT,
    }

    all_transactions: list[dict] = []
    page = 1
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            log.warning("[transaction] レート制限 - 5秒待機してリトライ")
            time.sleep(5)
            continue
        r.raise_for_status()
        data = r.json()

        all_transactions.extend(data.get("transactions", []))
        log.info(f"[transaction] page={page} 累計={len(all_transactions)}件")

        if data.get("next"):
            url = data["next"]
            params = None
            page += 1
            time.sleep(0.3)
        else:
            url = None

    return all_transactions


# --------------------------------------------------
# eBay Finances API: /payout 全件取得（ページネーション）
# --------------------------------------------------
def fetch_payouts(token: str, start: datetime, end: datetime) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    url = PAYOUT_URL
    params = {
        "filter": f"payoutDate:[{_fmt_ebay_datetime(start)}..{_fmt_ebay_datetime(end)}]",
        "limit": PAYOUT_PAGE_LIMIT,
    }

    all_payouts: list[dict] = []
    page = 1
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            log.warning("[payout] レート制限 - 5秒待機してリトライ")
            time.sleep(5)
            continue
        r.raise_for_status()
        data = r.json()

        all_payouts.extend(data.get("payouts", []))
        log.info(f"[payout] page={page} 累計={len(all_payouts)}件")

        if data.get("next"):
            url = data["next"]
            params = None
            page += 1
            time.sleep(0.3)
        else:
            url = None

    return all_payouts


# --------------------------------------------------
# Fulfillment API: SKU / タイトル補完（orderIdごとにキャッシュ）
# --------------------------------------------------
def fetch_order_lineitem_map(token: str, order_id: str) -> dict[str, dict]:
    """
    lineItemId -> {sku, title, legacy_item_id, quantity} のマップを返す。
    取得失敗時は空dict（呼び出し側はSKU/タイトルなしのまま登録を継続する）。
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }
    try:
        r = requests.get(ORDER_URL_TMPL.format(order_id=order_id), headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning(f"[fulfillment] order取得失敗 order_id={order_id} status={r.status_code}")
            return {}
        data = r.json()
    except Exception as e:
        log.warning(f"[fulfillment] order取得例外 order_id={order_id} error={e}")
        return {}

    result: dict[str, dict] = {}
    for li in data.get("lineItems", []):
        line_item_id = li.get("lineItemId")
        if not line_item_id:
            continue
        result[line_item_id] = {
            "sku": li.get("sku"),
            "title": li.get("title"),
            "legacy_item_id": li.get("legacyItemId"),
            "quantity": li.get("quantity"),
        }
    return result


# --------------------------------------------------
# マッピング: Transaction API レスポンス → フラット行
# --------------------------------------------------
_KNOWN_FEE_COLUMNS = {
    "FINAL_VALUE_FEE": "final_value_fee",
    "FINAL_VALUE_FEE_FIXED_PER_ORDER": "fixed_fee",
    "INTERNATIONAL_FEE": "international_fee",
    "AD_FEE": "ad_fee",
}


def _dec(v) -> Optional[Decimal]:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except InvalidOperation:
        return None


def map_transaction_to_row(t: dict, account: str) -> dict:
    order_line_items = t.get("orderLineItems") or []
    first_li = order_line_items[0] if order_line_items else {}

    fee_cols = {col: Decimal("0") for col in _KNOWN_FEE_COLUMNS.values()}
    other_fee = Decimal("0")
    for li in order_line_items:
        for fee in li.get("marketplaceFees", []):
            fee_type = fee.get("feeType", "")
            fee_value = _dec(fee.get("amount", {}).get("value")) or Decimal("0")
            col = _KNOWN_FEE_COLUMNS.get(fee_type)
            if col:
                fee_cols[col] += fee_value
            else:
                other_fee += fee_value

    # eBay Finances APIは amount.value / totalFeeBasisAmount.value を常に絶対値で返し、
    # 入金か出金かは bookingEntry ("CREDIT"=入金 / "DEBIT"=出金) 側にしか出ない。
    # セラーハブのTransaction report CSVはDEBIT側をマイナス表示するため、それに合わせる。
    # 手数料内訳(final_value_fee等)は常に正の金額のまま（ネイティブCSVでも符号反転しない）。
    is_debit = t.get("bookingEntry") == "DEBIT"
    net_amount = abs(_dec((t.get("amount") or {}).get("value")) or Decimal("0"))
    gross_amount = _dec((t.get("totalFeeBasisAmount") or {}).get("value"))
    if is_debit:
        net_amount = -net_amount
        if gross_amount is not None:
            gross_amount = -abs(gross_amount)
    elif gross_amount is not None:
        gross_amount = abs(gross_amount)

    row = {
        "account": account,
        "record_type": "TRANSACTION",
        "record_id": t.get("transactionId"),
        "transaction_date": t.get("transactionDate"),
        "transaction_type": t.get("transactionType"),
        "transaction_status": t.get("transactionStatus"),
        "order_id": t.get("orderId"),
        "payout_id": t.get("payoutId"),
        "buyer_username": (t.get("buyer") or {}).get("username"),
        "ebay_item_id": first_li.get("legacyItemId"),
        "custom_label": first_li.get("sku"),
        "item_title": first_li.get("title"),
        "quantity": first_li.get("quantity"),
        "gross_amount": gross_amount,
        "final_value_fee": fee_cols["final_value_fee"],
        "fixed_fee": fee_cols["fixed_fee"],
        "international_fee": fee_cols["international_fee"],
        "ad_fee": fee_cols["ad_fee"],
        "other_fee_amount": other_fee,
        "total_fee_amount": _dec((t.get("totalFeeAmount") or {}).get("value")),
        "net_amount": net_amount,
        "currency": (t.get("amount") or {}).get("currency"),
        "payout_status_detail": None,
        "bank_reference": None,
        "memo": t.get("transactionMemo"),
        "raw_json": json.dumps(t, ensure_ascii=False),
        # DB保存はしないが、後段の補完処理でlineItemIdを引くために保持しておく
        "_line_item_id": first_li.get("lineItemId"),
    }
    return row


def map_payout_to_row(p: dict, account: str) -> dict:
    return {
        "account": account,
        "record_type": "PAYOUT",
        "record_id": p.get("payoutId"),
        "transaction_date": p.get("payoutDate"),
        "transaction_type": "PAYOUT",
        "transaction_status": p.get("payoutStatus"),
        "order_id": None,
        "payout_id": p.get("payoutId"),
        "buyer_username": None,
        "ebay_item_id": None,
        "custom_label": None,
        "item_title": None,
        "quantity": None,
        "gross_amount": None,
        "final_value_fee": None,
        "fixed_fee": None,
        "international_fee": None,
        "ad_fee": None,
        "other_fee_amount": None,
        "total_fee_amount": None,
        # Payoutは常にeBay残高からの出金のため、Transaction同様マイナスに統一する
        # （Payout APIのamount.valueは絶対値で返る）
        "net_amount": -abs(_dec((p.get("amount") or {}).get("value")) or Decimal("0")),
        "currency": (p.get("amount") or {}).get("currency"),
        "payout_status_detail": p.get("payoutStatus"),
        "bank_reference": p.get("bankReference"),
        "memo": None,
        "raw_json": json.dumps(p, ensure_ascii=False),
        "_line_item_id": None,
    }


# --------------------------------------------------
# SKU / タイトル補完（Fulfillment APIをorderIdごとに1回だけ呼ぶ）
# --------------------------------------------------
def _parse_ebay_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def enrich_with_fulfillment(token: str, rows: list[dict]) -> None:
    """
    rows を破壊的に更新する。custom_label が既に埋まっている行はスキップ。
    transaction_date が FULFILLMENT_RETENTION_DAYS より古い行は、Fulfillment APIを
    呼んでも保持期限切れで確実に失敗する（実測で確認済み）ため、最初から呼ばずに
    custom_label/item_title 等をNULLのまま確定させる。
    """
    order_cache: dict[str, dict[str, dict]] = {}
    retention_floor = datetime.now(timezone.utc) - timedelta(days=FULFILLMENT_RETENTION_DAYS)

    for row in rows:
        if row["record_type"] != "TRANSACTION":
            continue
        if row.get("custom_label"):
            continue
        order_id = row.get("order_id")
        line_item_id = row.get("_line_item_id")
        if not order_id or not line_item_id:
            continue

        tx_date = _parse_ebay_datetime(row.get("transaction_date"))
        if tx_date and tx_date < retention_floor:
            continue  # 保持期限切れ想定 - APIを呼ばずNULLのまま

        if order_id not in order_cache:
            order_cache[order_id] = fetch_order_lineitem_map(token, order_id)
            time.sleep(0.2)  # Fulfillment API連打を避ける

        li_info = order_cache[order_id].get(line_item_id)
        if li_info:
            row["custom_label"] = li_info.get("sku")
            row["item_title"] = li_info.get("title")
            row["ebay_item_id"] = row["ebay_item_id"] or li_info.get("legacy_item_id")
            row["quantity"] = row["quantity"] or li_info.get("quantity")


# --------------------------------------------------
# UPSERT
# --------------------------------------------------
_COLUMNS = [
    "account", "record_type", "record_id",
    "transaction_date", "transaction_type", "transaction_status",
    "order_id", "payout_id", "buyer_username",
    "ebay_item_id", "custom_label", "item_title", "quantity",
    "gross_amount", "final_value_fee", "fixed_fee", "international_fee",
    "ad_fee", "other_fee_amount", "total_fee_amount",
    "net_amount", "currency",
    "payout_status_detail", "bank_reference", "memo", "raw_json",
]

_UPDATE_COLUMNS = [c for c in _COLUMNS if c not in ("account", "record_type", "record_id")]

# Fulfillment APIでの補完項目。再取得時にAPI呼び出しが失敗してNULLになっても、
# 既存の正しい値を消さないよう COALESCE で「新値があれば採用、無ければ現状維持」にする。
_COALESCE_ON_UPDATE = {"ebay_item_id", "custom_label", "item_title", "quantity"}


def upsert_row(cur, row: dict) -> None:
    values = [row.get(c) for c in _COLUMNS]

    set_clause = ", ".join(
        f"{c} = COALESCE(?, {c})" if c in _COALESCE_ON_UPDATE else f"{c} = ?"
        for c in _UPDATE_COLUMNS
    )
    set_clause += ", updated_at = SYSUTCDATETIME()"
    update_values = [row.get(c) for c in _UPDATE_COLUMNS]

    cur.execute(
        f"""
        UPDATE trx.ebay_transactions
        SET {set_clause}
        WHERE account = ? AND record_type = ? AND record_id = ?
        """,
        *update_values, row["account"], row["record_type"], row["record_id"],
    )

    if cur.rowcount == 0:
        placeholders = ", ".join("?" for _ in _COLUMNS)
        cur.execute(
            f"""
            INSERT INTO trx.ebay_transactions ({', '.join(_COLUMNS)})
            VALUES ({placeholders})
            """,
            *values,
        )


# --------------------------------------------------
# 既知のSKU/タイトル補完（再取得ウィンドウ内のFulfillment API呼び出しを削減）
# --------------------------------------------------
def get_known_enrichment(cn, account: str, record_ids: list[str]) -> dict[str, dict]:
    """
    DBに既にSKU等が入っているrecord_idについて、値を引いておく。
    ローリングウィンドウでの再取得のたびにFulfillment APIを叩き直す無駄と、
    一時的な失敗でNULL上書きしてしまうリスクを避けるため。
    """
    if not record_ids:
        return {}
    cur = cn.cursor()
    placeholders = ", ".join("?" for _ in record_ids)
    cur.execute(
        f"""
        SELECT record_id, custom_label, item_title, ebay_item_id, quantity
        FROM trx.ebay_transactions
        WHERE account = ? AND record_type = 'TRANSACTION'
          AND custom_label IS NOT NULL
          AND record_id IN ({placeholders})
        """,
        account, *record_ids,
    )
    return {
        row[0]: {
            "custom_label": row[1], "item_title": row[2],
            "ebay_item_id": row[3], "quantity": row[4],
        }
        for row in cur.fetchall()
    }


# --------------------------------------------------
# アカウント単位の同期処理
# --------------------------------------------------
def sync_account(cn, account: str) -> None:
    token = get_access_token_new(account)
    if not token:
        log.error(f"[{account}] アクセストークン取得失敗 - スキップ")
        return

    now = datetime.now(timezone.utc)

    tx_start = get_sync_start(cn, account, "TRANSACTION")
    payout_start = get_sync_start(cn, account, "PAYOUT")

    log.info(f"[{account}] transaction同期範囲: {tx_start} ~ {now}")
    transactions = fetch_transactions(token, tx_start, now)

    log.info(f"[{account}] payout同期範囲: {payout_start} ~ {now}")
    payouts = fetch_payouts(token, payout_start, now)

    rows = [map_transaction_to_row(t, account) for t in transactions]
    rows += [map_payout_to_row(p, account) for p in payouts]

    if not rows:
        log.info(f"[{account}] 新規データなし")
        return

    # 既に補完済みのrecord_idはDBの値を先に埋め、enrich_with_fulfillment側で
    # 「custom_labelが既にあればスキップ」される（=Fulfillment API再呼び出し不要）
    tx_record_ids = [r["record_id"] for r in rows if r["record_type"] == "TRANSACTION"]
    known = get_known_enrichment(cn, account, tx_record_ids)
    for row in rows:
        if row["record_type"] != "TRANSACTION":
            continue
        k = known.get(row["record_id"])
        if k:
            row["custom_label"] = row.get("custom_label") or k["custom_label"]
            row["item_title"] = row.get("item_title") or k["item_title"]
            row["ebay_item_id"] = row.get("ebay_item_id") or k["ebay_item_id"]
            row["quantity"] = row.get("quantity") or k["quantity"]

    enrich_with_fulfillment(token, rows)

    cur = cn.cursor()
    for row in rows:
        row.pop("_line_item_id", None)
        upsert_row(cur, row)
    cn.commit()

    log.info(
        f"[{account}] 登録完了: transaction={len(transactions)}件 payout={len(payouts)}件"
    )


# --------------------------------------------------
# CSV検証モード（SQL Serverに書き込まず、取得結果をCSVで確認する）
# --------------------------------------------------
_CSV_OUTPUT_DIR = _PROJECT_ROOT / "apps" / "etc" / "output"


def sync_account_to_csv(
    account: str, start: datetime, end: datetime, out_dir: Path, include_raw: bool = False
) -> Path:
    token = get_access_token_new(account)
    if not token:
        raise RuntimeError("アクセストークン取得失敗")

    print(f"[{account}] transaction取得中... ({start:%Y-%m-%d} ~ {end:%Y-%m-%d})")
    transactions = fetch_transactions(token, start, end)
    print(f"[{account}] transaction {len(transactions)}件")

    print(f"[{account}] payout取得中... ({start:%Y-%m-%d} ~ {end:%Y-%m-%d})")
    payouts = fetch_payouts(token, start, end)
    print(f"[{account}] payout {len(payouts)}件")

    rows = [map_transaction_to_row(t, account) for t in transactions]
    rows += [map_payout_to_row(p, account) for p in payouts]

    if rows:
        print(f"[{account}] Fulfillment APIでSKU/タイトル補完中...")
        enrich_with_fulfillment(token, rows)

    for row in rows:
        row.pop("_line_item_id", None)

    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = out_dir / f"ebay_transactions_{account}_{timestamp}.csv"

    columns = list(_COLUMNS) if include_raw else [c for c in _COLUMNS if c != "raw_json"]

    with open(filepath, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row.get(c, "") for c in columns])

    return filepath


def run_csv(args: argparse.Namespace) -> None:
    accounts = [args.account] if args.account else load_accounts()

    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    elif args.days:
        start = datetime.now(timezone.utc) - timedelta(days=args.days)
    else:
        start = DEFAULT_START_DATE

    end = datetime.now(timezone.utc)

    for account in accounts:
        try:
            path = sync_account_to_csv(account, start, end, _CSV_OUTPUT_DIR, include_raw=args.raw)
            print(f"[{account}] CSV出力完了 -> {path}")
        except Exception as e:
            log.error(f"[{account}] CSV出力中にエラー: {e}", exc_info=True)
            print(f"[{account}] エラー: {e}")


# --------------------------------------------------
# メイン処理（1回実行して終了）
# --------------------------------------------------
def run(account: Optional[str] = None) -> None:
    log.info(f"START: {datetime.now():%Y-%m-%d %H:%M:%S}")

    accounts = [account] if account else load_accounts()
    log.info(f"対象アカウント数: {len(accounts)}")

    cn = get_sql_server_connection()
    try:
        for account in accounts:
            try:
                sync_account(cn, account)
            except Exception as e:
                log.error(f"[{account}] 同期中にエラー: {e}", exc_info=True)
                cn.rollback()
    finally:
        cn.close()

    log.info("DONE")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="eBay Finances データ取得（DB同期モード / CSV検証モード）"
    )
    parser.add_argument(
        "--mode", choices=["db", "csv"], default="db",
        help="db: trx.ebay_transactions へUPSERT（既定）/ csv: SQL Serverに書き込まずCSV出力のみ",
    )
    parser.add_argument("--account", help="対象アカウントを1件だけ指定（未指定なら全アカウント）")
    parser.add_argument("--start", help="取得開始日 YYYY-MM-DD（csvモードのみ有効。未指定なら2024-04-01）")
    parser.add_argument("--days", type=int, help="--start の代わりに直近N日を指定（csvモードのみ有効）")
    parser.add_argument("--raw", action="store_true", help="CSVにraw_json列（API生レスポンス）を含める")
    args = parser.parse_args()

    if args.mode == "csv":
        run_csv(args)
    else:
        run(args.account)


if __name__ == "__main__":
    main()
