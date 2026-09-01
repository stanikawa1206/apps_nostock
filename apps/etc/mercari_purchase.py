import sys
sys.stdout.reconfigure(encoding="utf-8")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import json
import re
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from apps.common.utils import get_sql_server_connection
from apps.common.vendor_purchase_common import (
    ACCESS_TABLE,
    ARRIVED_STATUS,
    ARRIVED_STATUS_BY_SALES_CHANNEL,
    ensure_daily_record,
    fetch_japanpost_tracking,  # noqa: F401  (共通処理の一部として再エクスポート)
    fetch_yamato_tracking,  # noqa: F401
    get_access_connection,
    sync_carrier_tracking_to_daily,
    update_daily_tracking_info,
    write_ebay_status_if_advancing,
)

CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PROFILE_DIR = r"D:\apps_nostock\selenium_profile"
DEBUG_PORT = 9223
LAUNCH_TIMEOUT_SEC = 30

TARGET_URL = "https://jp.mercari.com/mypage/purchases"

CURRENT_URL_RETRY_COUNT = 5
CURRENT_URL_RETRY_INTERVAL_SEC = 1

# trx.vendor_purchase / trx.vendor_message / trx.vendor_purchase_unregistered の
# vendor_name。既存システムで使われている表記に合わせる。
VENDOR_NAME = "メルカリ"

# status が以下の場合は人手入力とみなして上書きしない
STATUS_NO_OVERWRITE = ("GA鑑定待ち", "出荷済み", "◎有在庫")

SQL_UPSERT_VENDOR_PURCHASE = """
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

SQL_UPSERT_VENDOR_MESSAGE_BY_ID = """
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


# ============================================================
# Chrome起動・タブ管理
# ============================================================
def _debugger_alive(port: int) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


def ensure_chrome_debugger(port: int = DEBUG_PORT, profile_dir: str = PROFILE_DIR,
                            timeout: int = LAUNCH_TIMEOUT_SEC) -> None:
    """デバッグポートで応答するChromeがなければ起動し、応答するまで待つ"""
    if _debugger_alive(port):
        print(f"OK: 起動済みのChrome(ポート{port})を利用します")
        return

    print(f"Chromeをリモートデバッグモードで起動します（ポート{port}）...")
    subprocess.Popen([
        CHROME_EXE,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
    ])

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _debugger_alive(port):
            print("OK: Chrome起動完了")
            return
        time.sleep(0.5)

    raise RuntimeError(f"Chromeの起動確認がタイムアウトしました（{timeout}秒）")


def _get_current_url_with_retry(driver, retries: int = CURRENT_URL_RETRY_COUNT,
                                 interval: float = CURRENT_URL_RETRY_INTERVAL_SEC) -> str:
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


def _close_stale_tabs(driver, keep_handle):
    """keep_handle以外で、前回実行の結果タブ（TARGET_URL）や壊れたタブを閉じる"""
    for handle in driver.window_handles:
        if handle == keep_handle:
            continue
        try:
            driver.switch_to.window(handle)
            is_stale = driver.current_url.startswith(TARGET_URL)
        except Exception:
            # current_urlの取得自体に失敗するタブは使い物にならないので閉じる対象とみなす
            is_stale = True
        if is_stale:
            try:
                driver.close()
            except Exception:
                pass
    driver.switch_to.window(keep_handle)


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
        return []

    _expand_all_transactions(driver)
    return _current_urls()


# ============================================================
# Access（日常テーブル）への同期
# ============================================================
def sync_arrival_status_to_access(sql_conn):
    """
    trx.vendor_purchase(vendor_name=VENDOR_NAME) の status / 到着日を、
    日常テーブルの eBayステータス / 到着日 へ 注文ID(=vendor_item_id) をキーに反映する。
    到着日は「日常.到着日が現在NULLのときだけ」arrival_datetime（メルカリのステータスから
    到着を検知した日）で埋める。既に値がある場合は上書きしない
    ——より正確な到着日は sync_carrier_tracking_to_daily() がヤマト運輸／日本郵便の
    追跡結果から直接更新するため、ここで古い検知日に巻き戻さないようにするのが目的。
    このため main() では本関数を sync_carrier_tracking_to_daily() の後に呼ぶこと。
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
        """, VENDOR_NAME)
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
    trx.vendor_purchase(vendor_name=VENDOR_NAME) のうち、日常テーブルに対応する
    注文IDのレコードが存在しないものを trx.vendor_purchase_unregistered に記録する。
    （逆に日常にはあるがメルカリの取引中に無いケースは、日常側がカード履歴と突合して
    いずれ判明するため対象外）
    日常に登録されて解消されたものはリストから自動的に外す。
    """
    with sql_conn.cursor() as cur:
        cur.execute("SELECT vendor_item_id FROM trx.vendor_purchase WHERE vendor_name = ?", VENDOR_NAME)
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
        cur.execute("SELECT vendor_item_id FROM trx.vendor_purchase_unregistered WHERE vendor_name = ?", VENDOR_NAME)
        tracked = {row[0] for row in cur.fetchall()}

        newly_missing = missing - tracked
        resolved = tracked - missing

        for vendor_item_id in newly_missing:
            cur.execute(
                "INSERT INTO trx.vendor_purchase_unregistered (vendor_name, vendor_item_id, detected_at) VALUES (?, ?, GETDATE())",
                VENDOR_NAME, vendor_item_id
            )
        for vendor_item_id in resolved:
            cur.execute(
                "DELETE FROM trx.vendor_purchase_unregistered WHERE vendor_name = ? AND vendor_item_id = ?",
                VENDOR_NAME, vendor_item_id
            )
    sql_conn.commit()

    print(f"日常未登録チェック: 現在{len(missing)}件（新規{len(newly_missing)}件, 解消{len(resolved)}件）")


# ============================================================
# 取引ページのスクレイピング
# ============================================================
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


def get_raw_status(driver, retries: int = GET_RAW_STATUS_RETRY_COUNT,
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
SEND_MESSAGE_API_URL_SUBSTR = "transaction_messages/post"

JST = timezone(timedelta(hours=9))


def _get_ws_debugger_url(port: int, url_substr: str) -> str:
    tabs = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list", timeout=10).read())
    for t in tabs:
        if t.get("type") == "page" and url_substr in t.get("url", ""):
            return t["webSocketDebuggerUrl"]
    raise RuntimeError(f"対象タブが見つかりません（url_substr={url_substr!r}）")


def _capture_mercari_api_responses(driver, order_id: str, port: int = DEBUG_PORT, timeout_sec: float = 15) -> dict:
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


def get_messages(driver, order_id: str):
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


# ============================================================
# メッセージ送信（/messages画面からの実送信）
# ============================================================
# 本文入力欄・送信ボタンは実機DOM調査で特定済み（推測ではない）。
#   本文入力欄: [data-testid="transaction:chat-textarea"] 配下の textarea[name="chat"]
#   送信ボタン: [data-partner-id="send-chat"] 配下の button[type="submit"]（文言「取引メッセージを送る」）
# スタンプ機能（[data-testid="stamp-popup-trigger"]）・定型文チップ
# （[data-testid="message-template-chip"]、例:「購入後のあいさつをする」）とは
# data-testid/data-partner-idが完全に別で、本文入力欄・送信ボタンには一切触れない。
CHAT_TEXTAREA_SELECTOR = '[data-testid="transaction:chat-textarea"] textarea[name="chat"]'
CHAT_SEND_BUTTON_SELECTOR = '[data-partner-id="send-chat"] button[type="submit"]'

SEND_RESPONSE_WAIT_SEC = 10.0


def send_chat_message(driver, order_id: str, expected_last_message_id, reply_text: str) -> dict:
    """
    メルカリの取引ページへ実際にメッセージを送信する（誤送信防止のため必ずこの手順で行う）。

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
      2. /messages画面表示時点の最新message_id(expected_last_message_id)と比較し、
         新しいメッセージが増えていないか確認する（増えていれば送信せず中止）
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

    current_messages = get_messages(driver, order_id)
    current_last_id = current_messages[-1]["message_id"] if current_messages else None
    if current_last_id != expected_last_message_id:
        # 送信は中止するが、ここで既に取得できているget_messages()の結果
        # （通常scrapeと全く同じ形式・Mercari APIの正規データ）を呼び出し元へ渡す。
        # 呼び出し元(messages_blueprint.py)がこれを使ってtrx.vendor_messageへ保存し、
        # 画面を最新化する（わざわざ再度APIを呼び直したり通常scrapeを起動したりしない）。
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

    ws_url = _get_ws_debugger_url(DEBUG_PORT, f"transaction/{order_id}")
    ws = _ws_client.create_connection(ws_url, timeout=SEND_RESPONSE_WAIT_SEC, suppress_origin=True)
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

        deadline = time.time() + SEND_RESPONSE_WAIT_SEC
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
                if req.get("method") == "POST" and SEND_MESSAGE_API_URL_SUBSTR in req.get("url", ""):
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


TRACKING_NUMBER_RE = re.compile(r"\d{10,14}")

# 送り状番号の直下に表示される文言で配送会社を判定する（番号の桁数・書式からは判定しない）
TRACKING_CARRIER_MARKERS = (
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


def get_tracking_info(driver):
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

        m = TRACKING_NUMBER_RE.search(window)
        if not m:
            return None, None
        tracking_number = m.group(0)

        for marker, carrier in TRACKING_CARRIER_MARKERS:
            if marker in window:
                return tracking_number, carrier
        return tracking_number, None

    idx = body_text.find("送り状番号")
    if idx == -1:
        return None, None

    window = body_text[idx: idx + 200]
    m = TRACKING_NUMBER_RE.search(window)
    if not m:
        return None, None
    tracking_number = m.group(0)

    for marker, carrier in TRACKING_CARRIER_MARKERS:
        if marker in window:
            return tracking_number, carrier

    return tracking_number, None


# ============================================================
# メイン
# ============================================================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--item-ids", nargs="*", default=None, help="注文ID(取引URL末尾)で対象を絞り込む（テスト用）")
    args = parser.parse_args()
    wanted_ids = set(args.item_ids) if args.item_ids else None

    ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{DEBUG_PORT}"

    driver = webdriver.Chrome(options=options)
    conn = get_sql_server_connection()
    access_conn = get_access_connection()

    try:
        # 既存タブを使い回すと、ログインのクロスオリジンリダイレクト
        # （mercari.com → login.mercari.com → OAuthコールバック）でレンダラープロセスが
        # 切り替わり、ChromeDriverのexecution contextが永久に迷子になることがあるため、
        # チェック専用の新規タブを開いて直接遷移する。
        # 全タブを閉じきってしまうとChromeごと終了するので、新規タブを先に作ってから
        # 前回実行の残骸タブを閉じる順序にしている。
        driver.switch_to.new_window('tab')
        check_tab = driver.current_window_handle
        _close_stale_tabs(driver, keep_handle=check_tab)

        driver.get(TARGET_URL)
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
                time.sleep(4)

                vendor_item_id   = get_vendor_item_id(url)
                raw_status       = get_raw_status(driver)
                item_name        = get_item_name(driver)
                purchase_datetime, purchase_price = get_purchase_info(driver)
                messages         = get_messages(driver, vendor_item_id)
                status           = determine_status(raw_status, messages)
                tracking_number, carrier = get_tracking_info(driver)

                with conn.cursor() as cur:
                    cur.execute(
                        SQL_UPSERT_VENDOR_PURCHASE,
                        (VENDOR_NAME, vendor_item_id, purchase_datetime, purchase_price, status, item_name)
                    )
                    for msg in messages:
                        cur.execute(
                            SQL_UPSERT_VENDOR_MESSAGE_BY_ID,
                            (
                                VENDOR_NAME,
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
                    access_conn, VENDOR_NAME, vendor_item_id,
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
        # options.debugger_addressで常駐Chromeへ外部接続しているだけなので、
        # driver.quit()しても常駐Chrome本体・既存タブは終了しない（実機確認済み）。
        # 今回のSeleniumセッション（chromedriver.exeプロセス）だけを終了し、
        # 毎回実行するたびにchromedriver.exeが残留し続けるのを防ぐ。
        driver.quit()


if __name__ == "__main__":
    main()
