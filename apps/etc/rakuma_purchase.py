"""
ラクマ（fril.jp）の購入管理スクリプト。

対象は「取引中」の取引のみ（購入済み＝完了済みの過去取引は対象外）。
https://fril.jp/buy は「取引中」「購入済」の両タブの中身が同一DOM内に存在し、
表示/非表示（CSSのdisplay切替）で出し分けられている（実機確認済み）。そのため
リンクを単純に全件取得すると購入済み分まで混ざるので、is_displayed()で
実際に画面表示されているものだけに絞り込む。

Access「日常」への反映・ヤマト運輸/日本郵便の追跡・trx.vendor_messageへの保存は
apps/common/vendor_purchase_common.py の共通処理を利用する。
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import re
import subprocess
import time
import urllib.error
import urllib.request

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from apps.common.utils import get_sql_server_connection
from apps.common.vendor_purchase_common import (
    get_access_connection,
    save_vendor_messages,
    sync_carrier_tracking_to_daily,
    update_daily_purchase_status,
    update_daily_tracking_info,
)

VENDOR_NAME = "ラクマ"

CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PROFILE_DIR = r"D:\apps_nostock\selenium_profile"
DEBUG_PORT = 9223
LAUNCH_TIMEOUT_SEC = 30

BUY_LIST_URL = "https://fril.jp/buy"

TRACKING_NUMBER_RE = re.compile(r"\d{10,14}")
ORDER_ID_RE = re.compile(r"item_id=([0-9a-f]{32})")

# 送り状番号の直後に表示される文言で配送会社を判定する（番号の書式からは判定しない）
TRACKING_CARRIER_MARKERS = (
    ("ヤマト運輸のサイトへ移動します", "ヤマト"),
    ("日本郵便のサイトへ移動します", "日本郵便"),
)

# 実機確認済みの文言のみで判定する。未確認の文言は追加しない。
ARRIVED_MARKER = "配達が完了しました"
# 発送前ステータスは、ステータス表示専用の要素(.status-title、h5)の文言で判定する
# （2026-08-29実機確認済み）。旧文言「出品者の発送をお待ちください」は実際のページと
# 一致せず、未発送の取引を発送済みと誤判定する不具合の原因になっていたため修正。
BEFORE_SHIP_MARKER = "商品発送までしばらくお待ちください"


# ============================================================
# Chrome起動・タブ管理（mercari_purchase.pyと同じ仕組み）
# ============================================================
def _debugger_alive(port: int) -> bool:
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


def ensure_chrome_debugger(port: int = DEBUG_PORT, profile_dir: str = PROFILE_DIR,
                            timeout: int = LAUNCH_TIMEOUT_SEC) -> None:
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


# ============================================================
# 取引一覧
# ============================================================
def collect_in_progress_transaction_urls(driver, retries: int = 6, interval: float = 2.0):
    """
    「取引中」タブに実際に表示されている取引URLだけを返す。
    購入済み(完了)分もDOM上には存在するため、is_displayed()で可視要素のみに絞る。
    """
    driver.get(BUY_LIST_URL)

    for attempt in range(1, retries + 1):
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='transaction?item_id=']")
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
            return visible_urls

        time.sleep(interval)

    return []


def get_order_id(url: str):
    m = ORDER_ID_RE.search(url)
    return m.group(1) if m else None


# ============================================================
# 個別取引ページ
# ============================================================
def get_raw_status(driver) -> str:
    """
    配送状況を取得する。「発送済みであることを確認できた場合だけ発送済みとする」を
    原則とし、既知のいずれの文言にも一致しない場合は自動返信を誤らせないよう
    例外を送出する（mercari_purchase.pyのget_raw_status()と同じ方針）。

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


def get_tracking_info(driver):
    """
    「お問い合わせ伝票番号」の値と、続く「※◯◯のサイトへ移動します」の文言から
    送り状番号・配送会社を取得する。見つからない場合は (None, None)。
    """
    body_text = driver.find_element(By.TAG_NAME, "body").text
    idx = body_text.find("伝票番号")
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


def get_seller_name(driver):
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


def get_messages(driver, self_name: str):
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


# ============================================================
# メッセージ送信（/messages画面からの実送信）
# ============================================================
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
CHAT_SEND_BUTTON_TEXT = "取引メッセージを送る"
SEND_MESSAGE_API_URL_SUBSTR = "/api/order/comment/add"

SEND_RESPONSE_WAIT_SEC = 15.0


def send_chat_message(driver, order_id: str, expected_last_message_no, reply_text: str) -> dict:
    """
    ラクマの取引ページへ実際にメッセージを送信する（誤送信防止のため必ずこの手順で行う）。

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
      送信成功後にget_messages()を再実行し、送信分を含む最新の全件をそのまま
      save_vendor_messages()へ渡せば、通常scrapeと全く同じ経路で重複なく保存できる。

    手順:
      1. 取引ページを開き、現在のメッセージ件数をget_messages()で取得する
      2. /messages画面表示時点の件数(expected_last_message_no)と比較し、新しいメッセージが
         増えていないか確認する（増えていれば送信せず中止し、取得済みのget_messages()結果を
         そのまま呼び出し元へ渡す）
      3. Page.bringToFront → JS focus() → CDP Input.insertText で本文を入力し、valueの
         読み返しと送信ボタンのdisabled解除を確認する
      4. 送信ボタンをDOM直接clickで「1回だけ」クリックする（このスクリプト内で再クリックは
         一切行わない。失敗時も自動リトライしない＝二重送信防止を最優先する）
      5. 実際のPOSTレスポンス（HTTPステータス・本文）をCDP経由で捕捉し、status==200 かつ
         本文の result が真であることをもって初めて成功と判定する
         （クリックの成否・HTTP 200単体では判定しない）
      6. 成功していれば、get_messages()を再度呼び直し、送信したメッセージを含む最新の全件を
         戻り値として返す

    戻り値: {
        "ok": bool, "error": str|None,
        "reason": "new_message_detected"|None,
        "new_messages": list|None,  # 成功時・新着検出時とも、get_messages()の戻り値そのもの
    }
    """
    url = f"https://fril.jp/transaction?item_id={order_id}"
    driver.get(url)
    time.sleep(4)

    self_name = get_self_name(driver)
    current_messages = get_messages(driver, self_name)
    if len(current_messages) != expected_last_message_no:
        return {"ok": False, "error": "新しいメッセージを受信したため送信を中止しました",
                "reason": "new_message_detected", "new_messages": current_messages}

    textarea_els = driver.find_elements(By.CSS_SELECTOR, "textarea#order-comment")
    if not textarea_els:
        return {"ok": False, "error": "本文入力欄が見つかりません", "reason": None, "new_messages": None}
    textarea = textarea_els[0]

    send_button = None
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        if btn.text.strip() == CHAT_SEND_BUTTON_TEXT:
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
    with urllib.request.urlopen(f"http://127.0.0.1:{DEBUG_PORT}/json/list") as resp:
        targets = _json.loads(resp.read().decode("utf-8"))
    current_url_now = driver.current_url
    target = next((t for t in targets if t.get("url") == current_url_now), None)
    if target is None:
        return {"ok": False, "error": "CDPターゲットが見つかりません", "reason": None, "new_messages": None}

    ws = _ws_client.create_connection(target["webSocketDebuggerUrl"], timeout=SEND_RESPONSE_WAIT_SEC, suppress_origin=True)
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

        deadline = time.time() + SEND_RESPONSE_WAIT_SEC
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
                if req.get("method") == "POST" and SEND_MESSAGE_API_URL_SUBSTR in req.get("url", ""):
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

    # 実送信成功。get_messages()を再実行し、送信分を含む最新の全件を返す
    # （通常scrapeと同じsave_vendor_messages()経路でDB保存できるようにするため）。
    time.sleep(1.0)
    fresh_messages = get_messages(driver, self_name)

    return {"ok": True, "error": None, "reason": None, "new_messages": fresh_messages}


# ============================================================
# メイン
# ============================================================
def main():
    ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{DEBUG_PORT}"

    driver = webdriver.Chrome(options=options)
    sql_conn = get_sql_server_connection()
    access_conn = get_access_connection()

    try:
        driver.switch_to.new_window('tab')

        transaction_urls = collect_in_progress_transaction_urls(driver)
        print(f"取引URL数(取引中のみ): {len(transaction_urls)}")
        print()

        for url in transaction_urls:
            try:
                driver.get(url)
                time.sleep(4)

                order_id = get_order_id(url)
                if not order_id:
                    print(url)
                    print("ERROR: 注文ID(item_id)を取得できませんでした")
                    print()
                    continue

                self_name = get_self_name(driver)
                raw_status = get_raw_status(driver)
                tracking_number, carrier = get_tracking_info(driver)
                seller_name = get_seller_name(driver)
                messages = get_messages(driver, self_name)
                has_seller_message = any(m["sender_type"] == "出品者" for m in messages)

                daily_updated = update_daily_purchase_status(access_conn, order_id, raw_status, has_seller_message)
                update_daily_tracking_info(access_conn, order_id, tracking_number, carrier)
                save_vendor_messages(sql_conn, VENDOR_NAME, order_id, messages)

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


if __name__ == "__main__":
    main()
