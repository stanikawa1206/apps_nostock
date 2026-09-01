"""
Yahoo!フリマ（旧PayPayフリマ、paypayfleamarket.yahoo.co.jp）の購入管理スクリプト。

一覧 https://paypayfleamarket.yahoo.co.jp/my/purchase には、
「発送待ち」「商品が到着したら評価をしてください」「取引完了」等のステータスが
テキストで表示されている（ラクマのようなタブでの絞り込みは無い）。
このうち「取引完了」を除外し、それ以外を対象とする。「もっと見る」は押さず、
最初に表示されている範囲だけを対象にする。

配送会社は、送り状番号が実際に kuronekoyamato.co.jp / post.japanpost.jp への
<a href> になっていることを利用して判定する（文言推測より確実）。

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

VENDOR_NAME = "ＰａｙＰａｙフリマ"

CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
PROFILE_DIR = r"D:\apps_nostock\selenium_profile"
DEBUG_PORT = 9223
LAUNCH_TIMEOUT_SEC = 30

PURCHASE_LIST_URL = "https://paypayfleamarket.yahoo.co.jp/my/purchase"

ORDER_ID_RE = re.compile(r"/item/([A-Za-z0-9]+)/trade/buyer")

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
# 分前に切り替わるまでの間、この形式に非対応だとget_messages()が検出できなかった）。
RELATIVE_TIME_RE = re.compile(r"^(たった今|\d+秒前|\d+分前|\d+時間前|\d+日前|\d+週間前|\d+ヶ月前|\d+年前)$")


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
# 購入一覧
# ============================================================
def collect_active_transactions(driver, retries: int = 6, interval: float = 2.0):
    """
    最初に表示されている範囲（「もっと見る」は押さない）から、
    ステータスが「取引完了」以外の取引URL・生ステータス文言の一覧を返す。
    戻り値: [(url, list_status_text), ...]
    """
    driver.get(PURCHASE_LIST_URL)

    for attempt in range(1, retries + 1):
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/trade/buyer']")
        if links or attempt == retries:
            break
        time.sleep(interval)

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


def get_order_id(url: str):
    m = ORDER_ID_RE.search(url)
    return m.group(1) if m else None


def get_raw_status(driver) -> str:
    """
    個別取引ページの文言から、共通の raw_status 語彙
    （"発送前" / "発送済み" / "☆出荷可能"）へ変換する。
    一覧の文言だけでは「商品が到着したら評価をしてください」
    「未評価の場合は評価してください」のように表記が複数あり判定しきれないため、
    詳細ページの共通見出しで判定する（実機確認済み）。

    「発送済みであることを確認できた場合だけ発送済みとする」を原則とし、
    既知のいずれの文言にも一致しない場合は自動返信を誤らせないよう例外を送出する
    （mercari_purchase.pyのget_raw_status()と同じ方針）。
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


# ============================================================
# 個別取引ページ
# ============================================================
def get_tracking_info(driver):
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


def get_seller_name(driver):
    """出品者名は安定したクラス名 .UserInfo__Name で取得できる（実機確認済み）。"""
    els = driver.find_elements(By.CSS_SELECTOR, ".UserInfo__Name")
    return els[0].text.strip() if els else None


def get_messages(driver, seller_name: str):
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


# ============================================================
# メッセージ送信（/messages画面からの実送信）
# ============================================================
# 本文入力欄・送信ボタンは実機DOM調査で特定済み（推測ではない）。
#   本文入力欄: <textarea placeholder="メッセージを入力">（React管理下、name/id無し）
#   送信ボタン: 文言「取引メッセージを送る」の<button type="button">（本文が空だとdisabled）
# フォーム(<form>)は存在せず、送信ボタンクリック位置に対するdocument.elementFromPoint()も
# メルカリ同様Noneになる（座標系のズレが同じく発生する）ため、Selenium座標クリックではなく
# DOM直接clickを使う。
CHAT_SEND_BUTTON_TEXT = "取引メッセージを送る"

SEND_RESPONSE_WAIT_SEC = 15.0


def send_chat_message(driver, order_id: str, expected_last_message_no, reply_text: str) -> dict:
    """
    PayPayフリマの取引ページへ実際にメッセージを送信する（誤送信防止のため必ずこの手順で行う）。

    実機調査済み（2026-08-29、実送信1回で確認）:
      送信通信: POST https://paypayfleamarket-sec.yahoo.co.jp/api/v2/items/{order_id}/message
      同一オリジンのためCORSプリフライトは発生しない（メルカリで問題になったOPTIONS誤認識の
      心配は無いが、念のためメルカリと同じ「1本の継続的な受信ループ」構造で捕捉する）。
      レスポンスにメッセージ固有の一意ID（message_id相当）は含まれず、
      {"thread": [{"text":..., "date":..., "userId":...}, ...]} という会話全文のみが返る。
      そのためtrx.vendor_messageへの保存はmessage_id方式ではなく、既存のsave_vendor_messages()
      （(vendor_name, vendor_item_id, message_no)キーのMERGE）をそのまま使う。送信成功後に
      get_messages()を再実行し、送信分を含む最新の全件をそのまま渡せば、通常scrapeと全く同じ
      経路で重複なく保存できる。

    手順:
      1. 取引ページを開き、現在のメッセージ件数をget_messages()で取得する
      2. /messages画面表示時点の件数(expected_last_message_no)と比較し、新しいメッセージが
         増えていないか確認する（増えていれば送信せず中止し、取得済みのget_messages()結果を
         そのまま呼び出し元へ渡す＝呼び出し元がDB保存・画面更新・返信判定の再実行に使う）
      3. Page.bringToFront → JS focus() → CDP Input.insertText で本文を入力し、valueの
         読み返しと送信ボタンのdisabled解除を確認する
      4. 送信ボタンをDOM直接clickで「1回だけ」クリックする（このスクリプト内で再クリックは
         一切行わない。失敗時も自動リトライしない＝二重送信防止を最優先する）
      5. 実際のPOSTレスポンス（HTTPステータス・本文）をCDP経由で捕捉し、status==200 かつ
         本文に thread 配列が含まれることをもって初めて成功と判定する
         （クリックの成否・HTTP 200単体では判定しない）
      6. 成功していれば、get_messages()を再度呼び直し、送信したメッセージを含む最新の全件を
         戻り値として返す

    戻り値: {
        "ok": bool, "error": str|None,
        "reason": "new_message_detected"|None,
        "new_messages": list|None,  # 成功時・新着検出時とも、get_messages()の戻り値そのもの
    }
    """
    url = f"https://paypayfleamarket-sec.yahoo.co.jp/item/{order_id}/trade/buyer"
    driver.get(url)
    time.sleep(4)

    seller_name = get_seller_name(driver)
    current_messages = get_messages(driver, seller_name)
    if len(current_messages) != expected_last_message_no:
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
        if btn.text.strip() == CHAT_SEND_BUTTON_TEXT:
            send_button = btn
            break
    if send_button is None:
        return {"ok": False, "error": "送信ボタンが見つかりません", "reason": None, "new_messages": None}

    import json as _json
    import websocket as _ws_client  # ローカルimport: 送信結果の監視専用のため使用箇所を限定する

    with urllib.request.urlopen(f"http://127.0.0.1:{DEBUG_PORT}/json/list") as resp:
        targets = _json.loads(resp.read().decode("utf-8"))
    target = next((t for t in targets if order_id in t.get("url", "")), None)
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

        send_api_substr = f"/api/v2/items/{order_id}/message"
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

    # 実送信成功。get_messages()を再実行し、送信分を含む最新の全件を返す
    # （通常scrapeと同じsave_vendor_messages()経路でDB保存できるようにするため）。
    time.sleep(1.0)
    fresh_messages = get_messages(driver, seller_name)

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

        transactions = collect_active_transactions(driver)
        print(f"取引URL数(取引完了を除く): {len(transactions)}")
        print()

        for url, list_status_text in transactions:
            try:
                driver.get(url)
                time.sleep(4)

                order_id = get_order_id(url)
                if not order_id:
                    print(url)
                    print("ERROR: 注文IDを取得できませんでした")
                    print()
                    continue

                raw_status = get_raw_status(driver)
                tracking_number, carrier = get_tracking_info(driver)
                seller_name = get_seller_name(driver)
                messages = get_messages(driver, seller_name)
                has_seller_message = any(m["sender_type"] == "出品者" for m in messages)

                daily_updated = update_daily_purchase_status(access_conn, order_id, raw_status, has_seller_message)
                update_daily_tracking_info(access_conn, order_id, tracking_number, carrier)
                save_vendor_messages(sql_conn, VENDOR_NAME, order_id, messages)

                print(url)
                print(f"order_id={order_id}  list_status={list_status_text!r}  raw_status={raw_status}  "
                      f"tracking={tracking_number}  carrier={carrier}  seller={seller_name}  "
                      f"messages={len(messages)}  日常更新={'OK' if daily_updated else '対象行なし'}")
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
