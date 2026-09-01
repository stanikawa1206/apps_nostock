"""
/messages画面の「送信」ボタンから、PayPayフリマの取引ページへ実際に返信を送信する処理。

Webアプリ本体（D:\\apps_resale\\furima\\webapp\\messages_blueprint.py）から呼び出される。
DOM操作（本文入力欄・送信ボタンの特定、送信前の再確認、実送信結果の確認）は
apps/etc/yahoo_furima_purchase.py の send_chat_message() に実装済みのものをそのまま使う
（本モジュールでは新たにセレクタ・判定ロジックを実装しない）。

既存のyahoo_furima_purchase.pyが使っているChromeデバッグセッション（ポート9223・
永続プロファイル）をそのまま利用する。ログイン状態を壊さないよう、送信は
新しいタブを開いて行い、完了後にそのタブだけを閉じる。
"""
import sys
from pathlib import Path

_NOSTOCK_ROOT = Path(r"D:\apps_nostock")
if str(_NOSTOCK_ROOT) not in sys.path:
    sys.path.insert(0, str(_NOSTOCK_ROOT))

from selenium import webdriver  # noqa: E402
from selenium.webdriver.chrome.options import Options  # noqa: E402

from apps.etc.yahoo_furima_purchase import (  # noqa: E402
    DEBUG_PORT,
    ensure_chrome_debugger,
    send_chat_message,
)


def send_paypay_reply(vendor_item_id: str, expected_last_message_no, reply_text: str) -> dict:
    """
    expected_last_message_no: /messages画面表示時点の最新メッセージのmessage_no
    （PayPayフリマにはメルカリのような安定した一意message_idが無いため、
    件数ベースの位置で新着有無を判定する）。
    戻り値: {"ok": bool, "error": str|None, "reason": str|None, "new_messages": list|None}
    """
    ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{DEBUG_PORT}"
    driver = webdriver.Chrome(options=options)

    try:
        driver.switch_to.new_window('tab')
        result = send_chat_message(driver, vendor_item_id, expected_last_message_no, reply_text)
    finally:
        try:
            driver.close()
        except Exception:
            pass

    return result
