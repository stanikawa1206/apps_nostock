"""
/messages画面の「送信」ボタンから、メルカリの取引ページへ実際に返信を送信する処理。

Webアプリ本体（D:\\apps_resale\\furima\\webapp\\messages_blueprint.py）から呼び出される。
DOM操作（本文入力欄・送信ボタンの特定、送信前の再確認）は
apps/etc/mercari_purchase.py の send_chat_message() に実装済みのものをそのまま使う
（本モジュールでは新たにセレクタを推測しない）。

既存のmercari_purchase.pyが使っているChromeデバッグセッション（ポート9223・
永続プロファイル）をそのまま利用する。ログイン状態を壊さないよう、送信は
新しいタブを開いて行い、完了後にそのタブだけを閉じる。

【2026-08-28 実機調査により変更】send_chat_message()の成功判定はクリックの成否ではなく、
実際の transaction_messages/post のレスポンス（result=="OK" かつ本物のmessage_id）を
CDP経由で捕捉できたことをもって行うよう改修済み。戻り値には message_id・message_no・
message_datetime が含まれる（成功時のみ）。trx.vendor_messageへの実際のINSERTは、
本モジュールではなく呼び出し元（messages_blueprint.py）が行う。
"""
import sys
from pathlib import Path

_NOSTOCK_ROOT = Path(r"D:\apps_nostock")
if str(_NOSTOCK_ROOT) not in sys.path:
    sys.path.insert(0, str(_NOSTOCK_ROOT))

from selenium import webdriver  # noqa: E402
from selenium.webdriver.chrome.options import Options  # noqa: E402

from apps.etc.mercari_purchase import (  # noqa: E402
    DEBUG_PORT,
    ensure_chrome_debugger,
    send_chat_message,
)


def send_mercari_reply(vendor_item_id: str, expected_last_message_id, reply_text: str) -> dict:
    """
    expected_last_message_id: /messages画面表示時点の最新メッセージのmessage_id
    （メルカリ内部の安定した一意ID。DOM順のmessage_noではない）。
    戻り値: {"ok": bool, "error": str|None}
    """
    ensure_chrome_debugger()

    options = Options()
    options.debugger_address = f"127.0.0.1:{DEBUG_PORT}"
    driver = webdriver.Chrome(options=options)

    try:
        driver.switch_to.new_window('tab')
        result = send_chat_message(driver, vendor_item_id, expected_last_message_id, reply_text)
    finally:
        try:
            driver.close()
        except Exception:
            pass

    return result
