# -*- coding: utf-8 -*-
"""
LINE の手動テスト送信（本番の「eBay通知：谷川用」チャネルへ broadcast する）。

    python -m apps.tests.manual_line_send

- 実行したときだけ送信する。import しただけでは何もしない。
  （2026-11-15、旧 test_line.py が全テストの一括実行で import されるたびに送信され、6回届いた）
- ファイル名を test_*.py にしないこと（unittest discover や test_*.py のループに拾われる）。
- トークンは eBay受注通知（apps/inventory/fetch_orders_ebay.py）と同じものを使う。
"""
import requests

TEXT = "プログラムからのテスト配信です。このラインメッセージが届きましたら、お知らせください"


def main():
    from apps.inventory.fetch_orders_ebay import _LINE_TOKEN_DEFAULT

    r = requests.post(
        "https://api.line.me/v2/bot/message/broadcast",
        headers={
            "Authorization": f"Bearer {_LINE_TOKEN_DEFAULT}",
            "Content-Type": "application/json",
        },
        json={"messages": [{"type": "text", "text": TEXT}]},
        timeout=10,
    )
    print(r.status_code)
    print(r.text)


if __name__ == "__main__":
    main()
