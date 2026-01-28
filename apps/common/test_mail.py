# test_send_mail.py
# utils.py と同じフォルダーに置いて実行するテスト用スクリプト

from pathlib import Path
import os

# .env を読む（python-dotenv が入っている前提）
# 入ってない場合は: pip install python-dotenv
from dotenv import load_dotenv

from utils import send_mail


def main():
    here = Path(__file__).resolve().parent
    env_path = here / ".env"

    # .env があれば読み込む（無ければ OS の環境変数を使う）
    if env_path.exists():
        load_dotenv(env_path)

    sender = os.getenv("GMAIL_SENDER_EMAIL")
    app_pass = os.getenv("GMAIL_APP_PASSWORD")

    # ここで落ちるなら「.envを読めてない」か「キー名が違う」
    if not sender or not app_pass:
        raise RuntimeError(
            "GMAIL_SENDER_EMAIL / GMAIL_APP_PASSWORD が見つかりません。.env の場所/キー名を確認してください。"
        )

    subject = "TEST: send_mail() from apps_nostock"
    body = "This is a test email.\n\nIf you received this, SMTP auth is OK.\n"

    # 添付テストしたければ、同フォルダの files/test.txt みたいに置いてパスを追加
    attachments = None
    # attachments = [str(here / "errors.txt")]  # 例: 既存ファイルを添付

    send_mail(
        subject=subject,
        body=body,
        sender_email=sender,       # 省略でもOK（utils側で getenv する）
        receiver_email=sender,     # 自分宛て
        password=app_pass,         # 省略でもOK（utils側で getenv する）
        attachments=attachments,
    )


if __name__ == "__main__":
    main()
