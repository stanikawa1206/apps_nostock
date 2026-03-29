import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

# .env を読み込む
load_dotenv()

def test_takafumi2_mail():
    # 1. 環境変数から情報を取得
    main_email = os.getenv("GMAIL_SENDER_EMAIL")
    
    t2_email = os.getenv("TAKAFUMI2_EMAIL")
    t2_password = os.getenv("TAKAFUMI2_PASSWORD")

    if not t2_email or not t2_password:
        print("❌ エラー: .env に TAKAFUMI2_EMAIL または PASSWORD が設定されていません")
        return

    # 2. メールオブジェクトの作成
    subject = "【テスト】貴文② 設定確認メール"
    body = f"""
    これは貴文②のメール送信テストです。
    
    送信元（From）: {t2_email}
    宛先（To）: {t2_email}
    CC: {main_email}
    
    このメールが届いていれば、.envの設定とログイン認証は正常です。
    """

    msg = MIMEMultipart()
    msg["From"] = t2_email
    msg["To"] = t2_email
    msg["Cc"] = main_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    # 送信先リスト（ToとCcの両方に届けるために必要）
    recipients = [t2_email, main_email]

    # 3. 送信実行
    print(f"🚀 {t2_email} にログインして送信を試みます...")
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(t2_email, t2_password) # 貴文②のアカウントでログイン
            
            # sendmailを使用してリスト全員に送る
            server.sendmail(t2_email, recipients, msg.as_string())
            
        print("\n✅ 送信に成功しました！")
        print(f"📬 {t2_email} (To) と {main_email} (Cc) の両方の受信トレイを確認してください。")
        
    except Exception as e:
        print(f"\n❌ 送信失敗: {e}")
        print("\n💡 ヒント:")
        print("- アプリパスワードが正しいか確認してください")
        print("- Googleアカウントの2段階認証が有効か確認してください")

if __name__ == "__main__":
    test_takafumi2_mail()