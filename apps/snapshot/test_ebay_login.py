import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException

# 既存のプロジェクト設定からインポート
from apps.common.utils import get_sql_server_connection

def build_driver():
    opts = Options()
    # VBAと同じプロファイルを使用
    opts.add_argument(r"--user-data-dir=C:\Users\stani\AppData\Local\Google\Chrome\User Data")
    opts.add_argument("--profile-directory=Default")
    
    # 安定化のための設定
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    
    # パスワードマネージャーやパスキーの干渉を防止
    opts.add_experimental_option("prefs", {
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False
    })
    opts.add_argument("--disable-features=WebAuthenticationServices")

    driver = webdriver.Chrome(options=opts)
    # navigator.webdriver を隠す
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def ebay_login_stable(driver, username, password):
    """VBAの安定ロジックを完全再現したログイン関数"""
    print(f"ログイン試行中: {username}")
    driver.get("https://signin.ebay.com/signin/")
    
    # --- 1. CAPTCHA対応 (最大5分待機) ---
    timeout = time.time() + 300
    while time.time() < timeout:
        # CAPTCHAの有無を確認
        is_captcha = driver.execute_script(
            "return document.body.innerText.indexOf('Please verify yourself to continue') !== -1;"
        )
        if is_captcha:
            print("CAPTCHA検出：ブラウザで手動解除してください...")
            time.sleep(5)
            continue
        
        # ID入力欄かパスワード入力欄が出現したか確認
        ready_state = driver.execute_script(
            "return (document.getElementById('userid') !== null || document.querySelector('input[type=password]') !== null);"
        )
        if ready_state:
            break
        
        time.sleep(2)

    # --- 2. パターン判定（useridがある場合） ---
    has_user = driver.execute_script("return (document.getElementById('userid') !== null);")
    
    if has_user:
        # ID入力 (VBA同様にSendKeysを使用)
        user_input = driver.find_element(By.ID, "userid")
        user_input.clear()
        user_input.send_keys(username)
        
        # Continueボタンクリック (JSで確実に実行)
        driver.execute_script("""
            var btn = document.getElementById('signin-continue-btn');
            if(btn) btn.click();
        """)
        time.sleep(2)

    # --- 3. パスワード欄が出るまで待つ (最大60秒) ---
    timeout = time.time() + 60
    while time.time() < timeout:
        has_pass = driver.execute_script("return (document.querySelector('input[type=password]') !== null);")
        if has_pass:
            break
        time.sleep(1)

    # --- 4. JSでパスワード代入 ＋ イベント発火 ---
    # VBAの script = "var p = ..." の部分を完全再現
    escaped_pwd = password.replace("'", "\\'")
    js_input_pwd = f"""
        var p = document.querySelector('input[type=password]');
        if(p){{
            p.focus();
            p.value = '{escaped_pwd}';
            p.dispatchEvent(new Event('input', {{bubbles:true}}));
            p.dispatchEvent(new Event('change', {{bubbles:true}}));
        }}
    """
    driver.execute_script(js_input_pwd)
    time.sleep(1)

    # --- 5. ログインボタンをクリック ---
    clicked = driver.execute_script("""
        var b = document.querySelector('button[type=submit], button#sgnBt');
        if(b){ b.click(); return true; } else { return false; }
    """)
    
    if clicked:
        print("ログインボタンをクリックしました。")
        # 2段階認証などが必要な場合のために少し待機
        time.sleep(5)
        # 最終目的地へ移動
        driver.get("https://www.ebay.com/sh/ovw")
    else:
        print("ログインボタンが見つかりませんでした。")

def main():
    # DBからアカウント取得
    conn = get_sql_server_connection()
    cur = conn.cursor()
    cur.execute("SELECT TOP 1 account, username, [password] FROM mst.ebay_accounts WHERE ISNULL(is_excluded,0) = 0")
    row = cur.fetchone()
    conn.close()

    if not row:
        print("アカウント情報がありません")
        return

    account, user, pwd = row
    driver = build_driver()

    try:
        ebay_login_stable(driver, user, pwd)
        print(f"【{account}】ログインプロセス完了")
        # 動作確認のため10秒維持
        time.sleep(10)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()