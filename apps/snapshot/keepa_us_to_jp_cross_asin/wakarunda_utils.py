# wakarunda_utils.py
# -*- coding: utf-8 -*-
import time
import re
import subprocess
import os
import socket
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# =============================================================================
# 設定
# =============================================================================
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"C:\chrome_wakurunda_profile"
DEBUG_PORT = 9222

def is_port_in_use(port):
    """指定したポートが使用中(Chromeがリッスンしているか)を確認する"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def ensure_chrome_running():
    """Chromeが起動していなければ永続化モードで起動し、起動済みなら何もしない"""
    if is_port_in_use(DEBUG_PORT):
        print(f">> 既存のChrome(ポート{DEBUG_PORT})を検出しました。そのまま接続します。")
        return True

    print(f">> Chromeが見つかりません。永続化Chromeを新規起動します...")
    if not os.path.exists(CHROME_PATH):
        print(f"❌ エラー: Chromeが見つかりません: {CHROME_PATH}")
        return False

    os.makedirs(USER_DATA_DIR, exist_ok=True)
    cmd = f'start "" "{CHROME_PATH}" --remote-debugging-port={DEBUG_PORT} --user-data-dir="{USER_DATA_DIR}"'
    subprocess.run(cmd, shell=True)
    
    # 起動完了(ポートがリッスン状態になる)まで最大10秒待機
    for _ in range(10):
        if is_port_in_use(DEBUG_PORT):
            time.sleep(2) # 起動直後の安定化のための追加待機
            print(">> Chromeの起動完了を確認しました。")
            return True
        time.sleep(1)
        
    print("❌ Chromeの起動確認にタイムアウトしました。")
    return False

def get_wakurunda_risk(driver):
    """iframe内の拡張機能からリスク判定を取得"""
    driver.switch_to.default_content()
    for _ in range(5): 
        try:
            iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src^='chrome-extension://']")
            for iframe in iframes:
                driver.switch_to.frame(iframe)
                els = driver.find_elements(By.CSS_SELECTOR, "span.risk_title, span.no-result-text, div")
                for el in els:
                    raw_text = el.text.strip() or el.get_attribute("textContent").strip()
                    clean_text = raw_text.replace("\n", "").replace(" ", "").replace(" ", "")
                    if "アカウントリスクが低い" in clean_text:
                        driver.switch_to.default_content()
                        return "-"
                    match = re.search(r"危険度([A-Z])", clean_text)
                    if match:
                        driver.switch_to.default_content()
                        return match.group(1)
                driver.switch_to.default_content()
        except: pass
        time.sleep(1)
    return "判定不能"

def get_internal_brand(driver):
    """商品ページからブランド名を取得"""
    driver.switch_to.default_content()
    try:
        element = driver.find_element(By.ID, "bylineInfo")
        text = element.text.strip()
        brand = text.replace("ブランド:", "").replace("ブランド：", "").replace("のストアを表示", "").strip()
        return brand
    except:
        return "取得失敗"

def fetch_rank_and_brand_by_asin(asin):
    """ASINから直接ページを開き、判定ランクとブランド名を返す"""
    opt = Options()
    opt.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")
    
    try:
        driver = webdriver.Chrome(options=opt)
        driver.get(f"https://www.amazon.co.jp/dp/{asin}")
        time.sleep(3) 
        
        risk = get_wakurunda_risk(driver)
        page_brand = get_internal_brand(driver)
        
        return risk, page_brand
    except Exception as e:
        print(f"   [Selenium Error] {asin}: {e}")
        return "判定不能", "取得失敗"