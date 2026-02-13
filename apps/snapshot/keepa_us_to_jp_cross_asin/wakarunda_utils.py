# wakarunda_utils.py
# -*- coding: utf-8 -*-
import time
import re
import subprocess
import os
import urllib.parse
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

def kill_chrome_on_port(port):
    """指定ポートのゾンビプロセスを掃除"""
    try:
        cmd = f'netstat -ano | findstr :{port}'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if not result.stdout: return
        for line in result.stdout.strip().split('\n'):
            if "LISTENING" in line:
                pid = line.split()[-1]
                if pid.isdigit():
                    subprocess.run(f'taskkill /F /PID {pid}', shell=True)
                    time.sleep(1)
    except: pass

def launch_fresh_chrome():
    """既存のプロセスを殺してから新しくChromeを起動する"""
    kill_chrome_on_port(DEBUG_PORT)
    if not os.path.exists(CHROME_PATH):
        print(f"❌ エラー: Chromeが見つかりません: {CHROME_PATH}")
        return False

    cmd = [CHROME_PATH, f"--remote-debugging-port={DEBUG_PORT}", f'--user-data-dir={USER_DATA_DIR}', "about:blank"]
    print(f">> Chromeを新規起動します...")
    subprocess.Popen(cmd)
    
    for _ in range(10):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('127.0.0.1', DEBUG_PORT)) == 0:
                time.sleep(2)
                return True
        time.sleep(1)
    return False

def get_wakurunda_risk(driver):
    """iframe内の拡張機能からリスク判定を取得"""
    driver.switch_to.default_content()
    found_text_candidate = None
    for _ in range(5): 
        try:
            iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src^='chrome-extension://']")
            for iframe in iframes:
                driver.switch_to.frame(iframe)
                els = driver.find_elements(By.CSS_SELECTOR, "span.risk_title, span.no-result-text, div")
                for el in els:
                    raw_text = el.text.strip() or el.get_attribute("textContent").strip()
                    clean_text = raw_text.replace("\n", "").replace(" ", "").replace("　", "")
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
        # 一般的なブランド表示箇所 (bylineInfo) を確認
        element = driver.find_element(By.ID, "bylineInfo")
        text = element.text.strip()
        # 「ブランド: 〇〇」や「〇〇のストアを表示」からブランド名を抽出
        brand = text.replace("ブランド:", "").replace("ブランド：", "").replace("のストアを表示", "").strip()
        return brand
    except:
        return "取得失敗"

def fetch_rank_and_brand_by_asin(asin):
    """ASINから直接ページを開き、判定ランクとブランド名を返す"""
    # ポートが空いていなければ再起動
    launch_fresh_chrome()

    opt = Options()
    opt.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")
    
    try:
        driver = webdriver.Chrome(options=opt)
        driver.get(f"https://www.amazon.co.jp/dp/{asin}")
        time.sleep(3) # 拡張機能の読み込み待機
        
        risk = get_wakurunda_risk(driver)
        page_brand = get_internal_brand(driver)
        
        return risk, page_brand
    except Exception as e:
        print(f"   [Selenium Error] {asin}: {e}")
        return "判定不能", "取得失敗"