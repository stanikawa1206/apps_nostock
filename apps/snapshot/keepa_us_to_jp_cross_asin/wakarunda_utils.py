# wakarunda_utils.py
# -*- coding: utf-8 -*-
import time
import re
import subprocess
import os
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# =============================================================================
# 設定 (元のファイルから引き継ぎ)
# =============================================================================
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
USER_DATA_DIR = r"C:\chrome_wakurunda_profile"
DEBUG_PORT = 9222

# =============================================================================
# Chrome制御関数
# =============================================================================
def kill_chrome_on_port(port):
    """ゾンビプロセスの掃除"""
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
    """Chrome起動"""
    # 既に起動しているかチェックし、起動していなければ起動するロジックの方が高速ですが、
    # 元のコードの安定性を重視して「都度起動」または「ポート確認」を行います。
    # ここでは簡易的に「ポートが開いてなければ起動」とします。
    
    # ポートチェック (簡易)
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', DEBUG_PORT))
        sock.close()
        if result == 0:
            # 既に起動しているので何もしない
            return
    except:
        pass

    if not os.path.exists(CHROME_PATH):
        print("エラー: Chromeが見つかりません")
        return

    cmd = [CHROME_PATH, f"--remote-debugging-port={DEBUG_PORT}", f'--user-data-dir={USER_DATA_DIR}', "about:blank"]
    print(f">> Chromeを起動します...")
    subprocess.Popen(cmd)
    time.sleep(3)

def get_candidate_asins_from_html(html: str, limit: int = 3) -> list:
    """HTMLからASIN抽出"""
    candidates = []
    seen = set()
    if not html: return []
    # シンプルな正規表現
    matches = list(re.finditer(r"/dp/([A-Z0-9]{10})", html))
    for m in matches:
        if len(candidates) >= limit: break
        asin = m.group(1)
        if asin in seen: continue
        
        # スポンサープロダクトを除外する簡易ロジック
        start = max(0, m.start() - 500)
        end = min(len(html), m.end() + 500)
        around = html[start:end]
        if ("Sponsored" in around) or ("スポンサー" in around) or ("s-sponsored" in around):
            continue
            
        candidates.append(asin)
        seen.add(asin)
    return candidates

def get_wakurunda_risk(driver):
    """ワカルンダのリスク判定を取得"""
    driver.switch_to.default_content()
    # print("   -> [Risk] ワカルンダの判定を確認中...")
    
    found_text_candidate = None
    
    # 最大5秒待機
    for _ in range(5): 
        try:
            iframes = driver.find_elements(By.CSS_SELECTOR, "iframe[src^='chrome-extension://']")
            for iframe in iframes:
                driver.switch_to.default_content()
                driver.switch_to.frame(iframe)
                
                # 判定テキストを探す
                els = driver.find_elements(By.CSS_SELECTOR, "span.risk_title, span.no-result-text, div")
                for el in els:
                    raw_text = el.text.strip()
                    if not raw_text: raw_text = el.get_attribute("textContent").strip()
                    clean_text = raw_text.replace("\n", "").replace(" ", "").replace("　", "")
                    
                    if "アカウントリスクが低い" in clean_text:
                        driver.switch_to.default_content()
                        return "-"
                    match = re.search(r"危険度([A-Z])", clean_text)
                    if match:
                        driver.switch_to.default_content()
                        return match.group(1)
                        
                    # 候補として保持
                    if "アカウントリスク" in clean_text or "危険度" in clean_text:
                        found_text_candidate = raw_text
            
            # 見つからなかった場合の再試行ウェイト
            time.sleep(1)
            
        except: 
            time.sleep(1)
            
    driver.switch_to.default_content()
    
    # 最終確認
    if found_text_candidate:
        clean_candidate = found_text_candidate.replace("\n", "").replace(" ", "")
        if "アカウントリスクが低い" in clean_candidate:
            return "-"
        # 危険度X の抽出
        m = re.search(r"危険度([A-Z])", clean_candidate)
        if m: return m.group(1)

    return "判定不能"

# =============================================================================
# メイン関数: ブランド名からランクを返す
# =============================================================================
def fetch_brand_rank_from_selenium(brand_name):
    """
    Step 3から呼び出される関数。
    ブランド名をAmazon検索し、トップに出てきた商品のワカルンダ判定を返す。
    """
    # 1. Chrome起動確認
    launch_fresh_chrome()
    
    # 2. Driver接続
    opt = Options()
    opt.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")
    try:
        driver = webdriver.Chrome(options=opt)
    except Exception as e:
        print(f"[Selenium Error] 接続失敗: {e}")
        return "E" # 接続エラー時はEとする

    # 3. Amazon検索
    try:
        # print(f"   [Wakarunda] '{brand_name}' を検索...")
        url = f"https://www.amazon.co.jp/s?rh=p_89%3A{urllib.parse.quote(brand_name)}"
        driver.get(url)
        time.sleep(2)
        
        # 4. ASIN取得 (上位1件だけ見れば概ね判定可能)
        asins = get_candidate_asins_from_html(driver.page_source, limit=1)
        
        if not asins:
            # print("   -> 検索結果なし (ASIN見つからず)")
            return "E" # 商品が見つからない場合は判定不能としてE

        target_asin = asins[0]
        
        # 5. 商品ページへ移動
        driver.get(f"https://www.amazon.co.jp/dp/{target_asin}")
        time.sleep(2)

        # 6. ワカルンダ判定取得
        risk = get_wakurunda_risk(driver)
        
        if risk and risk != "判定不能":
            print(f"   [Wakarunda] {brand_name} -> {risk}")
            return risk
        else:
            return "E"

    except Exception as e:
        print(f"   [Wakarunda Error] {e}")
        return "E"