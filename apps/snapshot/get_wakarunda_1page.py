# -*- coding: utf-8 -*-
r"""
get_wakurunda_risk.py
- 127.0.0.1:9222 のデバッグChromeに接続
- dpページを開く
- chrome-extension iframe を全探索して span.risk_title を見つけたら出力
"""
import subprocess
import time
import urllib.request

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

DP_URL = "https://www.amazon.co.jp/dp/B0CBPDJQFL?th=1"

CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DEBUG_PORT = 9222
USER_DATA_DIR = r"C:\chrome_debug_9222"
USER_DATA_DIR = r"C:\chrome_wakurunda_profile"

def is_debug_chrome_alive(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1) as r:
            return r.status == 200
    except Exception:
        return False


def launch_debug_chrome():
    if is_debug_chrome_alive(DEBUG_PORT):
        return  # すでに起動していれば何もしない

    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={DEBUG_PORT}",
        f"--user-data-dir={USER_DATA_DIR}",
    ]
    subprocess.Popen(cmd)
    time.sleep(3)  # 起動待ち


def main() -> None:
    launch_debug_chrome()   # ← これを追加

    opt = Options()
    opt.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")

    driver = webdriver.Chrome(options=opt)
    driver.get(DP_URL)

    wait = WebDriverWait(driver, 60)

    # 拡張由来っぽい iframe を全部拾う
    iframes = wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "iframe[src^='chrome-extension://']")
        )
    )

    last_err = None
    for idx, iframe in enumerate(iframes, start=1):
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(iframe)

            el = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.risk_title"))
            )
            print(el.text.strip())
            return
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"risk_title が見つかりませんでした。iframes={len(iframes)} last_err={last_err}")


if __name__ == "__main__":
    main()
