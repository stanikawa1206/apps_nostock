# -*- coding: utf-8 -*-
r"""
get_wakurunda_risk.py

- BRAND を指定
- Amazon検索URL (p_89) をHTTP取得して ASIN を1つ特定（スポンサー除外を軽く試みる）
- 127.0.0.1:9222 のデバッグChromeに接続（無ければ自動起動）
- dpページを開く
- chrome-extension iframe を全探索して span.risk_title を見つけたら出力

前提:
- USER_DATA_DIR のプロファイルにワカルンダ拡張が入っていること
"""

import re
import subprocess
import time
import urllib.request
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ====== 設定 ======
BRAND = "Braun"  # ←ここを指定

CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
DEBUG_PORT = 9222
USER_DATA_DIR = r"C:\chrome_wakurunda_profile"

STARTUP_TIMEOUT_SEC = 20
SEARCH_TIMEOUT_SEC = 15


def is_debug_chrome_alive(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1) as r:
            return r.status == 200
    except Exception:
        return False


def wait_debug_chrome(port: int, timeout_sec: int) -> None:
    t0 = time.time()
    while True:
        if is_debug_chrome_alive(port):
            return
        if time.time() - t0 > timeout_sec:
            raise RuntimeError(f"デバッグChromeが起動しませんでした: port={port} timeout={timeout_sec}s")
        time.sleep(0.2)


def launch_debug_chrome() -> None:
    if is_debug_chrome_alive(DEBUG_PORT):
        return

    cmd = [
        CHROME_PATH,
        f"--remote-debugging-port={DEBUG_PORT}",
        f'--user-data-dir={USER_DATA_DIR}',
        "about:blank",
    ]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_debug_chrome(DEBUG_PORT, STARTUP_TIMEOUT_SEC)


def fetch_brand_search_html(brand: str) -> str:
    url = f"https://www.amazon.co.jp/s?rh=p_89%3A{quote(brand)}"
    req = urllib.request.Request(
        url,
        headers={
            # AmazonはUA無しだと弾く/簡易ページになることがあるので固定
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Accept-Language": "ja,en;q=0.9",
        },
    )
    with urllib.request.urlopen(req, timeout=SEARCH_TIMEOUT_SEC) as r:
        return r.read().decode("utf-8", errors="ignore")


def pick_one_asin_from_search_html(html: str) -> str:
    """
    /dp/ASIN を抽出して、周辺テキストに Sponsored/スポンサー がある候補を避けつつ1つ返す。
    """
    # href 内の /dp/ASIN を拾う（Amazonは10桁英数）
    matches = list(re.finditer(r"/dp/([A-Z0-9]{10})", html))
    if not matches:
        raise RuntimeError("検索HTMLから /dp/ASIN が見つかりませんでした（HTML構造変化 or ブロックの可能性）")

    # できるだけ「スポンサー」を避ける（周辺に Sponsored/スポンサー が含まれる候補はスキップ）
    for m in matches:
        asin = m.group(1)
        start = max(0, m.start() - 600)
        end = min(len(html), m.end() + 600)
        around = html[start:end]
        if ("Sponsored" in around) or ("スポンサー" in around):
            continue
        return asin

    # 全部スポンサー判定になった場合は最初のASINを採用（ここまで来るのは稀）
    return matches[0].group(1)


def get_wakurunda_risk_in_current_page(driver) -> str:
    wait = WebDriverWait(driver, 60)

    iframes = wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "iframe[src^='chrome-extension://']")
        )
    )

    last_err = None
    for iframe in iframes:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(iframe)
            el = WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.risk_title"))
            )
            return el.text.strip()
        except Exception as e:
            last_err = e

    raise RuntimeError(f"risk_title が見つかりませんでした。iframes={len(iframes)} last_err={last_err}")


def main() -> None:
    # 1) BRAND → ASIN（ブラウザ使わずにHTTPで確定）
    html = fetch_brand_search_html(BRAND)
    asin = pick_one_asin_from_search_html(html)

    dp_url = f"https://www.amazon.co.jp/dp/{asin}"

    # 2) Chrome起動 → 9222接続
    launch_debug_chrome()

    opt = Options()
    opt.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")
    driver = webdriver.Chrome(options=opt)

    # 3) dpを開いて危険度取得
    driver.get(dp_url)
    risk = get_wakurunda_risk_in_current_page(driver)

    print(f"brand={BRAND} asin={asin} risk={risk}")


if __name__ == "__main__":
    main()
