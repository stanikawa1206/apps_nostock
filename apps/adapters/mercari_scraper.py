# -*- coding: utf-8 -*-
"""
mercari_scraper.py

【このファイルは何をする？】
Selenium（Chrome）でメルカリの検索結果ページをスクレイプするための
共通ユーティリティをまとめています。大きく分けて：
- WebDriver の生成/終了（プロファイル一時ディレクトリ付き）
- 「1ページ分」の商品カード（ID/タイトル/価格）を取り切るロジック
  - 個人出品（personal）用
  - Shops（ショップ出品）用

【関数一覧】
- build_driver(use_profile=False, user_data_dir=None, headless=True)
    … Chrome WebDriver を作る。プロファイル衝突を避ける一時ディレクトリ運用も面倒みる。
- safe_quit(driver)
    … driver.quit() の後始末（作った一時プロファイルの削除）もする安全終了。
- extract_item_listings(driver)
    …（personal用）現在表示中のページから (item_id, title, price) のタプル配列を抽出。
       item_id は /item/m12345678 の m～ を拾う。
- scroll_until_stagnant_collect_items(driver, pause, stagnant_times=3)
    …（personal用）スクロールしながら「伸びが止まるまで」1ページ分を取り切って返す。
- extract_shops_listings(driver)
    …（shops用）現在表示中のページから (shops_product_id, title, price) のタプル配列を抽出。
       product_id は /shops/product/ の末尾IDを拾う。
- scroll_until_stagnant_collect_shops(driver, pause, stagnant_times=3)
    …（shops用）スクロールしながら「伸びが止まるまで」1ページ分を取り切って返す。

【使い分けの目安】
- メルカリ通常（個人出品）URL → scroll_until_stagnant_collect_items()
- メルカリShops（item_types=beyond など）URL → scroll_until_stagnant_collect_shops()
"""

import re
import time
import random
import tempfile
import shutil
import os
import platform
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


# =========================
# WebDriver
# =========================
def build_driver(
    use_profile: bool = False,
    user_data_dir: str | None = None,
    headless: bool = True,
) -> webdriver.Chrome:
    """
    メルカリ一覧ページ取得用の Chrome WebDriver（Windows / Linux 両対応）

    - Windows / ローカル: 通常Chrome
    - Linux(VPS): headless + no-sandbox 対応
    """
    opts = Options()

    # =========================
    # OS 判定
    # =========================
    is_linux = platform.system() == "Linux"

    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")

    # ★ VPS(Linux) 必須
    if is_linux:
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")

    opts.add_argument("--window-size=1400,1000")

    # 言語・通知など最低限
    opts.add_argument("--lang=ja-JP")
    opts.add_experimental_option('prefs', {'intl.accept_languages': 'ja,ja-JP'})
    opts.add_argument("--disable-notifications")

    # ユーザーエージェントを日本語環境のWindowsに偽装
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    opts.add_argument(f'user-agent={user_agent}')

    # =========================
    # user-data-dir
    # =========================
    tmp_dir: Path | None = None

    if use_profile and user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    else:
        tmp_dir = Path(
            tempfile.mkdtemp(prefix=f"chrome-sess-{os.getpid()}-")
        )
        opts.add_argument(f"--user-data-dir={tmp_dir}")

    # =========================
    # Service
    # =========================
    # ★ パス指定しない → 環境に任せる
    service = Service()

    driver = webdriver.Chrome(service=service, options=opts)

    driver.set_page_load_timeout(20)
    driver.set_script_timeout(20)

    # safe_quit 用
    driver._tmp_user_data_dir = str(tmp_dir) if tmp_dir else None
    return driver

def safe_quit(driver) -> None:
    """
    driver.quit() と、必要なら一時 user-data-dir の削除も行う。
    """
    try:
        driver.quit()
    finally:
        tmp = getattr(driver, "_tmp_user_data_dir", None)
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


# =========================
# personal（個人出品）向け：一覧抽出
# =========================
def extract_item_listings(driver):
    """
    （personal用）
    一覧から (item_id, title, price) を抽出。
    VPS / headless でも「必ず戻る」安全版。
    """
    import time, re
    from selenium.common.exceptions import (
        StaleElementReferenceException,
        WebDriverException,
        TimeoutException,
    )
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    items, seen = [], set()

    MAX_ANCHORS = 200
    TIMEOUT_SEC = 10

    start = time.time()

    # ★ 最大の修正ポイント（shops と同じ）
    try:
        anchors = WebDriverWait(driver, 10).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "a[href*='/item/m']")
        )
    except (TimeoutException, WebDriverException):
        return items

    for a in anchors[:MAX_ANCHORS]:

        # 時間上限
        if time.time() - start > TIMEOUT_SEC:
            break

        try:
            href = a.get_attribute("href") or ""
            m = re.search(r"/item/(m\d{8,})", href)
            if not m:
                continue
            iid = m.group(1)
            if iid in seen:
                continue
            seen.add(iid)

            raw_title = (a.get_attribute("aria-label") or a.text or "").strip()
            clean_title = re.sub(r"^¥\s?[\d,]+\s*", "", raw_title).strip()

            price = None
            price_elem = a.find_elements(
                By.CSS_SELECTOR,
                "span[class*='number'], [data-testid*='price']",
            )
            if price_elem:
                txt = (price_elem[0].get_attribute("innerText") or "").strip()
                txt = re.sub(r"[^\d]", "", txt)
                if txt.isdigit():
                    price = int(txt)

            items.append((iid, clean_title, price))

        except (StaleElementReferenceException, WebDriverException):
            continue

    return items



def scroll_until_stagnant_collect_items(driver, pause: float, stagnant_times: int = 3):
    """
    （personal用）
    伸びなくなるまでスクロールして (item_id, title, price) を“1ページ分取り切って”返す。
    """
    last_len = 0
    stagnant = 0
    while True:
        print("[E] scrolling...", flush=True)
        time.sleep(pause + random.uniform(0.15, 0.35))
        items = extract_item_listings(driver)
        cur_len = len(items)

        if cur_len <= last_len:
            stagnant += 1
        else:
            stagnant = 0

        if stagnant >= stagnant_times:
            return items  # 伸びが止まった＝1ページ取り切った

        last_len = cur_len
        try:
            driver.execute_script(
                "window.scrollBy(0, Math.floor(window.innerHeight*0.9));"
            )
        except Exception:
            return items


# =========================
# Shops（ショップ出品）向け：一覧抽出
# =========================
def extract_shops_listings(driver):
    """
    （shops用）
    一覧から (product_id, title, price) を抽出。
    ・進捗がある限りは処理を続ける
    ・無進捗が一定時間続いたら打ち切る
    ・必ず list を返す（None は返さない）
    """
    import time, re
    from selenium.common.exceptions import (
        StaleElementReferenceException,
        WebDriverException,
        TimeoutException,
    )
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    items, seen = [], set()

    MAX_ANCHORS = 200
    TIMEOUT_SEC = 10  # 無進捗タイムアウト

    last_progress = time.time()   # ★最後に items が増えた時刻

    try:
        anchors = WebDriverWait(driver, 10).until(
            lambda d: d.find_elements(
                By.CSS_SELECTOR, "a[href*='/shops/product/']"
            )
        )
    except (TimeoutException, WebDriverException):
        return items

    for a in anchors[:MAX_ANCHORS]:
        try:
            href = a.get_attribute("href") or ""
            m = re.search(r"/shops/product/([A-Za-z0-9]+)", href)
            if not m:
                continue

            pid = m.group(1)
            if pid in seen:
                continue

            seen.add(pid)

            raw_title = (a.get_attribute("aria-label") or a.text or "").strip()
            clean_title = re.sub(r"^¥\s?[\d,]+\s*", "", raw_title).strip()

            price = None
            price_elem = a.find_elements(
                By.CSS_SELECTOR,
                "[data-testid*='price'], span[class*='number']",
            )
            if price_elem:
                txt = (price_elem[0].get_attribute("innerText") or "").strip()
                txt = re.sub(r"[^\d]", "", txt)
                if txt.isdigit():
                    price = int(txt)

            items.append((pid, clean_title, price))

            # ★進捗があったので時刻を更新
            last_progress = time.time()

        except (StaleElementReferenceException, WebDriverException):
            pass

        # ★無進捗 TIMEOUT 判定
        if time.time() - last_progress > TIMEOUT_SEC:
            break

    return items


def scroll_until_stagnant_collect_shops(driver, pause: float, stagnant_times: int = 3):
    """
    （shops用）
    伸びなくなるまでスクロールして (product_id, title, price) を
    「1ページ分取り切って」返す。

    ・進捗（件数増加）がある限りは待つ
    ・一定時間「無進捗」が続いたら打ち切る
    ・必ず list を返す（None は返さない）
    """
    last_len = 0
    stagnant = 0
    MAX_SCROLL = 20

    TIMEOUT_SEC = 30
    last_progress = time.time()   # ★最後に進捗があった時刻

    items = []  # ★必ず初期化しておく（最終 return 用）

    for i in range(MAX_SCROLL):
        print("[E] scrolling...", flush=True)
        time.sleep(pause + random.uniform(0.15, 0.35))

        items = extract_shops_listings(driver)
        print(f"[E] extracted items={len(items)}", flush=True)


        cur_len = len(items)

        # --- 進捗判定 ---
        if cur_len > last_len:
            last_progress = time.time()   # ★進捗あり
            stagnant = 0
            last_len = cur_len
        else:
            stagnant += 1

        # --- 伸びが止まった判定 ---
        if stagnant >= stagnant_times:
            return items

        # --- 無進捗 TIMEOUT 判定 ---
        if time.time() - last_progress > TIMEOUT_SEC:
            return items

        try:
            driver.execute_script(
                "window.scrollBy(0, Math.floor(window.innerHeight * 0.9));"
            )
        except Exception:
            return items

    return items  # ★MAX_SCROLL 到達時も必ず返す
