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
    opts.add_argument("--lang=ja-JP,ja")
    opts.add_argument("--disable-notifications")

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
    - item_id: /item/m12345678 の m～ を抽出
    - title  : 先頭の「¥ 123,456」を除去してクリーンにする
    - price  : 数字のみを抜いて int 化（取れないときは None）
    """
    items, seen = [], set()
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/item/m']")
    for a in anchors:
        href = a.get_attribute("href") or ""
        m = re.search(r"/item/(m\d{8,})", href)
        if not m:
            continue
        iid = m.group(1)
        if iid in seen:
            continue
        seen.add(iid)

        raw_title = (a.get_attribute("aria-label") or a.text or "").strip()
        # 先頭の価格（¥ 123,456）を除去
        clean_title = re.sub(r"^¥\s?[\d,]+\s*", "", raw_title).strip()

        # 価格（取得できない場合は None）
        price = None
        price_elem = a.find_elements(
            By.CSS_SELECTOR,
            "span[class*='number'], [data-testid*='price']",
        )
        if price_elem:
            txt = (price_elem[0].get_attribute("innerText") or price_elem[0].text or "").strip()
            txt = re.sub(r"[^\d]", "", txt)
            if txt.isdigit():
                price = int(txt)

        items.append((iid, clean_title, price))
    return items


def scroll_until_stagnant_collect_items(driver, pause: float, stagnant_times: int = 3):
    """
    （personal用）
    伸びなくなるまでスクロールして (item_id, title, price) を“1ページ分取り切って”返す。
    """
    last_len = 0
    stagnant = 0
    while True:
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
    - product_id: /shops/product/<ID> の末尾IDを抽出
    - title     : a要素の aria-label もしくはテキストを使う（先頭の価格行は除去）
    - price     : 「¥ 123,456」などから数値だけを抜いて int 化（取れないときは None）
    """
    items, seen = [], set()
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/shops/product/']")
    for a in anchors:
        href = a.get_attribute("href") or ""
        m = re.search(r"/shops/product/([A-Za-z0-9]+)", href)
        if not m:
            continue
        pid = m.group(1)
        if pid in seen:
            continue
        seen.add(pid)

        # タイトル（先頭に価格が載ってくるケースがあるので除去）
        raw_title = (a.get_attribute("aria-label") or a.text or "").strip()
        clean_title = re.sub(r"^¥\s?[\d,]+\s*", "", raw_title).strip()

        # 価格
        price = None
        price_elem = a.find_elements(
            By.CSS_SELECTOR,
            "[data-testid*='price'], span[class*='number']",
        )
        if price_elem:
            txt = (price_elem[0].get_attribute("innerText") or price_elem[0].text or "").strip()
            txt = re.sub(r"[^\d]", "", txt)
            if txt.isdigit():
                price = int(txt)

        items.append((pid, clean_title, price))
    return items


def scroll_until_stagnant_collect_shops(driver, pause: float, stagnant_times: int = 3):
    """
    （shops用）
    伸びなくなるまでスクロールして (product_id, title, price) を“1ページ分取り切って”返す。
    """
    last_len = 0
    stagnant = 0
    MAX_SCROLL = 20 

    start = time.time()
    TIMEOUT_SEC = 30  # まず30秒でOK（あとで調整）

    for i in range(MAX_SCROLL):
        if time.time() - start > TIMEOUT_SEC:
            break  # ←ここが修正②のキモ

        time.sleep(pause + random.uniform(0.15, 0.35))
        items = extract_shops_listings(driver)
        cur_len = len(items)

        if cur_len == last_len:
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
