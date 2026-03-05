import time
import glob
import os
import re
import pandas as pd
import win32com.client as win32
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from apps.common.utils import (
    get_sql_server_connection,
)

def build_driver_for_ebay_csv():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()

    # headless禁止
    # opts.add_argument("--headless=new") ← 使わない

    # 画像ON（重要）
    # opts.add_argument("--blink-settings=imagesEnabled=false") ← 使わない

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=opts)

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver

# ==========================================
# アカウント取得
# ==========================================
def get_accounts():
    conn = get_sql_server_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT account, username, [password]
        FROM mst.ebay_accounts
    """)

    rows = cur.fetchall()
    conn.close()
    return rows


# ==========================================
# ログイン（必要時のみ）
# ==========================================
from selenium.common.exceptions import NoSuchElementException
import time

def ensure_login(driver, username, password):

    driver.get("https://signin.ebay.com/signin/")
    time.sleep(3)

    print("ログイン画面確認中...")

    # 最大5分待機
    timeout = time.time() + 300

    while time.time() < timeout:

        # ① userid があれば通常ログイン
        try:
            user_input = driver.find_element(By.ID, "userid")
            user_input.clear()
            user_input.send_keys(username)

            driver.find_element(By.ID, "signin-continue-btn").click()
            time.sleep(2)

            pwd_input = driver.find_element(By.XPATH, "//input[@type='password']")
            pwd_input.clear()
            pwd_input.send_keys(password)

            driver.find_element(By.ID, "sgnBt").click()
            print("ログイン実行")
            time.sleep(5)
            return

        except NoSuchElementException:
            pass

        # ② CAPTCHA判定
        if "verify" in driver.page_source.lower():
            print("CAPTCHA発生 → 手動で対応してください")
            time.sleep(5)
            continue

        time.sleep(2)

    raise Exception("ログイン画面に到達できませんでした")


def get_account_snapshot(driver):

    driver.get("https://www.ebay.com/sh/ovw")
    time.sleep(5)

    result = {
        "free_left": None,
        "listed_count": None,
        "amount_used": None,
        "amount_left": None
    }

    try:
        # =========================
        # ① Premium Free Left
        # =========================
        promo_text = driver.find_element(
            By.XPATH,
            "//*[contains(text(),'Used/Left')]"
        ).text

        # 例: Used/Left: 6,646 / 3,354
        import re
        m = re.search(r'(\d{1,3}(?:,\d{3})*)\s*/\s*(\d{1,3}(?:,\d{3})*)', promo_text)

        if m:
            result["free_left"] = int(m.group(2).replace(",", ""))

    except:
        pass

    try:
        # =========================
        # ② 出品数
        # =========================
        qty_text = driver.find_element(
            By.XPATH,
            "//*[contains(text(),'listed and sold') and contains(text(),'limit on quantity')]"
        ).text

        # 例: 5,094 listed and sold / 100,000 limit on quantity of items
        m = re.search(r'(\d{1,3}(?:,\d{3})*)\s+listed', qty_text)
        if m:
            result["listed_count"] = int(m.group(1).replace(",", ""))

    except:
        pass

    try:
        # =========================
        # ③ 空き金額
        # =========================
        money_left_text = driver.find_element(
            By.XPATH,
            "//*[contains(text(),'more') and contains(text(),'$')]"
        ).text

        # 例: $2,430.15 more
        m = re.search(r'\$([\d,]+\.\d+)', money_left_text)
        if m:
            result["amount_left"] = float(m.group(1).replace(",", ""))

    except:
        pass

    try:
        # =========================
        # ④ 使用金額
        # =========================
        amount_text = driver.find_element(
            By.XPATH,
            "//*[contains(text(),'listed and sold') and contains(text(),'limit')]"
        ).text

        # 例: $2.4M listed and sold / $2.4M limit
        m = re.search(r'\$([\d\.]+)M\s+listed', amount_text)

        if m:
            result["amount_used"] = float(m.group(1)) * 1_000_000

    except:
        pass

    return result


# ==========================================
# Excel書き込み
# ==========================================
def write_excel(account_name,
                free_left,
                listed_count,
                amount_used,
                amount_left):

    excel = win32.Dispatch("Excel.Application")
    excel.Visible = False

    wb = excel.Workbooks.Open(r"Y:\ebay\ebay.xlsx")
    ws = wb.Sheets("アカウント別")

    for col in range(1, 20):
        if ws.Cells(1, col).Value == account_name:

            ws.Cells(2, col).Value = free_left
            ws.Cells(3, col).Value = listed_count
            ws.Cells(4, col).Value = amount_used
            ws.Cells(5, col).Value = amount_left

            break

    wb.Save()
    wb.Close()
    excel.Quit()



# ==========================================
# レポートDL
# ==========================================
def download_report(driver, download_dir):

    wait = WebDriverWait(driver, 30)

    driver.get("https://www.ebay.com/sh/reports/downloads")

    # ───────── RefID取得待機 ─────────
    wait.until(
        EC.presence_of_element_located((By.XPATH, "//td[3]//div"))
    )

    ref_elements = driver.find_elements(By.XPATH, "//td[3]//div")
    initial_ref = None
    for el in ref_elements:
        txt = el.text.strip()
        if txt.isdigit() and len(txt) >= 8:
            initial_ref = txt
            break

    if not initial_ref:
        raise Exception("初期RefID取得失敗")

    # ───────── Download report ─────────
    download_btn = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'Download report')]")
        )
    )
    download_btn.click()

    # ───────── LISTINGS ─────────
    listings_radio = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//label[contains(@for,'LISTINGS')]")
        )
    )
    listings_radio.click()

    # ───────── ALL ACTIVE ─────────
    all_active_radio = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//label[contains(@for,'ALL_LISTINGS')]")
        )
    )
    all_active_radio.click()

    # ───────── 最終Download ─────────
    final_download = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//*[@id='reports-bam']//button[contains(.,'Download')]")
        )
    )
    final_download.click()

    # ───────── 新RefID検出 ─────────
    timeout = time.time() + 30
    new_ref = None

    while time.time() < timeout:
        ref_elements = driver.find_elements(By.XPATH, "//td[3]//div")
        for el in ref_elements:
            txt = el.text.strip()
            if txt.isdigit() and len(txt) >= 8:
                if txt != initial_ref:
                    new_ref = txt
                    break
        if new_ref:
            break
        time.sleep(1)

    if not new_ref:
        raise Exception("新RefID検出失敗")

    # ───────── CSV検出 ─────────
    timeout = time.time() + 180
    while time.time() < timeout:
        for f in os.listdir(download_dir):
            if new_ref in f and f.endswith(".csv"):
                return os.path.join(download_dir, f)
        time.sleep(1)

    raise Exception("CSV未検出")




# ==========================================
# 新規CSV特定
# ==========================================
def get_new_csv(before_files):

    path = r"C:\Users\stani\Downloads\*.csv"
    after_files = set(glob.glob(path))

    new_files = list(after_files - before_files)

    if not new_files:
        raise Exception("新しいCSVが見つかりません")

    return new_files[0]


# ==========================================
# SQL書込（高速版）
# ==========================================
def write_to_sql(account, df):

    conn = get_sql_server_connection()
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM ext.ebay_active_download
        WHERE account = ?
    """, account)

    conn.commit()

    data = [
        (
            account,
            row["Item number"],
            row["Custom label (SKU)"],
            row["Title"],
            row.get("Watchers", 0),
            row["Start price"],
        )
        for _, row in df.iterrows()
    ]

    cur.executemany("""
        INSERT INTO ext.ebay_active_download
        (account, listing_id, vendor_item_id, title_en, watchers, [Start price])
        VALUES (?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()


# ==========================================
# メイン
# ==========================================
def main():

    accounts = get_accounts()

    for account, username, password in accounts:

        print(f"==== {account} 開始 ====")

        driver = build_driver_for_ebay_csv()

        ensure_login(driver, username, password)

        # CSVだけ
        before_files = set(glob.glob(r"C:\Users\stani\Downloads\*.csv"))

        download_dir = r"C:\Users\stani\Downloads"
        download_report(driver, download_dir)

        csv_file = get_new_csv(before_files)

        df = pd.read_csv(csv_file)

        write_to_sql(account, df)

        driver.quit()

        print(f"==== {account} 完了 ====")


if __name__ == "__main__":
    main()
