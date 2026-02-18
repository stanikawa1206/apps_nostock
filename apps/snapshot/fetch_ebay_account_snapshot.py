import time
import os
import glob
import pandas as pd
import pyodbc
import win32com.client as win32
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


# ==========================================
# DB接続
# ==========================================
def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.100.105,1433;"
        "DATABASE=nostock;"
        "UID=sa;"
        "PWD=tani6021;"
    )


# ==========================================
# アカウント取得
# ==========================================
def get_accounts():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT account_name, username, [password]
        FROM mst.ebay_accounts
        WHERE is_active = 1
    """)

    rows = cur.fetchall()
    conn.close()
    return rows


# ==========================================
# Selenium起動
# ==========================================
def create_driver():

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument(
        r"--user-data-dir=C:\Users\stani\AppData\Local\Google\Chrome\User Data"
    )

    driver = webdriver.Chrome(options=chrome_options)
    return driver


# ==========================================
# ログイン
# ==========================================
def login(driver, username, password):

    driver.get("https://signin.ebay.com/signin/")
    time.sleep(3)

    try:
        user_input = driver.find_element(By.ID, "userid")
        user_input.clear()
        user_input.send_keys(username)
        driver.find_element(By.ID, "signin-continue-btn").click()
        time.sleep(2)
    except:
        pass

    try:
        pwd_input = driver.find_element(By.XPATH, "//input[@type='password']")
        pwd_input.clear()
        pwd_input.send_keys(password)
        driver.find_element(By.ID, "sgnBt").click()
    except:
        pass

    print("CAPTCHAあれば手動対応してください")
    time.sleep(10)


# ==========================================
# Left取得（Premium Store）
# ==========================================
def get_store_left(driver):

    driver.get("https://www.ebay.com/sh/ovw")
    time.sleep(5)

    page = driver.page_source

    import re

    # Used/Left抽出
    match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*/\s*(\d{1,3}(?:,\d{3})*)', page)

    if match:
        used = int(match.group(1).replace(",", ""))
        left = int(match.group(2).replace(",", ""))
        return used, left

    return None, None


# ==========================================
# Excel書き込み
# ==========================================
def write_excel(account_name, used, left):

    excel = win32.Dispatch("Excel.Application")
    wb = excel.Workbooks.Open(r"C:\path\to\your\excel.xlsx")
    ws = wb.Sheets("アカウント別")

    # account_nameを1行目から探す
    for col in range(1, 20):
        if ws.Cells(1, col).Value == account_name:
            ws.Cells(2, col).Value = used
            ws.Cells(3, col).Value = left
            break

    wb.Save()
    wb.Close()
    excel.Quit()


# ==========================================
# レポートDL
# ==========================================
def download_report(driver):

    driver.get("https://www.ebay.com/sh/reports/downloads")
    time.sleep(5)

    driver.find_element(By.XPATH, "//button[text()='Download report']").click()
    time.sleep(2)

    driver.find_element(By.XPATH, "//label[contains(@for,'LISTINGS')]").click()
    time.sleep(1)

    driver.find_element(
        By.XPATH,
        "//label[contains(@for,'ALL_LISTINGS')]"
    ).click()

    time.sleep(1)

    driver.find_element(
        By.XPATH,
        "//*[@id='reports-bam']/div/div[2]/div[4]/button[2]"
    ).click()

    print("CSVダウンロード待機...")
    time.sleep(15)


# ==========================================
# 最新CSV取得
# ==========================================
def get_latest_csv():

    path = r"C:\Users\stani\Downloads\*.csv"
    files = glob.glob(path)
    latest_file = max(files, key=os.path.getctime)
    return latest_file


# ==========================================
# SQL書込
# ==========================================
def write_to_sql(account_name, df):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM ext.ebay_active_download
        WHERE account = ?
    """, account_name)

    conn.commit()

    for _, row in df.iterrows():

        cur.execute("""
            INSERT INTO ext.ebay_active_download
            (account, listing_id, vendor_item_id, title_en, watchers, [Start price])
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            account_name,
            row["Item number"],
            row["Custom label (SKU)"],
            row["Title"],
            row.get("Watchers", 0),
            row["Start price"]
        )

    conn.commit()
    conn.close()


# ==========================================
# メイン
# ==========================================
def main():

    accounts = get_accounts()

    for account_name, username, password in accounts:

        print(f"==== {account_name} 開始 ====")

        driver = create_driver()

        login(driver, username, password)

        # ① Left取得
        used, left = get_store_left(driver)
        print("Used:", used, "Left:", left)

        write_excel(account_name, used, left)

        # ② CSV→SQL
        download_report(driver)
        csv_file = get_latest_csv()
        df = pd.read_csv(csv_file)
        write_to_sql(account_name, df)

        driver.quit()

        print(f"==== {account_name} 完了 ====")


if __name__ == "__main__":
    main()
