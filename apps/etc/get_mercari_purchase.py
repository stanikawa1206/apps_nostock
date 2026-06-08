import sys
sys.stdout.reconfigure(encoding="utf-8")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
import time

from apps.common.utils import get_sql_server_connection

PROFILE_DIR = r"D:\apps_nostock\profiles\mercari_firefox"

# status が以下の場合は人手入力とみなして上書きしない
STATUS_NO_OVERWRITE = ("GA鑑定待ち", "出荷済み", "◎有在庫")

SQL_UPSERT_VENDOR_PURCHASE = """
MERGE INTO trx.vendor_purchase WITH (HOLDLOCK) AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?)) AS src
    (vendor_name, vendor_item_id, purchase_datetime, purchase_price, status, item_name)
ON (tgt.vendor_name = src.vendor_name AND tgt.vendor_item_id = src.vendor_item_id)
WHEN MATCHED THEN
    UPDATE SET
        purchase_datetime = src.purchase_datetime,
        purchase_price    = src.purchase_price,
        status            = CASE
                                WHEN tgt.status IN (N'GA鑑定待ち', N'出荷済み', N'◎有在庫') THEN tgt.status
                                ELSE src.status
                            END,
        item_name         = src.item_name,
        updated_at        = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (vendor_name, vendor_item_id, purchase_datetime, purchase_price, status, item_name, updated_at)
    VALUES (src.vendor_name, src.vendor_item_id, src.purchase_datetime, src.purchase_price, src.status, src.item_name, GETDATE());
"""

SQL_UPSERT_VENDOR_MESSAGE = """
MERGE INTO trx.vendor_message WITH (HOLDLOCK) AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?)) AS src
    (vendor_name, vendor_item_id, message_no, sender_name, message_datetime_text, message_body)
ON (tgt.vendor_name = src.vendor_name
    AND tgt.vendor_item_id = src.vendor_item_id
    AND tgt.message_no = src.message_no)
WHEN MATCHED THEN
    UPDATE SET
        sender_name           = src.sender_name,
        message_datetime_text = src.message_datetime_text,
        message_body          = src.message_body,
        updated_at            = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (vendor_name, vendor_item_id, message_no, sender_name, message_datetime_text, message_body, updated_at)
    VALUES (src.vendor_name, src.vendor_item_id, src.message_no, src.sender_name, src.message_datetime_text, src.message_body, GETDATE());
"""


def get_vendor_item_id(url):
    return url.rstrip("/").split("/")[-1]


def parse_japanese_datetime(text):
    m = re.match(r"(\d+)年(\d+)月(\d+)日\s+(\d+):(\d+)", text.strip())
    if not m:
        raise ValueError(f"日時のパースに失敗: {text!r}")
    year, month, day, hour, minute = [int(x) for x in m.groups()]
    return datetime(year, month, day, hour, minute)


def get_raw_status(driver):
    """配送状態を取得する。発送前の場合は '発送前' を返す（購入済/連絡あり への分岐は呼び出し元で行う）"""
    waiting = driver.find_elements(
        By.XPATH,
        "//p[normalize-space(text())='発送をお待ちください']"
    )
    if waiting:
        return "発送前"

    progress_bar = driver.find_elements(
        By.CSS_SELECTOR,
        '[data-testid="transaction:shippingStatus.progressBar"]'
    )

    if not progress_bar:
        aside = driver.find_elements(
            By.CSS_SELECTOR,
            'aside[aria-label="受取評価が行われていません"]'
        )
        if aside:
            return "発送済み"
        raise RuntimeError("配送ステータス要素が見つかりません")

    current_step = progress_bar[0].find_elements(
        By.CSS_SELECTOR,
        '[aria-current="step"]'
    )
    if not current_step:
        raise RuntimeError("aria-current='step' の要素が見つかりません")

    step_text = current_step[0].text.strip()
    if not step_text:
        raise RuntimeError("aria-current='step' のテキストが空です")

    if step_text == "配達済み":
        return "☆出荷可能"

    return step_text


def get_item_name(driver):
    els = driver.find_elements(
        By.CSS_SELECTOR,
        '[data-testid="transaction:information-for-buyer.item-object-itemLabel"]'
    )
    if not els:
        raise RuntimeError("商品名要素が見つかりません (transaction:information-for-buyer.item-object-itemLabel)")
    return els[0].text.strip()


def get_purchase_info(driver):
    date_els = driver.find_elements(
        By.CSS_SELECTOR,
        '[data-partner-id="purchase-date"]'
    )
    if not date_els:
        raise RuntimeError("購入日時要素が見つかりません (data-partner-id='purchase-date')")
    purchase_datetime = parse_japanese_datetime(date_els[0].text.strip())

    price_els = driver.find_elements(
        By.CSS_SELECTOR,
        "span.number__6b270ca7"
    )
    if not price_els:
        raise RuntimeError("購入金額要素が見つかりません (span.number__6b270ca7)")
    price_text = price_els[0].text.strip().replace(",", "")
    if not price_text.isdigit():
        raise ValueError(f"購入金額のパースに失敗: {price_text!r}")
    purchase_price = int(price_text)

    return purchase_datetime, purchase_price


def get_messages(driver):
    """
    ds4-comment を全件取得する。
    is_from_seller は report-button の有無で判定する。
    出品者のメッセージには report-button が存在し、自分（購入者）のメッセージには存在しない。
    """
    comment_divs = driver.find_elements(
        By.CSS_SELECTOR,
        '[data-testid="ds4-comment"]'
    )

    messages = []

    for i, div in enumerate(comment_divs, start=1):

        name_els = div.find_elements(
            By.CSS_SELECTOR,
            'a[href^="/user/profile/"] > div > p'
        )
        if not name_els:
            raise RuntimeError(f"message_no={i}: sender_name要素が見つかりません (a[href^='/user/profile/'] > div > p)")
        sender_name = name_els[0].text.strip()

        time_els = div.find_elements(
            By.XPATH,
            ".//p[@data-testid='message-body']/../following-sibling::div/p/span"
        )
        if not time_els:
            raise RuntimeError(f"message_no={i}: message_datetime_text要素が見つかりません (message-body/../following-sibling::div/p/span)")
        message_datetime_text = time_els[0].text.strip()

        body_els = div.find_elements(
            By.CSS_SELECTOR,
            '[data-testid="message-body"]'
        )
        if not body_els:
            raise RuntimeError(f"message_no={i}: message_body要素が見つかりません (data-testid='message-body')")
        message_body = body_els[0].text.strip()

        is_from_seller = bool(div.find_elements(
            By.CSS_SELECTOR,
            '[data-testid="report-button"]'
        ))

        messages.append({
            "message_no": i,
            "sender_name": sender_name,
            "message_datetime_text": message_datetime_text,
            "message_body": message_body,
            "is_from_seller": is_from_seller,
        })

    return messages


def determine_status(raw_status, messages):
    """発送前の場合のみ出品者メッセージの有無で購入済/連絡あり に分岐する"""
    if raw_status != "発送前":
        return raw_status

    if any(msg["is_from_seller"] for msg in messages):
        return "連絡あり"

    return "購入済"


options = Options()
options.add_argument("-profile")
options.add_argument(PROFILE_DIR)

driver = webdriver.Firefox(options=options)
conn = get_sql_server_connection()

try:

    driver.get("https://jp.mercari.com/mypage/purchases")
    time.sleep(5)

    checkbox = driver.find_element(
        By.CSS_SELECTOR,
        '[data-testid="user-listing-inTransactionItemsCheckbox"]'
    )

    if not checkbox.is_selected():
        checkbox.click()
        time.sleep(3)

    links = driver.find_elements(By.TAG_NAME, "a")

    seen = set()
    transaction_urls = []

    for link in links:
        href = link.get_attribute("href")
        if href and "/transaction/" in href and href not in seen:
            seen.add(href)
            transaction_urls.append(href)

    print(f"取引URL数: {len(transaction_urls)}")
    print()

    for url in transaction_urls:

        try:

            driver.get(url)
            time.sleep(4)

            vendor_item_id   = get_vendor_item_id(url)
            raw_status       = get_raw_status(driver)
            item_name        = get_item_name(driver)
            purchase_datetime, purchase_price = get_purchase_info(driver)
            messages         = get_messages(driver)
            status           = determine_status(raw_status, messages)

            with conn.cursor() as cur:
                cur.execute(
                    SQL_UPSERT_VENDOR_PURCHASE,
                    ("mercari", vendor_item_id, purchase_datetime, purchase_price, status, item_name)
                )
                for msg in messages:
                    cur.execute(
                        SQL_UPSERT_VENDOR_MESSAGE,
                        (
                            "mercari",
                            vendor_item_id,
                            msg["message_no"],
                            msg["sender_name"],
                            msg["message_datetime_text"],
                            msg["message_body"],
                        )
                    )
            conn.commit()

            print(url)
            print(f"status={status}  price={purchase_price}  messages={len(messages)}  item={item_name[:30]}")
            print()

        except Exception as e:
            print(url)
            print(f"ERROR: {e}")
            print()

finally:
    driver.quit()
    conn.close()
