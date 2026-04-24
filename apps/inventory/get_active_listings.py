import requests
import datetime
import xml.etree.ElementTree as ET

from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import get_sql_server_connection

    
# =========================
# Trading API取得
# =========================
def get_active_listings(account: str):

    token = get_access_token_new(account)
    if not token:
        print(f"❌ token取得失敗: {account}")
        return []

    url = "https://api.ebay.com/ws/api.dll"

    headers = {
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml"
    }

    all_items = []
    page = 1
    per_page = 200

    while True:
        print(f"  → page {page}")

        body = f"""
        <?xml version="1.0" encoding="utf-8"?>
        <GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
            <ActiveList>
                <Pagination>
                    <EntriesPerPage>{per_page}</EntriesPerPage>
                    <PageNumber>{page}</PageNumber>
                </Pagination>
            </ActiveList>
        </GetMyeBaySellingRequest>
        """

        res = requests.post(url, headers=headers, data=body)
        root = ET.fromstring(res.text)

        ns = {"e": "urn:ebay:apis:eBLBaseComponents"}

        items = root.findall(".//e:Item", ns)
        count = len(items)

        print(f"    件数: {count}")

        if count == 0:
            break

        for item in items:
            item_id = item.findtext("e:ItemID", default="", namespaces=ns)
            title = item.findtext("e:Title", default="", namespaces=ns)

            price = item.findtext(".//e:CurrentPrice", default="0", namespaces=ns)
            price = float(price)

            watch = item.findtext("e:WatchCount", default="0", namespaces=ns)
            watch = int(watch)

            start_time = item.findtext(".//e:StartTime", default=None, namespaces=ns)

            sku = item.findtext(".//e:SKU", default="", namespaces=ns)

            all_items.append({
                "item_id": item_id,
                "sku": sku,
                "title": title,
                "price": price,
                "watch_count": watch,
                "start_time": start_time
            })

        # ★ ここが修正ポイント
        if count < per_page:
            break

        page += 1

    print(f"  ✔ 合計: {len(all_items)}")
    return all_items

# =========================
# DB INSERT
# =========================
def insert_items(cursor, account, items, fetched_at):

    for item in items:
        cursor.execute("""
            INSERT INTO ext.ebay_active_download
            (
                account,
                vendor_item_id,
                listing_id,
                title_en,
                watchers,
                [Start price],
                start_time,
                fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            account,
            item["sku"],
            item["item_id"],
            item["title"],
            item["watch_count"],
            item["price"],
            item["start_time"],
            fetched_at
        )


# =========================
# メイン処理
# =========================
def run():

    conn = get_sql_server_connection()
    cursor = conn.cursor()

    print("🧹 テーブル初期化（DELETE）")
    # cursor.execute("DELETE FROM ext.ebay_active_download")
    cursor.execute(
        "DELETE FROM ext.ebay_active_download WHERE account IN (?, ?)",
        ("川島","谷川②")
    )
    conn.commit()

    fetched_at = datetime.datetime.now()

    # cursor.execute("SELECT account FROM mst.ebay_accounts")
    cursor.execute(
        "SELECT account FROM mst.ebay_accounts WHERE account IN (?, ?)",
        ("川島","谷川②")
    )
    accounts = [row[0] for row in cursor.fetchall()]

    for account in accounts:
        print(f"▶ 開始: {account}")

        items = get_active_listings(account)

        print(f"  件数: {len(items)}")

        insert_items(cursor, account, items, fetched_at)

        conn.commit()

    conn.close()
    print("✅ 完了")


if __name__ == "__main__":
    run()