from selenium import webdriver
import time

item_ids = [
    "m81876639070",
    "m91872260315",
    "m89799124272"
]

driver = webdriver.Chrome()

driver.get("https://jp.mercari.com")

time.sleep(3)

for item_id in item_ids:

    data = driver.execute_async_script("""
    const callback = arguments[arguments.length - 1];
    const id = arguments[0];

    fetch("https://api.mercari.jp/items/get?id=" + id, {
        headers: {
            "x-platform": "web",
            "x-app-version": "20260301"
        }
    })
    .then(r => r.json())
    .then(data => callback(data));
    """, item_id)

    print(data)

driver.quit()