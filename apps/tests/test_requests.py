from selenium import webdriver
import time

items = [
    "m81876639070",
    "m91872260315",
    "m89799124272"
]

driver = webdriver.Chrome()

driver.get("https://jp.mercari.com")

time.sleep(5)

for item_id in items:

    script = """
    const callback = arguments[arguments.length - 1];
    const id = arguments[0];

    fetch("https://jp.mercari.com/item/" + id)
      .then(r => r.text())
      .then(html => callback(html));
    """

    html = driver.execute_async_script(script, item_id)

    if "売り切れました" in html:
        print(item_id, "売り切れ")
    else:
        print(item_id, "販売中")

driver.quit()