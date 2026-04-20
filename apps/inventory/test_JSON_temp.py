import requests

from apps.adapters.ebay_api import get_access_token_new


def dump_xml(account: str):

    token = get_access_token_new(account)
    if not token:
        print("❌ token取得失敗")
        return

    url = "https://api.ebay.com/ws/api.dll"

    headers = {
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "X-EBAY-API-IAF-TOKEN": token,
        "Content-Type": "text/xml"
    }

    body = """
    <?xml version="1.0" encoding="utf-8"?>
    <GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
        <ActiveList>
            <Pagination>
                <EntriesPerPage>20</EntriesPerPage>
                <PageNumber>1</PageNumber>
            </Pagination>
        </ActiveList>
    </GetMyeBaySellingRequest>
    """

    res = requests.post(url, headers=headers, data=body)

    # ★ 生XMLそのまま保存
    with open("debug_ebay.xml", "w", encoding="utf-8") as f:
        f.write(res.text)

    print("✅ XML保存完了: debug_ebay.xml")


if __name__ == "__main__":
    dump_xml("谷川②")