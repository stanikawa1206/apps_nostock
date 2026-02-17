import requests
import base64
import re
import urllib.parse

# 1. 認証情報
CLIENT_ID = "HRSPcomp-test-PRD-bfe43fae9-2313a335"
CLIENT_SECRET = "PRD-f02406089167-45bd-4421-9172-2b06"
REDIRECT_URI = "HRSP_company_li-HRSPcomp-test-P-vtqjmbxa"

# 2. Webhook.siteに届いた最新のURLを貼る
WEBHOOK_URL = "https://webhook.site/7004daf6-1158-418f-9d9a-34443427b4b7?code=v%5E1.1%23i%5E1%23p%5E3%23r%5E1%23f%5E0%23I%5E3%23t%5EUl41Xzk6NTUyNDY4MjQwM0FBMjlCOTY3MUNDM0Q3OEFGMzc1NzJfMV8xI0VeMjYw&expires_in=299"

# --- 内部処理 ---
match = re.search(r'code=([^&]+)', WEBHOOK_URL)
if not match:
    print("URLからcodeが見つかりませんでした。")
    exit()

# ★ポイント：一度デコードしてから、再度「eBayが受け取れる形」に整えます
auth_code = urllib.parse.unquote(match.group(1))

# Basic認証ヘッダー
auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
encoded_auth = base64.b64encode(auth_str.encode()).decode()

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": f"Basic {encoded_auth}"
}

# ★重要：requestsに任せず、手動でクエリ文字列を組み立てます
# これにより、変なエンコードがかかるのを防ぎます
body = (
    f"grant_type=authorization_code&"
    f"code={urllib.parse.quote(auth_code)}&"
    f"redirect_uri={REDIRECT_URI}"
)

print("--- 最終リクエスト送信 ---")
# data=body (文字列) として送るのがミソです
response = requests.post(
    "https://api.ebay.com/identity/v1/oauth2/token",
    headers=headers,
    data=body 
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")