# -*- coding: utf-8 -*-
"""
get_refresh_token.py（eBay Production / OAuth: Authorization Code → Refresh Token）

【目的】
- eBayのProduction環境で、Authorization Code（code）を使って
  User Access Token（access_token）と Refresh Token（refresh_token）を取得する。
- refresh_token は約18か月（responseの refresh_token_expires_in）有効で、
  以後は refresh_token を使って access_token（有効2時間）を更新できる。

【重要な理解】
- 「Your branded eBay Production Sign In (OAuth)」の長い authorize URL は何度でも使える（認可画面を開くためのURL）。
- そこから発行される「code（Authorization Code）」は使い捨て（基本1回のみ有効＆短時間で失効）。
  同じ code を再利用すると invalid_grant になる。
- Webhook.site などで受け取る code はURLエンコードされていることが多いので、
  このスクリプトでは urllib.parse.unquote() でデコードしてから送信している（これが必須）。

【前提】
- .env を apps_nostock 直下（D:\\apps_nostock\\.env）に置く。
- .env に以下が登録されている（Production想定）:

  EBAY_CLIENT_ID=...
  EBAY_CLIENT_SECRET=...
  EBAY_TOKEN_URL=https://api.ebay.com/identity/v1/oauth2/token
  EBAY_RUNAME=（eBay Developer Portalで発行した RuName / Redirect URI name）

【このスクリプトがやること】
1) WEBHOOK_URL から code=... を抽出（正規表現）
2) code を unquote して「生の code（v^1.1#...）」に戻す
3) Basic認証（CLIENT_ID:CLIENT_SECRET を base64）で token endpoint にPOST
4) 成功すると access_token / refresh_token が返る

【運用手順（手動で refresh_token を新規発行したいとき）】
A. eBay Developer Program で authorize URL を作る
1) ブラウザで https://developer.ebay.com/ にログイン（Production）
2) 対象アプリ（App ID / Client ID が HRSPcomp-test-PRD-... のもの）を開く
3) User Tokens（または OAuth / Redirect URI設定）の画面へ
4) 「Your branded eBay Production Sign In (OAuth)」の長いURL（authorize URL）をコピー
   ※ redirect_uri（RuName）と scope が含まれるURL

B. authorize URL を開いて code を取得する
5) ブラウザの “アドレスバー” に authorize URL を貼り付けて開く（検索窓ではない）
6) refresh_token を発行したい eBayユーザーでログインして「同意/許可」を押す
7) すると redirect 先（例: Webhook.site）にリダイレクトされ、Request URL に code=... が付く
   例: https://webhook.site/xxxx?code=v%5E1.1%23i%5E1%23...&expires_in=299

C. このスクリプトを実行して refresh_token を取得する
8) Webhook.site の「Request URL（code=... が含まれているURL）」をコピー
9) このスクリプト内の WEBHOOK_URL に貼り付ける（毎回ここだけ差し替え）
10) スクリプトを実行する（同じ code は1回しか使えないので、実行は基本1回）
11) Status: 200 なら成功。Response JSON 内の refresh_token を保存する。

【保存の推奨】
- refresh_token は強力な鍵。Gitに上げない・ログに残さない・第三者に渡さない。
- 使う運用に合わせて .env へ保存する例:
  EBAY_REFRESH_TOKEN_TANIKAWA2=...
  EBAY_REFRESH_TOKEN_KAWASHIMA1=...

【失敗時の典型エラーと原因】
- invalid_grant:
  (1) code の再利用（既に使った / 期限切れ）
  (2) redirect_uri（RuName）の不一致（発行時と交換時が一致していない）
  (3) Production/Sandbox の取り違え
  (4) code がURLエンコードのまま送られている（unquoteしないと起きやすい）

【メモ】
- このスクリプトは「refresh_token を新規発行する」用途。
- 日常運用（access_token更新）は、別スクリプトで refresh_token grant を使うのが基本。
"""


import os
import requests
import base64
import re
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("EBAY_RUNAME")
TOKEN_URL = os.getenv("EBAY_TOKEN_URL")

print("=== ENV DEBUG ===")
print("CLIENT_ID:", CLIENT_ID)
print("CLIENT_SECRET:", "SET" if CLIENT_SECRET else "NONE")
print("REDIRECT_URI:", REDIRECT_URI)
print("TOKEN_URL:", TOKEN_URL)
print("=================")

WEBHOOK_URL = "https://webhook.site/dbdc2fbd-c77a-43a0-a5b4-9cdf0107d9b7?code=v%5E1.1%23i%5E1%23I%5E3%23p%5E3%23f%5E0%23r%5E1%23t%5EUl41XzM6MEQ3MDcwRDRDRUMzNzUyNDg2M0RENEI5OTZGQkUxRERfMl8xI0VeMjYw&expires_in=299"

match = re.search(r'code=([^&]+)', WEBHOOK_URL)
if not match:
    print("URLからcodeが見つかりませんでした。")
    exit()

auth_code = urllib.parse.unquote(match.group(1))

print("=== CODE DEBUG ===")
print("RAW CODE:", auth_code)
print("CODE LENGTH:", len(auth_code))
print("==================")

auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
encoded_auth = base64.b64encode(auth_str.encode()).decode()

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": f"Basic {encoded_auth}"
}

body = {
    "grant_type": "authorization_code",
    "code": auth_code,
    "redirect_uri": REDIRECT_URI
}

print("=== REQUEST DEBUG ===")
print("BODY:", body)
print("=====================")

response = requests.post(
    TOKEN_URL,
    headers=headers,
    data=body
)

print("Status:", response.status_code)
print("Response:", response.text)
