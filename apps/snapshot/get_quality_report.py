# デバッグ用：内部で呼ばれている関数を確認するコード
import requests
from apps.adapters.ebay_api import _fetch_access_token_from_refresh, _get_refresh_token,get_access_token_new

import base64
import json

def check_token_scope(token):
    # トークンの真ん中の部分を取り出す
    parts = token.split('.')
    if len(parts) != 3:
        print("これはJWT形式ではありません")
        return

    # Base64デコード（読める形式にする）
    # パディング調整（文字数が4の倍数でない場合のエラー回避）
    payload = parts[1] + '=' * (-len(parts[1]) % 4)
    decoded_payload = base64.b64decode(payload).decode('utf-8')
    
    # JSONとして表示
    data = json.loads(decoded_payload)
    
    print("--- トークンのスコープ情報 ---")
    print(data.get("scope", "スコープ情報が見つかりません"))

# 実行例
token = get_access_token_new("BUZZ") # 既存の関数
print(f"--- トークンの正体を確認 ---")
print(f"トークンの長さ: {len(token)}")
print(f"トークンの先頭50文字: {token[:50]}")
check_token_scope(token)