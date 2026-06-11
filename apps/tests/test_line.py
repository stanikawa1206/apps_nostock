import requests

TOKEN = "Shdz82NrcFpbUirZ57RcFWDr8cHaP84QN4LOTiXGwnm0nQHpChPzOJ3J/G6H1Y/IDllje+wiDPSQ0diuYIN5Iau04MwMov89AIg9YSRdCGyQ3ByW7JL/plDYSEe4NutFqM07npe1gxSF+cYocFOduQdB04t89/1O/w1cDnyilFU="


headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

payload = {
    "messages": [
        {
            "type": "text",
            "text": "プログラムからのテスト配信です。このラインメッセージが届きましたら、お知らせください"
        }
    ]
}

r = requests.post(
    "https://api.line.me/v2/bot/message/broadcast",
    headers=headers,
    json=payload,
)

print(r.status_code)
print(r.text)