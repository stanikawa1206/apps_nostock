# apps/etc/fetch_messages_ebay.py
# 先頭部分を変更
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[2] / ".env")  # プロジェクトルートの.envを明示

import time
import requests
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import get_sql_server_connection
import anthropic

TRADING_API_URL = "https://api.ebay.com/ws/api.dll"

# ============================================================
# 1. メッセージ取得
# ============================================================
def fetch_messages(account: str) -> list[dict]:
    token = get_access_token_new(account)
    if not token:
        print(f"  ❌ token取得失敗: {account}")
        return []

    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)

    xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<GetMemberMessagesRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials>
    <eBayAuthToken>{token}</eBayAuthToken>
  </RequesterCredentials>
  <MailMessageType>All</MailMessageType>
  <MessageStatus>Unanswered</MessageStatus>
  <StartCreationTime>{start.strftime("%Y-%m-%dT%H:%M:%S.000Z")}</StartCreationTime>
  <EndCreationTime>{now.strftime("%Y-%m-%dT%H:%M:%S.000Z")}</EndCreationTime>
</GetMemberMessagesRequest>"""

    headers = {
        "Content-Type":                   "text/xml",
        "X-EBAY-API-CALL-NAME":           "GetMemberMessages",
        "X-EBAY-API-SITEID":              "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "X-EBAY-API-IAF-TOKEN":           token,
    }

    r = requests.post(TRADING_API_URL, data=xml_body.encode("utf-8"),
                      headers=headers, timeout=30)

    if r.status_code != 200:
        print(f"  ❌ API error: {r.status_code}")
        return []

    return _parse_messages(r.text)


def _parse_messages(xml_text: str) -> list[dict]:
    ns   = {"e": "urn:ebay:apis:eBLBaseComponents"}
    root = ET.fromstring(xml_text)

    ack = root.findtext("e:Ack", namespaces=ns)
    if ack not in ("Success", "Warning"):
        for err in root.findall(".//e:Errors/e:LongMessage", ns):
            print(f"  ⚠️  {err.text}")
        return []

    messages = []
    for exchange in root.findall(".//e:MemberMessageExchange", ns):
        item     = exchange.find("e:Item",     ns)
        question = exchange.find("e:Question", ns)
        if question is None:
            continue

        messages.append({
            "message_id":  question.findtext("e:MessageID", namespaces=ns),
            "sender_id":   question.findtext("e:SenderID",  namespaces=ns),
            "item_id":     item.findtext("e:ItemID",         namespaces=ns) if item else None,
            "item_title":  item.findtext("e:Title",          namespaces=ns) if item else None,
            "subject":     question.findtext("e:Subject",    namespaces=ns),
            "body_text":   question.findtext("e:Body",       namespaces=ns),
            "received_at": exchange.findtext("e:CreationDate", namespaces=ns),
        })

    print(f"  📨 取得件数: {len(messages)}")
    return messages

# ============================================================
# 2. メッセージ分類
# ============================================================
AUTO_REPLY_PATTERNS = {
    "price_negotiation": [
        "can you lower", "lower the price", "discount",
        "best price", "cheaper", "reduce", "any offer",
        "take less", "negotiate",
    ],
    "shipping_inquiry": [
        "how long", "shipping time", "when will",
        "tracking number", "delivery", "arrive",
    ],
    "item_condition": [
        "condition", "any defects", "scratches", "damage",
        "authentic", "genuine",
    ],
}

def classify_message(body: str) -> dict:
    body_lower = body.lower()
    for sub_type, keywords in AUTO_REPLY_PATTERNS.items():
        if any(kw in body_lower for kw in keywords):
            return {"type": "auto_reply", "sub_type": sub_type}
    return {"type": "needs_review", "sub_type": None}


# ============================================================
# 3. AI返信生成
# ============================================================
def generate_reply_draft(body: str, item_title: str = "") -> dict:
    client = anthropic.Anthropic()
    prompt = f"""You are a professional eBay seller based in Japan selling authentic Japanese items.

Item: {item_title}
Buyer message: {body}

Reply in JSON only (no markdown):
{{
  "reply_en": "English reply here",
  "reply_ja": "日本語訳をここに"
}}"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    try:
        return json.loads(response.content[0].text)
    except Exception:
        return {"reply_en": response.content[0].text, "reply_ja": "（解析エラー）"}


# ============================================================
# 4. DB保存
# ============================================================
def save_message(cur, account: str, msg: dict, category: dict, reply_draft: dict):
    cur.execute("SELECT 1 FROM trx.ebay_messages WHERE message_id = ?", msg["message_id"])
    if cur.fetchone():
        return False  # 重複スキップ

    cur.execute("""
        INSERT INTO trx.ebay_messages (
            message_id, account, item_id, sender_id,
            received_at, subject, body_text,
            category, auto_reply_type,
            reply_draft_en, reply_draft_ja
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """,
        msg["message_id"], account, msg["item_id"], msg["sender_id"],
        msg["received_at"], msg["subject"], msg["body_text"],
        category["type"], category["sub_type"],
        reply_draft.get("reply_en"), reply_draft.get("reply_ja")
    )
    return True


# ============================================================
# 5. メイン
# ============================================================
def run():
    print(f"START: {datetime.now():%Y-%m-%d %H:%M:%S}")

    while True:
        try:
            cn  = get_sql_server_connection()
            cur = cn.cursor()

            cur.execute("SELECT account FROM mst.ebay_accounts WHERE is_excluded = 0 ORDER BY account")
            accounts = [row[0] for row in cur.fetchall()]

            for account in accounts:
                print(f"[ACCOUNT] {account}")
                messages = fetch_messages(account)

                for msg in messages:
                    category = classify_message(msg["body_text"] or "")

                    if category["type"] == "auto_reply":
                        reply_draft = {"reply_en": None, "reply_ja": None}
                    else:
                        reply_draft = generate_reply_draft(
                            msg["body_text"] or "",
                            msg.get("item_title", "")
                        )

                    saved = save_message(cur, account, msg, category, reply_draft)
                    if saved:
                        cn.commit()
                        print(f"  ✅ SAVED: {msg['message_id']} [{category['type']}]")

            cur.close()
            cn.close()

        except Exception as e:
            print(f"❌ ERROR: {e}")

        time.sleep(60)


if __name__ == "__main__":
    run()