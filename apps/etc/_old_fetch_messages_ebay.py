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

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from apps.adapters.ebay_api import get_access_token_new
from apps.common.utils import get_sql_server_connection
from openai import OpenAI

TRADING_API_URL = "https://api.ebay.com/ws/api.dll"

# ============================================================
# 1. メッセージ取得
# ============================================================
def fetch_messages(account: str, status: str, start: datetime) -> list[dict]:
    token = get_access_token_new(account)
    if not token:
        print(f"  ❌ token取得失敗: {account}")
        return []

    now = datetime.now(timezone.utc)

    xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<GetMemberMessagesRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials>
    <eBayAuthToken>{token}</eBayAuthToken>
  </RequesterCredentials>
  <MailMessageType>All</MailMessageType>
  <MessageStatus>{status}</MessageStatus>
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

    r.encoding = "utf-8"  # requests のデフォルト誤検知(ISO-8859-1)を上書き
    return _parse_messages(r.content)


def _parse_messages(xml_bytes: bytes) -> list[dict]:
    ns   = {"e": "urn:ebay:apis:eBLBaseComponents"}
    # バイト列のままET に渡す → XML宣言の encoding="UTF-8" をETが直接解釈
    root = ET.fromstring(xml_bytes)

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

        listing_id    = item.findtext("e:ItemID", namespaces=ns) if item is not None else None
        item_title = item.findtext("e:Title",  namespaces=ns) if item is not None else None
        q_id       = question.findtext("e:MessageID", namespaces=ns)

        # バイヤーのメッセージ（Question）
        messages.append({
            "message_id":  q_id,
            "sender_id":   question.findtext("e:SenderID",  namespaces=ns),
            "listing_id":     listing_id,
            "item_title":  item_title,
            "body_text":   question.findtext("e:Body",       namespaces=ns),
            "received_at": exchange.findtext("e:CreationDate", namespaces=ns),
            "direction":   "buyer",
        })

        # セラーの返信（Response）
        response = exchange.find("e:Response", ns)
        if response is not None:
            resp_id = response.findtext("e:MessageID", namespaces=ns) or f"{q_id}_r"
            body    = response.findtext("e:Text", namespaces=ns) or response.findtext("e:Body", namespaces=ns)
            messages.append({
                "message_id":  resp_id,
                "sender_id":   response.findtext("e:SenderID", namespaces=ns),
                "listing_id":     listing_id,
                "item_title":  item_title,
                "body_text":   body,
                "received_at": response.findtext("e:CreationDate", namespaces=ns),
                "direction":   "seller",
            })

    print(f"  📨 取得件数: {len(messages)}")
    return messages

# ============================================================
# 2. メッセージ分類
# ============================================================
REPLY_TEMPLATES = {
    "price_negotiation": {
        "reply_en": "Thank you for your interest. We only offer small discounts, and large reductions are unlikely. If you are seriously considering purchasing, please let me know your best offer.",
        "reply_ja": "ご興味をお持ちいただきありがとうございます。小幅な値引きのみ対応しており、大幅な値下げは難しい状況です。ご購入をご検討でしたら、ご希望の金額をお知らせください。"
    },
    "price_negotiation_large": {
        "reply_en": "Thank you for your offer. I appreciate your interest, but this discount is too large to consider. I'm open to reasonable offers, but this one is far below the item's value. Thank you for your understanding.",
        "reply_ja": "ご提案ありがとうございます。ご興味をお持ちいただき嬉しく思いますが、この値引き幅は大きすぎてお受けすることができません。合理的なご提案であれば検討いたしますが、今回のご提案は商品の価値を大きく下回っております。ご理解のほどよろしくお願いいたします。"
    },
    "rude_offer": {
        "reply_en": "Thank you for your message. Unfortunately, the offer you proposed is significantly below our acceptable range. Therefore, we are completely unable to accommodate any discount requests for this transaction.",
        "reply_ja": "メッセージありがとうございます。ご提案いただいた金額は当店の許容範囲を大きく下回っております。そのため、今回のお取引では値引きのご要望には一切お答えできかねます。"
    }
}


def _get_listing_price(listing_id: str):
    """trx.listings から出品価格を取得"""
    if not listing_id:
        return None
    try:
        cn  = get_sql_server_connection()
        cur = cn.cursor()
        cur.execute("""
            SELECT TOP 1 start_price FROM trx.listings
            WHERE listing_id = ?
            ORDER BY is_deleted ASC, deleted_at DESC
        """, listing_id)
        row = cur.fetchone()
        cur.close(); cn.close()
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def classify_message(body: str, listing_id: str = None) -> dict:
    client = OpenAI()

    # Step 1: 価格交渉かどうかを判別
    try:
        resp1 = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=50,
            messages=[{"role": "user", "content":
                'Classify the following eBay buyer message into one category.\n'
                'Reply in JSON only: {"category": "price_negotiation" or "other"}\n\n'
                f'Message: {body}'
            }]
        )
        cat = json.loads(resp1.choices[0].message.content or "{}").get("category", "other")
    except Exception:
        return {"type": "needs_review", "sub_type": None}

    if cat != "price_negotiation":
        return {"type": "needs_review", "sub_type": None}

    # Step 2: 具体的な金額オファーを抽出
    try:
        resp2 = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=100,
            messages=[{"role": "user", "content":
                'Analyze this eBay buyer message about price negotiation.\n\n'
                '1. Does the message contain a specific price offer? (yes/no)\n'
                '2. If yes, what is the offered price in USD? (number only)\n\n'
                'Reply JSON only:\n'
                '{"has_price": true/false, "offered_price": number or null}\n\n'
                f'Message: {body}'
            }]
        )
        p2 = json.loads(resp2.choices[0].message.content or "{}")
        has_price     = p2.get("has_price", False)
        offered_price = p2.get("offered_price")
    except Exception:
        return {"type": "price_negotiation", "sub_type": None}

    if not has_price or offered_price is None:
        return {"type": "price_negotiation", "sub_type": None}

    # Step 3: 出品価格と比較してカテゴリを決定
    start_price = _get_listing_price(listing_id)
    if not start_price or start_price <= 0:
        return {"type": "price_negotiation", "sub_type": None}

    ratio = float(offered_price) / float(start_price)
    if ratio >= 0.8:
        return {"type": "needs_review", "sub_type": None}
    elif ratio >= 0.5:
        return {"type": "price_negotiation", "sub_type": None}
    else:
        return {"type": "rude_offer", "sub_type": None}


# ============================================================
# 3. 日本語訳生成
# ============================================================
def generate_translation(body: str) -> str:
    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=1000,
        messages=[{"role": "user", "content": f"以下のeBayバイヤーからのメッセージを日本語に翻訳してください。翻訳文のみを返してください。\n\n{body}"}]
    )
    return response.choices[0].message.content.strip()


# ============================================================
# 4. DB保存
# ============================================================
def save_message(cur, account: str, msg: dict, category: dict = None, body_text_ja: str = None):
    cur.execute("SELECT 1 FROM trx.ebay_messages WHERE message_id = ?", msg["message_id"])
    if cur.fetchone():
        print(f"  ⏭ SKIP (既存): {msg['message_id']}")
        return False

    cur.execute("""
        INSERT INTO trx.ebay_messages (
            message_id, account, listing_id, item_title, sender_id,
            direction, received_at, body_text, body_text_ja,
            category, auto_reply_type, skip_reply
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """,
        msg["message_id"], account, msg["listing_id"], msg["item_title"], msg["sender_id"],
        msg["direction"], msg["received_at"], msg["body_text"], body_text_ja,
        category["type"] if category else None,
        category["sub_type"] if category else None,
        0,
    )
    return True


# ============================================================
# 5. メイン
# ============================================================
def _fetch_once(start: datetime = None):
    """全アカウントを1回だけフェッチして保存する共通処理"""
    cn  = get_sql_server_connection()
    cur = cn.cursor()

    if start is None:
        # 開始時刻 = DBの最新バイヤーメッセージ受信時刻 - 1時間
        cur.execute("SELECT MAX(received_at) FROM trx.ebay_messages WHERE direction = 'buyer'")
        row = cur.fetchone()
        max_received_at = row[0] if row and row[0] else None
        if max_received_at:
            # DBの時刻はUTCで保存されているがタイムゾーン情報なし → UTC付与
            if max_received_at.tzinfo is None:
                max_received_at = max_received_at.replace(tzinfo=timezone.utc)
            start = max_received_at - timedelta(hours=1)
        else:
            start = datetime.now(timezone.utc) - timedelta(hours=24)

    print(f"  取得開始時刻: {start.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    cur.execute("SELECT account FROM mst.ebay_accounts WHERE is_excluded = 0 ORDER BY account")
    accounts = [row[0] for row in cur.fetchall()]

    for account in accounts:
        print(f"[ACCOUNT] {account}")

        for status in ("Unanswered", "Answered"):
            print(f"  [{status}]")
            messages = fetch_messages(account, status, start)

            for msg in messages:
                if msg["direction"] == "seller":
                    saved = save_message(cur, account, msg)
                    if saved:
                        cn.commit()
                        print(f"  ✅ SAVED seller: {msg['message_id']}")
                else:
                    category     = classify_message(msg["body_text"] or "", listing_id=msg.get("listing_id"))
                    body_text_ja = generate_translation(msg["body_text"] or "")
                    saved = save_message(cur, account, msg, category, body_text_ja)
                    if saved:
                        cn.commit()
                        print(f"  ✅ SAVED buyer: {msg['message_id']} [{category['type']}]")

    cur.close()
    cn.close()


def run_once():
    """1回だけ実行して終了（--once オプション用）"""
    print(f"START (once): {datetime.now():%Y-%m-%d %H:%M:%S}")
    try:
        _fetch_once()
    except Exception as e:
        print(f"❌ ERROR: {e}")
    print(f"DONE: {datetime.now():%Y-%m-%d %H:%M:%S}")


def run_refetch():
    """2026-05-28 00:00:00 UTC 以降を全アカウントで一括再取得"""
    start = datetime(2026, 5, 28, 0, 0, 0, tzinfo=timezone.utc)
    print(f"START refetch from {start.strftime('%Y-%m-%dT%H:%M:%SZ')}: {datetime.now():%Y-%m-%d %H:%M:%S}")
    try:
        _fetch_once(start=start)
    except Exception as e:
        print(f"❌ ERROR: {e}")
    print(f"DONE: {datetime.now():%Y-%m-%d %H:%M:%S}")


def run():
    """無限ループで60秒ごとに実行"""
    print(f"START: {datetime.now():%Y-%m-%d %H:%M:%S}")
    while True:
        try:
            _fetch_once()
        except Exception as e:
            print(f"❌ ERROR: {e}")
        time.sleep(60)


def reclassify_existing():
    """既存の needs_review / price_negotiation レコードを一括再分類"""
    cn  = get_sql_server_connection()
    cur = cn.cursor()
    cur.execute("""
        SELECT message_id, listing_id, body_text
        FROM trx.ebay_messages
        WHERE direction = 'buyer'
          AND category IN ('needs_review', 'price_negotiation')
        ORDER BY received_at DESC
    """)
    rows = cur.fetchall()
    print(f"再分類対象: {len(rows)}件")
    for message_id, listing_id, body_text in rows:
        if not body_text:
            continue
        category = classify_message(body_text, listing_id=listing_id)
        cur.execute(
            "UPDATE trx.ebay_messages SET category = ?, auto_reply_type = ? WHERE message_id = ?",
            category["type"], category["sub_type"], message_id
        )
        cn.commit()
        print(f"  ✅ RECLASSIFIED: {message_id} → {category['type']}")
    cur.close(); cn.close()
    print("完了")


if __name__ == "__main__":
    import sys
    if "--reclassify" in sys.argv:
        print(f"START reclassify: {datetime.now():%Y-%m-%d %H:%M:%S}")
        reclassify_existing()
    elif "--refetch" in sys.argv:
        run_refetch()
    elif "--once" in sys.argv:
        run_once()
    else:
        run()