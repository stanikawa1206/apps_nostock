# apps/etc/fetch_messages_ebay.py
# 先頭部分を変更
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).resolve().parents[2] / ".env")  # プロジェクトルートの.envを明示

import time
import requests
import json
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape
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
        "reply_en": "This offer would require my boss's approval.\n\nIf it gets approved, can you promise to complete the purchase?\n\nAlso, please note that we aim to ship within 5 business days after payment. I would appreciate it if you could confirm that this shipping timeframe is acceptable as well.\n\nIf you can confirm both, I'll do my best to negotiate with my boss for you.",
        "reply_ja": "こちらのご提案には上司の承認が必要となります。承認が下りた場合、必ずご購入いただけますでしょうか。また当店ではお支払い後5営業日以内の発送を目指しておりますので、この発送までの期間についてもご了承いただけるかあわせてご確認をお願いいたします。両方をご確認いただけましたら、上司との交渉に最善を尽くします。"
    },
    "price_negotiation_large": {
        "reply_en": "Thank you for your offer.\n\nWe generally only consider discounts of around 10% from the listed price, so unfortunately, your offer is lower than what we can accept.\n\nIf you are still interested, please feel free to make an offer closer to that range.",
        "reply_ja": "ご提案ありがとうございます。当店では基本的に、出品価格から約10%程度の値引きのみを検討しております。誠に恐れ入りますが、いただいたご提案はお受けできる範囲を下回っております。もしまだご興味をお持ちでしたら、その範囲に近いご提案を改めてお願いいたします。"
    },
    "rude_offer": {
        "reply_en": "Thank you for your message.\n\nWe do not continue negotiations with buyers who initially offer 50% or less of the listed price.\n\nThank you for your understanding.",
        "reply_ja": "メッセージありがとうございます。当店では、最初のご提案が出品価格の50%以下となるお客様とは値引き交渉を継続しておりません。ご理解のほどよろしくお願いいたします。"
    },
    # 具体的な希望価格がないvague("値引きできますか？"等)な price_negotiation 用の定型文。
    # message_viewer.py側のJS定型文(price_negotiation_vague)と同一内容にしてある。
    "price_negotiation_vague": {
        "reply_en": "Thank you for your interest.\n\nPlease let me know the price you have in mind, and I will see what I can do.",
        "reply_ja": "ご興味をお持ちいただきありがとうございます。ご希望の金額をお知らせいただけましたら、可能な範囲で検討いたします。"
    },
    # 真贋確認のみのメッセージ("Is this authentic?"「本物ですか？」等)用の定型文。
    # 現時点では自動送信の対象外(定型文セットのみ)。
    "authenticity_check": {
        "reply_en": "Yes, this item is authentic and genuine.\n\nThank you for your question.",
        "reply_ja": "はい、こちらの商品は本物・正規品です。ご質問ありがとうございます。"
    }
}


CATEGORY_GUIDE = {
    "price_negotiation":       "The buyer's requested discount is under 20% and specific. We generally lean toward accepting it, but before agreeing we must confirm the buyer will complete the purchase once approved and is fine with our ~5-business-day shipping timeframe after payment, and note that the offer still requires the boss's approval.",
    "price_negotiation_large": "The requested discount is 20% or more but under 50%. We generally only consider discounts of around 10% from the listed price, so this specific offer is below what we can accept; invite a smaller offer if the buyer is still interested.",
    "rude_offer":              "The requested price is 50% or less of the listed price. We do not continue negotiating with buyers whose initial offer is this low.",
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


def _parse_json_response(raw: str) -> dict:
    """GPTのJSON応答を安全にパースする。壊れていれば空dictを返す。"""
    try:
        data = json.loads(raw or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _extract_offer(body: str) -> dict:
    """
    Step2: Buyerメッセージが「何を要求しているか」の意味だけをGPTに分類させる。
    最終的な金額計算(offered_price/discount_rate)はここでは行わず、Python側(_compute_offer)に委ねる。
    """
    client = OpenAI()
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=100,
            messages=[{"role": "user", "content":
                'Analyze this eBay buyer message about price negotiation.\n'
                'Classify what the buyer is requesting into exactly one offer_type:\n'
                '- "absolute_price": buyer names a specific final price they want to pay\n'
                '- "amount_off": buyer requests a specific dollar amount to be discounted from the current price\n'
                '- "percent_off": buyer requests a specific percentage discount from the current price\n'
                '- "vague": no specific number or percentage is stated (e.g. "best price", "a discount", "lower the price")\n\n'
                'Also report "value": the number associated with that offer_type (the price, the amount off, or the '
                'percent off), as a plain number with no currency symbol. For "vague", value must be null.\n\n'
                'Also report "currency": "USD" if the message uses $, "USD", or a bare number with no currency stated '
                '(assume the listing\'s own currency in that case); "OTHER" if a different currency is explicitly used '
                '(e.g. €, £, EUR, GBP, JPY, etc.); "unknown" if it cannot be determined.\n\n'
                'Reply JSON only:\n'
                '{"offer_type": "absolute_price"|"amount_off"|"percent_off"|"vague", "value": number or null, '
                '"currency": "USD"|"OTHER"|"unknown"}\n\n'
                f'Message: {body}'
            }]
        )
        return _parse_json_response(resp.choices[0].message.content)
    except Exception:
        return {}


def _compute_offer(offer_type, value, currency, start_price):
    """
    GPTが返した「意味」(offer_type/value/currency)と実際のstart_priceから、
    Python側で確定的にoffered_price/discount_rateを計算する。
    値が信用できない・計算できない場合は (None, None) を返す。
    ここでは discount_rate を無理に確定させない(呼び出し側で安全側にフォールバックする)。
    """
    if offer_type not in ("absolute_price", "amount_off", "percent_off"):
        return None, None

    # 通貨が明示的にUSD以外の場合、為替換算の仕組みがないため計算しない
    currency_norm = currency.strip().upper() if isinstance(currency, str) else None
    if currency_norm == "OTHER":
        return None, None

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None, None
    if value < 0:
        return None, None
    if not start_price or start_price <= 0:
        return None, None

    start_price = float(start_price)
    value       = float(value)

    if offer_type == "absolute_price":
        offered_price = value
    elif offer_type == "amount_off":
        offered_price = start_price - value
    else:  # percent_off
        if value > 100:
            return None, None
        offered_price = start_price * (1 - value / 100)

    if offered_price < 0:
        return None, None

    # round: 二進浮動小数点誤差でちょうど20%/50%の境界がずれるのを防ぐ
    discount_rate = round(1 - (offered_price / start_price), 6)
    return offered_price, discount_rate


def _category_from_discount_rate(discount_rate: float) -> str:
    if discount_rate < 0.20:
        return "price_negotiation"
    elif discount_rate < 0.50:
        return "price_negotiation_large"
    else:
        return "rude_offer"


def analyze_price_negotiation(body: str, listing_id: str = None, start_price: float = None) -> dict:
    """
    Buyerメッセージを分析し、値引き交渉の構造化情報を返す。

    - 「意味の分類」(offer_type/value/currency) はGPTが行う
    - 「最終計算」(offered_price/discount_rate) はPython側で行う(GPTには計算させない)
    - 判定不能な場合は price_negotiation (最も軽い扱い) にフォールバックする。
      rude_offer 等の強い判定へは倒さない。

    fetch_messages_ebay.py の分類保存(classify_message)と、
    message_viewer.py のAI返信生成(/api/generate)の両方から共通で呼び出される。
    """
    result = {
        "is_negotiation": False,
        "offer_type":     None,
        "value":          None,
        "currency":       None,
        "offered_price":  None,
        "discount_rate":  None,
        "category":       "needs_review",
    }

    client = OpenAI()

    # Step 1: 価格交渉/真贋確認/その他 を判別
    try:
        resp1 = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=50,
            messages=[{"role": "user", "content":
                'Classify the following eBay buyer message into exactly one category.\n'
                '- "price_negotiation": the buyer is negotiating or asking for a discount on the price.\n'
                '- "authenticity_check": the buyer is ONLY asking whether the item is authentic/genuine/real '
                '(e.g. "Is this authentic?", "Is this genuine?", "本物ですか？", "正規品ですか？"), with no '
                'price negotiation involved.\n'
                '- "other": anything else.\n'
                'Reply in JSON only: {"category": "price_negotiation" | "authenticity_check" | "other"}\n\n'
                f'Message: {body}'
            }]
        )
        cat = _parse_json_response(resp1.choices[0].message.content).get("category", "other")
    except Exception:
        return result  # needs_review のまま

    if cat == "authenticity_check":
        result["category"] = "authenticity_check"
        return result

    if cat != "price_negotiation":
        return result  # needs_review のまま

    result["is_negotiation"] = True

    # Step 2: Buyerが何を要求しているか(意味)をGPTに構造化させる
    offer      = _extract_offer(body)
    offer_type = offer.get("offer_type")
    value      = offer.get("value")
    currency   = offer.get("currency")

    if offer_type not in ("absolute_price", "amount_off", "percent_off", "vague"):
        offer_type = None  # 想定外の値はvague相当として扱う(_compute_offerで弾かれる)

    result["offer_type"] = offer_type
    result["value"]      = value
    result["currency"]   = currency

    # Step 3: 出品価格と組み合わせて最終計算(Python側)
    if start_price is None:
        start_price = _get_listing_price(listing_id)

    offered_price, discount_rate = _compute_offer(offer_type, value, currency, start_price)

    if offered_price is None:
        # vague / 通貨不明・不一致 / 異常値 / start_price取得不可、いずれも安全側(price_negotiation)へ
        result["category"] = "price_negotiation"
        return result

    result["offered_price"] = offered_price
    result["discount_rate"] = discount_rate
    result["category"]      = _category_from_discount_rate(discount_rate)
    return result


def _encode_sub_type(offer_type, value, currency) -> str:
    """
    offer_type/value/currency を DB(trx.ebay_messages.auto_reply_type, nvarchar(50))へ
    保存するためのコンパクトな文字列表現(新しいカラムは追加しない)。
    message_viewer.py 側でこれをデコードし、GPTを再度呼ばずに
    offered_price/discount_rate を再計算できるようにするためのもの。
    """
    if offer_type is None:
        return None
    if offer_type == "vague":
        return "vague"

    value_str    = "" if value is None else str(value)
    currency_str = currency or ""
    encoded = f"{offer_type}|{value_str}|{currency_str}"

    # nvarchar(50) を超える異常なケースは安全側でoffer_typeのみにする(通常は発生しない想定)
    if len(encoded) > 50:
        encoded = offer_type
    return encoded


def decode_sub_type(sub_type: str) -> dict:
    """
    auto_reply_type に保存された文字列から offer_type/value/currency を復元する。
    旧フォーマット(単なるoffer_type文字列)やNoneにも安全にフォールバックする。
    """
    result = {"offer_type": None, "value": None, "currency": None}
    if not sub_type:
        return result

    parts = sub_type.split("|")
    offer_type = parts[0]
    if offer_type not in ("absolute_price", "amount_off", "percent_off", "vague"):
        return result
    result["offer_type"] = offer_type

    if len(parts) >= 3:
        value_str, currency_str = parts[1], parts[2]
        if value_str:
            try:
                result["value"] = float(value_str)
            except ValueError:
                result["value"] = None
        result["currency"] = currency_str or None

    return result


def compute_negotiation_display(sub_type: str, start_price) -> dict:
    """
    保存済みのsub_type(offer_type/value/currency)と現在のstart_priceから、
    GPTを一切呼ばずに(Python側の既存の確定計算のみで) 表示用の
    offered_price/discount_rate/categoryを再計算する。

    fetch時に既に確定していたoffer_type/value/currencyの「意味の分類」を再利用し、
    「最終計算」(_compute_offer / _category_from_discount_rate)は
    既存のAI返信生成ロジックと同じ関数をそのまま使う。

    計算できない場合は reason に理由("vague"/"currency_mismatch"/"unavailable")を入れて返す。
    """
    decoded    = decode_sub_type(sub_type)
    offer_type = decoded["offer_type"]
    value      = decoded["value"]
    currency   = decoded["currency"]

    result = {
        "offer_type":    offer_type,
        "value":         value,
        "currency":      currency,
        "offered_price": None,
        "discount_rate": None,
        "category":      None,
        "reason":        None,
    }

    if offer_type == "vague":
        result["reason"] = "vague"
        return result

    if offer_type not in ("absolute_price", "amount_off", "percent_off"):
        result["reason"] = "unavailable"
        return result

    currency_norm = currency.strip().upper() if isinstance(currency, str) else None
    if currency_norm == "OTHER":
        result["reason"] = "currency_mismatch"
        return result

    offered_price, discount_rate = _compute_offer(offer_type, value, currency, start_price)
    if offered_price is None:
        result["reason"] = "unavailable"
        return result

    result["offered_price"] = offered_price
    result["discount_rate"] = discount_rate
    result["category"]      = _category_from_discount_rate(discount_rate)
    return result


def classify_message(body: str, listing_id: str = None) -> dict:
    """DB保存用の従来インタフェース(type/sub_type)を維持するラッパー"""
    analysis = analyze_price_negotiation(body, listing_id=listing_id)
    sub_type = _encode_sub_type(analysis["offer_type"], analysis["value"], analysis["currency"])
    return {"type": analysis["category"], "sub_type": sub_type}


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
def _message_exists(cur, message_id: str) -> bool:
    cur.execute("SELECT 1 FROM trx.ebay_messages WHERE message_id = ?", message_id)
    return cur.fetchone() is not None


def save_message(cur, account: str, msg: dict, category: dict = None, body_text_ja: str = None):
    if _message_exists(cur, msg["message_id"]):
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
# 4.5 自動返信
# ============================================================
# 自動送信の対象とする分類:
#   - rude_offer               : 提示額が現在価格の50%未満
#   - price_negotiation_large  : 提示額が現在価格の50%以上80%未満
#   - price_negotiation(vague) : 金額提示なしの「値引きできますか？」のみ
# それ以外(price_negotiationで具体的な金額あり=80%以上の値引き交渉、authenticity_check、
# needs_review等)は、これまでどおり人間が確認してから返信する。
AUTO_REPLY_TEMPLATE_KEYS = {
    "rude_offer":              "rude_offer",
    "price_negotiation_large": "price_negotiation_large",
}


def _get_auto_reply_body(category: str, sub_type: str):
    """自動送信の対象なら送信本文(reply_en)を返す。対象外なら None。"""
    template_key = AUTO_REPLY_TEMPLATE_KEYS.get(category)
    if template_key:
        return REPLY_TEMPLATES[template_key]["reply_en"]

    if category == "price_negotiation":
        decoded = decode_sub_type(sub_type)
        if decoded["offer_type"] == "vague":
            return REPLY_TEMPLATES["price_negotiation_vague"]["reply_en"]

    return None


def _send_ebay_reply(account: str, listing_id: str, sender_id: str,
                      parent_message_id: str, body: str) -> dict:
    """message_viewer.py の _send_ebay_reply と同じ eBay Trading API 呼び出し。"""
    token = get_access_token_new(account)
    if not token:
        return {"ok": False, "error": "token取得失敗"}

    xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<AddMemberMessageRTQRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials>
    <eBayAuthToken>{token}</eBayAuthToken>
  </RequesterCredentials>
  <ItemID>{listing_id}</ItemID>
  <MemberMessage>
    <Body>{xml_escape(body)}</Body>
    <RecipientID>{sender_id}</RecipientID>
    <ParentMessageID>{parent_message_id}</ParentMessageID>
  </MemberMessage>
</AddMemberMessageRTQRequest>""".encode("utf-8")

    headers = {
        "Content-Type":                   "text/xml",
        "X-EBAY-API-CALL-NAME":           "AddMemberMessageRTQ",
        "X-EBAY-API-SITEID":              "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "X-EBAY-API-IAF-TOKEN":           token,
    }

    try:
        r = requests.post(TRADING_API_URL, data=xml_body, headers=headers, timeout=30)
        r.encoding = "utf-8"
    except Exception as e:
        return {"ok": False, "error": f"HTTP error: {e}"}

    try:
        ns   = {"e": "urn:ebay:apis:eBLBaseComponents"}
        root = ET.fromstring(r.content)
        ack  = root.findtext("e:Ack", namespaces=ns) or ""
    except Exception as e:
        return {"ok": False, "error": f"XML parse error: {e} / body: {r.text[:200]}"}

    if ack in ("Success", "Warning"):
        return {"ok": True}

    errors = [err.text for err in root.findall(".//e:Errors/e:LongMessage", ns) if err.text]
    return {"ok": False, "error": " / ".join(errors) or f"API error (Ack={ack})"}


def _fetch_auto_reply_targets(cur):
    """
    スレッド(sender_id+listing_id)の最新メッセージがbuyerかつskip_reply=0
    (= message_viewer.py上での「未返信」)のもののうち、自動送信対象カテゴリに
    該当するものを一覧する。新規フェッチ直後・既存バックログの一括処理の両方で
    同じ抽出条件を使う(=既存の「未返信」判定の仕組みをそのまま利用)。
    """
    cur.execute("""
        SELECT message_id, account, listing_id, item_title, sender_id,
               category, auto_reply_type
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY sender_id, listing_id
                       ORDER BY received_at DESC
                   ) AS rn
            FROM trx.ebay_messages
        ) m
        WHERE rn = 1
          AND direction = 'buyer'
          AND skip_reply = 0
    """)
    cols = [d[0] for d in cur.description]
    targets = []
    for row in cur.fetchall():
        m = dict(zip(cols, row))
        body = _get_auto_reply_body(m["category"], m["auto_reply_type"])
        if body:
            m["auto_reply_body"] = body
            targets.append(m)
    return targets


def report_auto_reply_targets():
    """実際には送信せず、自動送信対象になる未返信メッセージの件数・内容だけを表示する。"""
    cn  = get_sql_server_connection()
    cur = cn.cursor()
    targets = _fetch_auto_reply_targets(cur)
    cur.close(); cn.close()

    print(f"自動送信対象件数: {len(targets)}件")
    for m in targets:
        print(f"  - [{m['category']}] account={m['account']} sender={m['sender_id']} "
              f"listing={m['listing_id']} item={m['item_title']} "
              f"sub_type={m['auto_reply_type']} message_id={m['message_id']}")
    return targets


def run_auto_reply():
    """未返信メッセージのうち自動送信対象のものへ、実際にeBay経由で返信を送信する。"""
    cn  = get_sql_server_connection()
    cur = cn.cursor()
    _auto_reply_pass(cur, cn)
    cur.close(); cn.close()


def _auto_reply_pass(cur, cn):
    """自動送信対象のメッセージへ実際に返信を送信し、DBへ送信済みメッセージとして保存する。"""
    targets = _fetch_auto_reply_targets(cur)

    for m in targets:
        result = _send_ebay_reply(m["account"], m["listing_id"], m["sender_id"],
                                   m["message_id"], m["auto_reply_body"])
        if not result["ok"]:
            print(f"  ❌ 自動返信失敗: {m['message_id']} ({result['error']})")
            continue

        now    = datetime.utcnow()
        new_id = f"SELLER-{int(now.timestamp() * 1000)}"
        cur.execute("""
            INSERT INTO trx.ebay_messages (
                message_id, account, listing_id, item_title, sender_id,
                direction, received_at, body_text, body_text_ja,
                category, auto_reply_type, skip_reply
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, new_id, m["account"], m["listing_id"], m["item_title"], m["sender_id"],
             "seller", now, m["auto_reply_body"], None, None, None, 0)
        cn.commit()
        print(f"  🤖 自動返信送信: {m['message_id']} [{m['category']}] → {new_id}")


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
                # 既存メッセージは分類・翻訳のAPI呼び出し前にスキップ（自動更新での無駄なOpenAI課金を防ぐ）
                if _message_exists(cur, msg["message_id"]):
                    print(f"  ⏭ SKIP (既存): {msg['message_id']}")
                    continue

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

    print("  [自動返信]")
    _auto_reply_pass(cur, cn)

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
    elif "--auto-reply-report" in sys.argv:
        # 実際には送信せず、自動送信対象になる未返信メッセージの件数・内容だけを表示する
        report_auto_reply_targets()
    elif "--auto-reply-send" in sys.argv:
        # 既存の未返信バックログに対して、自動送信対象のものへ実際に返信を送信する
        print(f"START auto-reply (backlog): {datetime.now():%Y-%m-%d %H:%M:%S}")
        run_auto_reply()
        print(f"DONE: {datetime.now():%Y-%m-%d %H:%M:%S}")
    elif "--once" in sys.argv:
        run_once()
    else:
        run()