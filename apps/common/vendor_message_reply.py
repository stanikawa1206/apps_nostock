"""
購入取引メッセージ対応画面（/messages）用の共通処理。

Webアプリ本体（D:\\apps_resale\\furima\\webapp\\messages_blueprint.py）から呼び出される。
Selenium等によるサイトへの実際のメッセージ送信は、本モジュールにはまだ含めない
（次段階で追加する。誤送信防止のため、現時点ではDB参照と定型文判定のみを行う）。
"""
import re

from apps.common.vendor_purchase_common import get_access_connection, is_shipped_status  # noqa: F401 (get_access_connectionは呼び出し元の便宜のため re-export)

# 画面表示・対象抽出の対象とするvendor_name（既存のtrx.vendor_message表記に合わせる）。
TARGET_VENDOR_NAMES = ("メルカリ", "ＰａｙＰａｙフリマ", "ラクマ")

# 各サイトの個別取引ページURL。既存の購入スクレイパーが実際に使用/取得している
# URL形式をそのまま流用する（新規に推測しない）。
#   メルカリ: mercari_purchase.py の transaction_urls 組み立て
#     (f"https://jp.mercari.com/transaction/{iid}")
#   ラクマ: rakuma_purchase.py が実際に収集するリンク
#     (https://fril.jp/transaction?item_id={id} / 実機確認済み)
#   ＰａｙＰａｙフリマ: 取引ページ(/item/{id}/trade/buyer)は「ご指定のページが
#     見つかりませんでした」になることを実機確認済みのため、商品ページ
#     (https://paypayfleamarket.yahoo.co.jp/item/{id}) を使う。
TRANSACTION_URL_BUILDERS = {
    "メルカリ": lambda oid: f"https://jp.mercari.com/transaction/{oid}",
    "ラクマ": lambda oid: f"https://fril.jp/transaction?item_id={oid}",
    "ＰａｙＰａｙフリマ": lambda oid: f"https://paypayfleamarket.yahoo.co.jp/item/{oid}",
}


# Access バックエンド（実データ本体。Y:\ヤフオクDB.accdb）に対して、フロントエンド
# ヤフオク.accdb の保存済みクエリ「到着日入力」と同じ抽出条件を直接実行する。
# フロントエンドの pyodbc 直結（旧get_access_frontend_connection）は、人がAccessで
# フロントエンドを開いている間ロック競合(-3810)を起こすことが実機で複数回確認された。
# 日常・ASINはいずれもフロントエンド側では単なるリンクテーブルで、実体はこのバックエンドに
# あるため、バックエンドに直結すればフロントエンドの開閉状態に影響されない
# （フロントエンドを開いたままバックエンドへ直結できることも実機確認済み）。
#
# 「到着日入力」クエリ自体（WHERE条件）はバックエンド側にはオブジェクトとして
# 保存されていないため、実機調査済みの条件をここに複製している:
#     仕入日 >= 2024/7/1 AND 区分 <> "ama輸出"
#     AND 返品依頼番号 IS NULL AND 発送日 IS NULL AND SKU IS NULL
#     AND 入金日 IS NULL AND 出品日 IS NULL
# 今後Access側でこの条件が変更された場合はここも追従して直す必要がある
# （以前は「クエリの実行結果を正として条件を再実装しない」方針だったが、
# ロック競合の解消を優先しバックエンド直結・条件複製の方針に変更した）。
#
# 商品名はフロントエンドの「到着日入力」と同じ導出方法（ASIN.品目があればそれを、
# 無ければ日常.品目text を使う）をそのまま再現する。
FETCH_ACTIVE_ORDERS_SQL = """
    SELECT 日常.注文ID,
           IIf(IsNull(ASIN.品目), 日常.品目text, ASIN.品目) AS 商品名,
           日常.eBayステータス,
           日常.店舗
    FROM 日常 LEFT JOIN ASIN ON 日常.ASIN = ASIN.ASIN
    WHERE 日常.仕入日 >= #7/1/2024#
      AND 日常.区分 <> 'ama輸出'
      AND 日常.返品依頼番号 IS NULL
      AND 日常.発送日 IS NULL
      AND 日常.SKU IS NULL
      AND 日常.入金日 IS NULL
      AND 日常.出品日 IS NULL
"""


def fetch_active_orders(access_conn) -> dict:
    """
    バックエンド（Y:\\ヤフオクDB.accdb）へ直結し、「到着日入力」と同じ条件で
    現在の抽出結果（注文ID・商品名・eBayステータス・店舗）を返す。

    店舗列は、trx.vendor_messageに保存されている(vendor_name, vendor_item_id)全129件と
    突き合わせて実機検証済み（一致126件・不一致0件・NULL0件。残り3件は日常に
    レコード自体が無いだけで矛盾ではない）。trx.vendor_messageに一度も履歴の無い
    「無言発送」の取引でも、この店舗列だけでvendor_nameを特定できる
    （trx.vendor_messageへダミー行を作らず、新規テーブルも増やさずに済む）。

    注文IDが日常テーブル上で複数行になっている場合（実データで実例あり。1回の
    購入で複数ASINを別行として記録している等）、それらをまとめて同一取引として扱う。
    eBayステータス・店舗は注文単位の情報で、既存の更新処理（write_ebay_status_if_advancing
    等）が常に注文ID一致の全行へUPDATEするため、対象行間で値が揃っている前提で
    先に見つかった行の値を採用する。商品名だけは行ごとに異なりうるため、
    重複を除いて出現順に全件保持する（1件も取りこぼさない）。

    戻り値: {注文ID: {"product_names": [str, ...], "ebay_status": str, "vendor_name": str}, ...}
    """
    result = {}
    with access_conn.cursor() as cur:
        cur.execute(FETCH_ACTIVE_ORDERS_SQL)
        for order_id, product_name, ebay_status, vendor_name in cur.fetchall():
            if not order_id:
                continue
            entry = result.setdefault(order_id, {
                "product_names": [],
                "ebay_status": ebay_status,
                "vendor_name": vendor_name,
            })
            if product_name and product_name not in entry["product_names"]:
                entry["product_names"].append(product_name)
    return result


# ============================================================
# 対象抽出
# ============================================================
def fetch_pending_seller_messages(sql_conn, access_frontend_conn):
    """
    対象抽出は次の順序で絞り込む（trx.vendor_messageの蓄積量に対象件数が
    連動して増え続けないようにするため、Access「到着日入力」の現在の対象を起点にする）:
      ① Access「到着日入力」に現在表示される注文ID（= trx.vendor_messageのvendor_item_id）
         と、そのeBayステータス・商品名・店舗（=vendor_name）を取得
      ② 店舗がTARGET_VENDOR_NAMESの注文IDについて、対応する trx.vendor_message の
         メッセージ履歴を取得する（履歴が1件も無い＝一度もメッセージが交換されて
         いない「無言発送」の注文IDも、店舗からvendor_nameが分かるため対象に含める）
      ③ 次のいずれかに該当する取引だけを対象にする（人が「返信不要」にした対象は除く）
           A. 最新メッセージが出品者（sender_type='出品者'）
           B. 発送済み（is_shipped_status）で、まだ返信2相当を送っていない
              （無言発送も対象に含まれる。最新メッセージが誰からでも良い）

    戻り値: [
        {
            "vendor_name": str,
            "vendor_item_id": str,
            "product_names": [str, ...],     # 同一注文IDに複数商品がある場合は全件
            "transaction_url": str|None,
            "seller_name": str,             # 履歴中の出品者メッセージの sender_name（無ければNone）
            "latest_message": {同形式の辞書}|None,
            "history": [同形式の辞書, ...],  # message_no昇順
            "suggested_reply": {"text": str, "source": str|None, "template_key": str|None},
            "can_skip": bool,               # 「返信不要」ボタンを表示してよいか
        },
        ...
    ]
    最新メッセージが新しい順（updated_atが無いものは末尾）で返す。
    """
    active_orders = fetch_active_orders(access_frontend_conn)
    if not active_orders:
        return []

    target_order_ids = [
        oid for oid, info in active_orders.items()
        if info["vendor_name"] in TARGET_VENDOR_NAMES
    ]
    if not target_order_ids:
        return []

    history_by_key = _fetch_histories_for_orders(sql_conn, target_order_ids)

    # trx.vendor_messageに一度も履歴が無い「無言発送」の注文IDも、店舗（=vendor_name）から
    # 判明するので、空の履歴として対象に加える（メッセージが無いこと自体は正常な状態）。
    for oid in target_order_ids:
        key = (active_orders[oid]["vendor_name"], oid)
        history_by_key.setdefault(key, [])

    items = []
    for (vendor_name, vendor_item_id), history in history_by_key.items():
        order_info = active_orders.get(vendor_item_id)
        if order_info is None:
            continue

        is_shipped = is_shipped_status(order_info["ebay_status"])
        latest = history[-1] if history else None
        suggested_reply = determine_suggested_reply(history, is_shipped)

        include = False

        # 条件A: 最新メッセージが出品者で、まだ「返信不要」にされていない
        # （reply_skippedはそのメッセージ行自体のフラグ。新しい出品者メッセージが
        # 来ると新しい行が追加され、そちらはreply_skipped=0がデフォルトなので
        # 自動的に再び対象になる＝eBay Messagesのskip_replyと同じ仕組み）。
        if latest and latest["sender_type"] == "出品者" and not latest["reply_skipped"]:
            include = True

        # 条件B: 発送済みで、まだ返信2相当を送っていない（無言発送も含む）。
        # メッセージが1件も無いことがあるため、reply_skippedを乗せる行が無く、
        # このケースは「返信不要」を保存しない（ボタン自体を表示しない）。
        if is_shipped and suggested_reply["template_key"] == "shipped_2":
            include = True

        if not include:
            continue

        # 返信不要ボタンは、実際にフラグを立てられる対象（条件Aで、かつ既に
        # 返信不要済みでない出品者メッセージが存在する場合）にのみ表示する。
        can_skip = bool(latest and latest["sender_type"] == "出品者" and not latest["reply_skipped"])

        seller_messages = [m for m in history if m["sender_type"] == "出品者"]
        items.append({
            "vendor_name": vendor_name,
            "vendor_item_id": vendor_item_id,
            "product_names": order_info["product_names"],
            "transaction_url": _build_transaction_url(vendor_name, vendor_item_id),
            "seller_name": seller_messages[-1]["sender_name"] if seller_messages else None,
            "is_shipped": is_shipped,
            "latest_message": latest,
            "history": history,
            "suggested_reply": suggested_reply,
            "can_skip": can_skip,
        })

    items.sort(key=lambda it: it["latest_message"]["message_no"] if it["latest_message"] else -1, reverse=True)
    return items


def _build_transaction_url(vendor_name, vendor_item_id):
    builder = TRANSACTION_URL_BUILDERS.get(vendor_name)
    return builder(vendor_item_id) if builder else None


def _fetch_histories_for_orders(sql_conn, order_ids):
    """
    order_ids（Access「到着日入力」の現在の対象注文ID）に含まれ、かつ
    vendor_nameがTARGET_VENDOR_NAMESであるtrx.vendor_messageの全メッセージを取得する。
    戻り値: {(vendor_name, vendor_item_id): [メッセージ辞書, ...]（message_no昇順）}
    """
    order_ids = list(order_ids)
    if not order_ids:
        return {}

    vendor_placeholders = ", ".join(["?"] * len(TARGET_VENDOR_NAMES))
    order_id_placeholders = ", ".join(["?"] * len(order_ids))
    params = list(TARGET_VENDOR_NAMES) + order_ids

    with sql_conn.cursor() as cur:
        cur.execute(f"""
            SELECT vendor_name, vendor_item_id, message_id, message_no, sender_name, sender_type,
                   message_datetime_text, message_datetime, message_body, reply_skipped
            FROM trx.vendor_message
            WHERE vendor_name IN ({vendor_placeholders})
              AND vendor_item_id IN ({order_id_placeholders})
            ORDER BY vendor_name, vendor_item_id, message_no
        """, params)
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    history_by_key = {}
    for row in rows:
        key = (row["vendor_name"], row["vendor_item_id"])
        # message_datetime（メルカリ・絶対日時）が入っていればそれを表示用文字列にする。
        # まだ移行していないサイト（ＰａｙＰａｙフリマ・ラクマ）や過去の未補正行は
        # message_datetime_text（画面表示の相対時刻等）にフォールバックする。
        dt = row["message_datetime"]
        display_datetime_text = dt.strftime("%Y/%m/%d %H:%M:%S") if dt else row["message_datetime_text"]
        history_by_key.setdefault(key, []).append({
            "message_id": row["message_id"],
            "message_no": row["message_no"],
            "sender_name": row["sender_name"],
            "sender_type": row["sender_type"],
            "message_datetime_text": display_datetime_text,
            "message_body": row["message_body"],
            "reply_skipped": bool(row["reply_skipped"]),
        })
    return history_by_key


# ============================================================
# 「返信不要」の永続化（trx.vendor_message.reply_skipped）
# ============================================================
# eBay Messages（trx.ebay_messages.skip_reply）と同じ考え方: 対応不要と判断した
# 「特定のメッセージ行」にフラグを立てるだけで、別テーブルは持たない。
# 常にその取引の最新メッセージ行のフラグで対象かどうかを判定するため、
# 新しい出品者メッセージが来ると新しい行（reply_skipped=0がデフォルト）が
# 最新行になり、自動的に再び対象へ戻る。
# 再スクレイピングのUPSERT（SQL_UPSERT_VENDOR_MESSAGE）はreply_skippedを
# UPDATE SET句に含めていないため、同じmessage_noの本文が更新されてもフラグは保持される。
#
# メッセージが1件も無い「無言発送」（発送済みで返信2待ちだが会話が無いケース）は、
# フラグを乗せる行が存在しないため、今回は返信不要を保存しない
# （fetch_pending_seller_messages側でcan_skip=Falseとしてボタン自体を出さない）。
def mark_reply_skipped(sql_conn, vendor_name: str, vendor_item_id: str, message_no: int) -> bool:
    """
    「返信不要」ボタン押下時に呼ぶ。指定した既存のメッセージ行のreply_skippedを1にする。
    戻り値: 該当行が存在し更新できたか。
    """
    with sql_conn.cursor() as cur:
        cur.execute("""
            UPDATE trx.vendor_message SET reply_skipped = 1
            WHERE vendor_name = ? AND vendor_item_id = ? AND message_no = ?
        """, vendor_name, vendor_item_id, message_no)
        updated = cur.rowcount > 0
    sql_conn.commit()
    return updated


# ============================================================
# 返信案（定型文判定）
# ============================================================
# 発送済み（無言発送含む）に対する定型文。到着報告・受取通知の一般的な断りを含む。
# 発送1回につき1回だけ提案する（履歴内に既に同趣旨の返信が無い場合のみ）。
TEMPLATE_SHIPPED = (
    "早々に発送いただきありがとうございます。\n"
    "到着を楽しみに待ってます。\n"
    "受取通知はなるべく早くできるように心がけておりますが、\n"
    "仕事等の事情により、少し遅くなる場合もございます。\n"
    "恐縮ですが、お待ちいただけますと助かります。"
)
# 既にこの趣旨の返信を送信済みかどうかの判定に使うキーワード（部分一致）。
_TEMPLATE_SHIPPED_DETECT_RE = re.compile("到着を楽しみに|受取通知")

# 出品者からのメッセージが「発送完了」の連絡かどうかの判定。
# 「発送」という文字を含むだけでは判定しない（「明日発送します」「発送予定です」
# 「まだ発送していません」「発送が遅れています」「発送できていません」等の
# 未完了・予定・否定表現を誤検知しないため）。「発送(手配)?(いた)?しました」の形の
# 完了表現（発送しました／発送いたしました／発送手配いたしました等）のみを拾う。
# メルカリ側の配送ステータス反映には時間差があるため（例: ゆうパケットポストの
# 投函直後など）、出品者が発送完了を明言した時点で発送のお礼を出したい、という
# 実運用上の要望に基づく。実際に発送済みかどうかの追跡はこの判定の責任範囲外
# （既存の通常の配送ステータス取得処理に任せる）。
_SHIPPED_COMPLETE_MESSAGE_RE = re.compile("発送(手配)?(いた)?しました")

# 出品者からの最初のメッセージに対する返信（まだ発送前・まだ一度も返信していない場合のみ）。
TEMPLATE_FIRST_REPLY_ONEGAI = (
    "こちらこそ、お手数をおかけしますが、\n"
    "お取引終了まで、何卒、よろしくお願いいたします。"
)
TEMPLATE_FIRST_REPLY_PLAIN = (
    "お手数をおかけしますが、\n"
    "お取引終了まで、何卒、よろしくお願いいたします。"
)
# 出品者の文言に「お願いします」系が含まれるかどうかの判定（1-1 / 1-2の分岐）。
# 過剰な意味解析はせず、「お願い」の一般的な表記揺れ（します/いたします/致します）のみ拾う。
_ONEGAI_RE = re.compile("お願いします|お願いいたします|お願い致します")


def determine_suggested_reply(history: list, is_shipped: bool) -> dict:
    """
    history: message_no昇順の会話履歴（sender_type='出品者'|'購入者'）。空のこともある
             （無言発送で一度もメッセージが交換されていない場合）。
    is_shipped: 既存の購入スクレイピングが取得した取引ステータス・配送状況から判定した
                「発送済み以上」かどうか。無言発送（メッセージが一切無い発送）でも
                「2」を提案できるよう、メッセージ本文とは別に必要な情報。

    優先順位:
      1. 次のいずれかを満たせば「発送済み」とみなし、まだ返信2相当を送っていない
         場合に限り「2」を提案する（履歴中に既に同趣旨の返信があれば、二重に提案しない）。
           A. is_shipped（既存の配送ステータス取得処理が「発送済み」と確認済み）
           B. 出品者からの最新メッセージが「発送しました」等の発送完了連絡
              （_SHIPPED_COMPLETE_MESSAGE_RE。メルカリ側の配送ステータス反映の
              時間差を待たず、出品者本人の発送完了連絡を優先する）。
      2. 上記に該当しなければ、出品者からのメッセージがこれまでにちょうど1件だけあり、
         かつその出品者メッセージより後に自分からの返信がまだ無い場合に限り
         「1-1」または「1-2」を提案する（出品者からのメッセージが今回で初めてのケース。
         出品者メッセージが既に2件以上ある取引には提案しない＝人が判断する）。
         出品者のそのメッセージに「お願いします」系が含まれれば1-1、それ以外は1-2。
         購入直後に買い手自身が送る挨拶（出品者メッセージより前に存在する自分の
         メッセージ）は「返信済み」の判定に使わない（実データm11655754962で、
         購入直後の挨拶のせいで本来出すべき初回提案が出ない不具合があったため）。
      3. それ以外は自動提案しない（人が判断する。「返信不要」の対象になりうる）。
    """
    own_messages = [m for m in history if m["sender_type"] == "購入者"]
    seller_messages = [m for m in history if m["sender_type"] == "出品者"]

    seller_announced_shipped = bool(
        seller_messages and _SHIPPED_COMPLETE_MESSAGE_RE.search(seller_messages[-1]["message_body"] or "")
    )

    if is_shipped or seller_announced_shipped:
        already_sent = any(
            _TEMPLATE_SHIPPED_DETECT_RE.search(m["message_body"] or "") for m in own_messages
        )
        if not already_sent:
            return {"text": TEMPLATE_SHIPPED, "source": "template", "template_key": "shipped_2"}
        return {"text": "", "source": None, "template_key": None}

    if len(seller_messages) == 1:
        first_seller_message = seller_messages[0]
        replied_after = any(
            m["sender_type"] == "購入者" and m["message_no"] > first_seller_message["message_no"]
            for m in history
        )
        if not replied_after:
            seller_text = first_seller_message["message_body"] or ""
            if _ONEGAI_RE.search(seller_text):
                return {"text": TEMPLATE_FIRST_REPLY_ONEGAI, "source": "template", "template_key": "first_reply_onegai"}
            return {"text": TEMPLATE_FIRST_REPLY_PLAIN, "source": "template", "template_key": "first_reply_plain"}

    return {"text": "", "source": None, "template_key": None}
