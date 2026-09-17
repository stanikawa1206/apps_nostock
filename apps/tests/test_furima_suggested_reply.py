"""
determine_suggested_reply()（フリマ購入メッセージの返信案判定）の回帰テスト。

determine_suggested_reply(history, is_shipped) は history(list)・is_shipped(bool)
だけを受け取る純粋関数のため、DB接続・Selenium起動を一切行わずに検証できる。
各ケースのhistoryは、実際にtrx.vendor_messageで確認済みの実取引のメッセージ内容を
そのまま固定値として埋め込んだもの（将来そのDB行が変わっても再取得しない。
このテストは「その入力に対して常にこの出力になるべき」という固定の期待値確認専用）。

実行方法: python apps/tests/test_furima_suggested_reply.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from apps.etc.furima_purchase import (  # noqa: E402
    _ONEGAI_RE,
    _TEMPLATE_SHIPPED_DETECT_RE,
    determine_suggested_reply,
)


def _msg(message_no, sender_type, sender_name, message_body, reply_skipped=False):
    return {
        "message_id": None,
        "message_no": message_no,
        "sender_name": sender_name,
        "sender_type": sender_type,
        "message_datetime_text": None,
        "message_body": message_body,
        "reply_skipped": reply_skipped,
    }


# ------------------------------------------------------------
# _ONEGAI_RE（「お願いします」系の表記揺れ検出）単体テスト
# ------------------------------------------------------------
ONEGAI_RE_CASES = [
    ("よろしくお願いします。", True),
    ("よろしくお願いいたします。", True),
    ("よろしくお願い致します。", True),
    # 実機不具合 m31821957285: 「お願い申し上げます」が旧regexでは未対応だった。
    ("よろしくお願い申し上げます。", True),
    ("よろしくお願い申しあげます。", True),
    ("ありがとうございました。", False),
    ("また機会がありましたらよろしくお願いします", True),
]


# ------------------------------------------------------------
# _TEMPLATE_SHIPPED_DETECT_RE（発送お礼を送信済みかどうかの判定。段階B: AND判定へ厳格化）
# 単体テスト
# ------------------------------------------------------------
TEMPLATE_SHIPPED_DETECT_RE_CASES = [
    # 1. 両キーワードを含む正式なshipped_2 → 送信済みと判定
    (
        "早々に発送いただきありがとうございます。\n到着を楽しみに待ってます。\n"
        "受取通知はなるべく早くできるように心がけておりますが、\n"
        "仕事等の事情により、少し遅くなる場合もございます。\n恐縮ですが、お待ちいただけますと助かります。",
        True,
    ),
    # 2. 少し編集されているが両キーワードを含む → 送信済みと判定
    # （実データ z662541578: 誤って本文が2回連結された編集済みメッセージ）
    (
        "早々に発送いただきありがとうございます。\n到着を楽しみに待ってます。\n"
        "受取通知はなるべく早くできるように心がけておりますが、\n仕事等の事情により、少し遅くなる場合もございます。\n"
        "恐縮ですが、お待ちいただけますと助かります。早々に発送いただきありがとうございます。\n到着を楽しみに待ってます。\n"
        "受取通知はなるべく早くできるように心がけておりますが、\n仕事等の事情により、少し遅くなる場合もございます。\n"
        "恐縮ですが、お待ちいただけますと助かります。",
        True,
    ),
    # 3. 「到着を楽しみに」だけを含む無関係なメッセージ → 送信済みと判定しない
    # （実データ ラクマ ab9e1ed4354125c5aecd60b37a47c82c message_no=4:
    #   誤送信を詫びる別内容のメッセージ。同じ会話のmessage_no=2に完全一致するshipped_2が
    #   別途存在するため、already_sent_shipped_thanks自体は取引単位でTrueになる＝
    #   この1件のOR誤検出を無くしても実害が無いことを調査で確認済み）
    (
        "昨日の返信が間違っておりました\n失礼しました\n到着を楽しみに待ってます",
        False,
    ),
    # 4. 「受取通知」だけを含む無関係なメッセージ → 送信済みと判定しない（合成例）
    (
        "受取通知はいつも少し遅れてしまうかもしれませんが、気長にお待ちください。",
        False,
    ),
]


# ------------------------------------------------------------
# determine_suggested_reply() 実データ回帰テスト
# ------------------------------------------------------------
CASES = [
    {
        "name": "z613953926: 未来形メッセージ「今夜発送させて頂きます」をSHIPPEDに誤判定しない",
        "history": [
            _msg(1, "出品者", "り", "現在外出中のため、夜までお待ちください。\n今夜発送させて頂きます。"),
        ],
        "is_shipped": False,
        "expected": {"template_key": "first_reply_plain", "ai_no_reply_candidate": False},
    },
    {
        "name": "l1244030615: shipped_2送信済み後の出品者の了承メッセージはNO_REPLY_CANDIDATE扱い（PayPayフリマ）",
        "history": [
            _msg(1, "購入者", "自分",
                 "早々に発送いただきありがとうございます。\n到着を楽しみに待ってます。\n"
                 "受取通知はなるべく早くできるように心がけておりますが、\n"
                 "仕事等の事情により、少し遅くなる場合もございます。\n恐縮ですが、お待ちいただけますと助かります。"),
            _msg(2, "出品者", "maf********",
                 "ご丁寧なご連絡をありがとうございます。委細承知いたしました。\n"
                 "もう少しで届くと思われます。\nよろしくお願いいたします。"),
        ],
        "is_shipped": True,
        "expected": {"template_key": None, "ai_no_reply_candidate": True},
    },
    {
        "name": "m93160436190: 同上パターン（メルカリ）",
        "history": [
            _msg(1, "購入者", "自分",
                 "早々に発送いただきありがとうございます。\n到着を楽しみに待ってます。\n"
                 "受取通知はなるべく早くできるように心がけておりますが、\n"
                 "仕事等の事情により、少し遅くなる場合もございます。\n恐縮ですが、お待ちいただけますと助かります。"),
            _msg(2, "出品者", "メル子", "ご購入ありがとうございます！⭐︎\n全然お気になさらず、ゆっくりで大丈夫ですよ~！"),
        ],
        "is_shipped": True,
        "expected": {"template_key": None, "ai_no_reply_candidate": True},
    },
    {
        "name": "m50492038090: 出品者の最初の実質メッセージより前の値引き交渉お礼は初回挨拶に数えない",
        "history": [
            _msg(1, "購入者", "自分", "お値引きいただきましてありがとうございます。\r\n早速、購入させていただきました。"),
            _msg(2, "出品者", "SOU",
                 "ご購入ありがとうございます。\n短い時間ですが，取引よろしくお願い致します。\n発送しましたら、またご連絡いたします！"),
            _msg(3, "出品者", "SOU", "先ほど発送手続きを致しました。到着までしばらくお待ちください！"),
            _msg(4, "購入者", "自分",
                 "早々に発送いただきありがとうございます。\n到着を楽しみに待ってます。\n"
                 "受取通知はなるべく早くできるように心がけておりますが、\n"
                 "仕事等の事情により、少し遅くなる場合もございます。\n恐縮ですが、お待ちいただけますと助かります。"),
            _msg(5, "出品者", "SOU",
                 "ご連絡ありがとうございます！\n受取通知はゆっくりで大丈夫ですので、お気になさらないでください☺️\nよろしくお願いいたします。"),
        ],
        "is_shipped": True,
        "expected": {"template_key": None, "ai_no_reply_candidate": True},
    },
    {
        "name": "m92188046474: 返品・キャンセルの実トラブル対応中は定型文・AI返信不要候補のいずれも出さない",
        "history": [
            _msg(1, "出品者", "cocomuse@ブランド",
                 "ご購入いただきありがとうございます。これから発送の準備をさせていただきます。"
                 "設定した期日内に発送予定ですので今しばらくお待ちください。取引終了までよろしくお願いいたします。"),
            _msg(2, "購入者", "自分", "こちらこそ、お手数をおかけしますが、\nお取引終了まで、何卒、よろしくお願いいたします。"),
            _msg(3, "出品者", "cocomuse@ブランド",
                 "商品を発送いたしました。到着まで今しばらくお待ちください。商品が届きましたらご確認後に受け取り評価をお願いいたします。"),
            _msg(4, "購入者", "自分",
                 "早々に発送いただきありがとうございます。\r\n到着を楽しみに待ってます。\r\n"
                 "受取通知はなるべく早くできるように心がけておりますが、\r\n"
                 "仕事等の事情により、少し遅くなる場合もございます。\r\n恐縮ですが、お待ちいただけますと助かります。"),
            _msg(5, "購入者", "自分",
                 "受け取った財布について確認したところ、革に補色が施されていることが判明しました。\r\n"
                 "購入時の商品説明を改めて確認しましたが、「補色」「リカラー」「染め直し」「色補修」等についての記載は"
                 "見当たりませんでした。\r\n補色されている商品であることを事前に把握していれば購入しておらず、"
                 "商品説明に記載のない重要な状態の相違だと考えております。\r\nそのため、大変申し訳ありませんが、"
                 "今回は返品を希望いたします。\r\nお手数をおかけしますがキャンセルお手続の程よろしくお願いいたします。"),
            _msg(6, "出品者", "cocomuse@ブランド",
                 "お世話になっております。こちらはこちらでは特に補修等は行なっていないお品です。"
                 "受け取り評価にもかなりお時間を要しております。お客様都合での返品でしたら受け付けます。よろしくお願いいたします。"),
            _msg(7, "出品者", "cocomuse@ブランド", "運営より連絡がきましたが、いかがされますでしょうか？？"),
            _msg(8, "購入者", "自分",
                 "ご連絡ありがとうございます。\r\n\r\n事務局から、無料の匿名返品を利用して返品・キャンセルの手続きを"
                 "進めるよう案内がありましたので、その手順でお願いしたいと思います。\r\n\r\nこれからキャンセル申請を"
                 "行いますので、ご確認とご同意をお願いいたします。"),
            _msg(9, "出品者", "cocomuse@ブランド",
                 "ご連絡ありがとうございます。こちらから理由を再度、変更させていただけるようでしたら"
                 "返品をお受けいたしますがよろしいでしょうか。"),
            _msg(10, "購入者", "自分",
                 "キャンセル申請が差し戻されました。商品がまだ私の手元にあるためだと思われます。事務局からは"
                 "無料の匿名返品を利用するよう案内されていますので、返品手続きを進めていただけますでしょうか。"
                 "返品可能になりましたら、すぐに発送いたします。"),
            _msg(11, "購入者", "自分",
                 "どの理由への変更をご希望でしょうか。今回の返品は、商品説明に記載のない補色が確認されたためであり、"
                 "購入者都合への変更には同意できません。事務局から案内された無料の匿名返品の手順でお願いいたします。"),
            _msg(12, "出品者", "cocomuse@ブランド", "こちらも不当な理由なので返品をお受けすることは難しいです。申し訳ございません。"),
            _msg(13, "出品者", "cocomuse@ブランド", "再度運営に相談しますのでお待ちください。"),
        ],
        "is_shipped": True,
        "expected": {"template_key": None, "ai_no_reply_candidate": False},
    },
    {
        "name": "m31821957285: 「よろしくお願い申し上げます」もONEGAI系として初回挨拶(お礼版)にする",
        "history": [
            _msg(1, "出品者", "プロフ要確認！トマトぶつけ太郎",
                 "ご購入ありがとうございます。\n14日月曜日の発送を予定しております。\nよろしくお願い申し上げます。"),
        ],
        "is_shipped": False,
        "expected": {"template_key": "first_reply_onegai", "ai_no_reply_candidate": False},
    },
    {
        "name": "無言発送（メッセージ無し・発送済み）はshipped_2",
        "history": [],
        "is_shipped": True,
        "expected": {"template_key": "shipped_2", "ai_no_reply_candidate": False},
    },
    {
        "name": "発送前・履歴無しは候補なし",
        "history": [],
        "is_shipped": False,
        "expected": {"template_key": None, "ai_no_reply_candidate": False},
    },
]


def run() -> int:
    failures = []

    print("=== _ONEGAI_RE 単体テスト ===")
    for text, expected in ONEGAI_RE_CASES:
        actual = bool(_ONEGAI_RE.search(text))
        status = "OK" if actual == expected else "NG"
        print(f"[{status}] {text!r} -> {actual} (期待={expected})")
        if actual != expected:
            failures.append(f"_ONEGAI_RE: {text!r}")

    print()
    print("=== _TEMPLATE_SHIPPED_DETECT_RE 単体テスト ===")
    for text, expected in TEMPLATE_SHIPPED_DETECT_RE_CASES:
        actual = bool(_TEMPLATE_SHIPPED_DETECT_RE.search(text))
        status = "OK" if actual == expected else "NG"
        print(f"[{status}] {text[:40]!r}... -> {actual} (期待={expected})")
        if actual != expected:
            failures.append(f"_TEMPLATE_SHIPPED_DETECT_RE: {text[:40]!r}")

    print()
    print("=== determine_suggested_reply() 回帰テスト ===")
    for case in CASES:
        result = determine_suggested_reply(case["history"], case["is_shipped"])
        actual = {k: result[k] for k in case["expected"]}
        status = "OK" if actual == case["expected"] else "NG"
        print(f"[{status}] {case['name']}")
        if actual != case["expected"]:
            print(f"       期待={case['expected']}")
            print(f"       実際={actual}")
            failures.append(case["name"])

    print()
    total = len(ONEGAI_RE_CASES) + len(TEMPLATE_SHIPPED_DETECT_RE_CASES) + len(CASES)
    if failures:
        print(f"FAIL: {len(failures)}/{total}件失敗")
        for name in failures:
            print(f"  - {name}")
        return 1

    print(f"PASS: 全{total}件成功")
    return 0


if __name__ == "__main__":
    sys.exit(run())
