# -*- coding: utf-8 -*-
"""
トレカ Condition 対応の動作確認用スクリプト。
- build_card_condition_fields() の解析ロジック（鑑定済み/未鑑定の判定、PSA10等のパース）
- register_inventory_item() が組み立てる Inventory API payload（condition / conditionDescriptors）
を、実際の eBay API には通信せずに確認する。

実行方法:
    python -m apps.tests.test_card_condition
"""

from unittest.mock import patch, MagicMock

from apps.publish.publish_ebay import build_card_condition_fields, parse_card_grade_text
from apps.adapters.ebay_api import register_inventory_item


def attrs_with_grade(text: str):
    return [
        {"text": "ブランド", "values": [{"text": "Pokemon"}]},
        {"text": "グレード", "values": [{"text": text}]},
    ]


def attrs_without_grade():
    return [
        {"text": "ブランド", "values": [{"text": "Pokemon"}]},
    ]


def check(label, actual, expected):
    ok = actual == expected
    mark = "OK" if ok else "NG"
    print(f"[{mark}] {label}: actual={actual!r} expected={expected!r}")
    return ok


def test_parse_card_grade_text():
    print("\n=== parse_card_grade_text ===")
    check("PSA10", parse_card_grade_text("PSA10"), ("PSA", "10"))
    check("BGS9.5", parse_card_grade_text("BGS9.5"), ("BGS", "9.5"))
    check("BGS 9.5 (with space)", parse_card_grade_text("BGS 9.5"), ("BGS", "9.5"))
    check("CGC9", parse_card_grade_text("CGC9"), ("CGC", "9"))
    check("unparseable", parse_card_grade_text("鑑定済み"), (None, None))


def test_build_card_condition_fields():
    print("\n=== build_card_condition_fields ===")

    cond, desc = build_card_condition_fields(attrs_with_grade("PSA10"))
    check("PSA10 -> conditionId", cond, "2750")
    check("PSA10 -> descriptors", desc, [
        {"name": "27501", "values": ["275010"]},
        {"name": "27502", "values": ["275020"]},
    ])

    cond, desc = build_card_condition_fields(attrs_with_grade("BGS9.5"))
    check("BGS9.5 -> conditionId", cond, "2750")
    check("BGS9.5 -> descriptors", desc, [
        {"name": "27501", "values": ["275013"]},
        {"name": "27502", "values": ["275021"]},
    ])

    cond, desc = build_card_condition_fields(attrs_without_grade())
    check("no グレード -> conditionId", cond, "4000")
    check("no グレード -> descriptors", desc, [{"name": "40001", "values": ["400015"]}])

    cond, desc = build_card_condition_fields(attrs_with_grade("謎の鑑定999"))
    check("unparseable グレード -> conditionId (fallback)", cond, "4000")
    check("unparseable グレード -> descriptors (fallback)", desc, [{"name": "40001", "values": ["400015"]}])

    cond, desc = build_card_condition_fields(None)
    check("item_attributes=None -> conditionId", cond, "4000")


def _fake_put_capture(captured: dict):
    def _put(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"sku": json.get("sku")}
        return resp
    return _put


def test_register_inventory_item_payload():
    print("\n=== register_inventory_item: payload構築確認（eBayには通信しない） ===")

    base_row = {
        "CustomLabel": "TESTSKU1",
        "*Title": "Test Card",
        "*Description": "desc",
        "*Quantity": 1,
        "PicURL": "https://example.com/1.jpg",
        "C:Brand": "Pokemon",
    }

    # --- 鑑定済み（トレカ / PSA10） ---
    row = dict(base_row)
    row["category_id"] = "183454"
    row["*ConditionID"] = "2750"
    row["conditionDescriptors"] = [
        {"name": "27501", "values": ["275010"]},
        {"name": "27502", "values": ["275020"]},
    ]
    row["C:Game"] = "Pokémon TCG"
    captured = {}
    with patch("apps.adapters.ebay_api.requests.put", side_effect=_fake_put_capture(captured)):
        register_inventory_item(row, token="dummy")
    check("graded(2750) -> condition", captured["json"]["condition"], "LIKE_NEW")
    check("graded(2750) -> conditionDescriptors", captured["json"]["conditionDescriptors"], row["conditionDescriptors"])
    check("C:Game -> aspects[Game]", captured["json"]["product"]["aspects"].get("Game"), ["Pokémon TCG"])

    # --- 未鑑定（トレカ） ---
    row = dict(base_row)
    row["category_id"] = "183050"
    row["*ConditionID"] = "4000"
    row["conditionDescriptors"] = [{"name": "40001", "values": ["400015"]}]
    captured = {}
    with patch("apps.adapters.ebay_api.requests.put", side_effect=_fake_put_capture(captured)):
        register_inventory_item(row, token="dummy")
    check("ungraded(4000) -> condition", captured["json"]["condition"], "USED_VERY_GOOD")
    check("ungraded(4000) -> conditionDescriptors", captured["json"]["conditionDescriptors"], row["conditionDescriptors"])

    # --- トレカ以外のカテゴリ: 既存動作が変わらないことを確認 ---
    row = dict(base_row)
    row["category_id"] = "999999"
    row["*ConditionID"] = "4000"
    captured = {}
    with patch("apps.adapters.ebay_api.requests.put", side_effect=_fake_put_capture(captured)):
        register_inventory_item(row, token="dummy")
    check("non-TCG category(4000) -> condition (既存ロジックのまま)", captured["json"]["condition"], "USED_GOOD")
    check("non-TCG category -> conditionDescriptors not set", "conditionDescriptors" in captured["json"], False)

    row = dict(base_row)
    row["category_id"] = "999999"
    row["*ConditionID"] = "2750"
    captured = {}
    with patch("apps.adapters.ebay_api.requests.put", side_effect=_fake_put_capture(captured)):
        register_inventory_item(row, token="dummy")
    # 非TCGカテゴリでは元々 "2750" は cond_map に存在せず、フォールバック既定値 USED_EXCELLENT になる（既存動作のまま）
    check("non-TCG category(2750) -> condition (既存ロジックのまま)", captured["json"]["condition"], "USED_EXCELLENT")


def main():
    test_parse_card_grade_text()
    test_build_card_condition_fields()
    test_register_inventory_item_payload()


if __name__ == "__main__":
    main()
