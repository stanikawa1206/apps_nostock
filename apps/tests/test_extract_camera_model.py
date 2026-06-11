# -*- coding: utf-8 -*-
"""
extract_camera_model() の単体テスト
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from apps.publish.publish_ebay import extract_camera_model
from apps.common.utils import get_openai_client

TITLE_JP = "Canon PowerShot SX700 HS 16.1MP 30x"

DESCRIPTION_JP = """30倍光学ズーム、赤色のコンパクトカメラ。

- ブランド: Canon
- モデル: SX700 HS
- カラー: 赤
- 光学ズーム: 30倍
- 付属品: バッテリーチャージャー、バッテリー
自宅保管品であることをご理解の上ご購入ください。
商品の状態はお写真にて必ずご確認ください☆

※付属品は写真に写っているものが全てです。
※中古品であることをご理解の上ご購入をお願いします。

『動作確認』
・基本動作確認済みです♪
・レンズの撮影に影響のあるゴミカビ曇りはなく綺麗です。
・バッテリーの持ちに関しては確認してません。
・液晶焼けがありますが写真には影響ありません。

ご覧いただきありがとうございます。"""


def test_extract_camera_model():
    print("=" * 50)
    print("入力")
    print("=" * 50)
    print(f"商品タイトル: {TITLE_JP}")
    print(f"商品説明:\n{DESCRIPTION_JP}")
    print()

    # GPT生レスポンスを直接確認
    print("=" * 50)
    print("GPT 生レスポンス")
    print("=" * 50)
    client = get_openai_client()
    prompt = f"""
あなたは中古カメラの専門家です。

商品タイトルと商品説明から、
eBay Item Specific の Model を抽出してください。

ルール:
- ブランド名は含めない
- 色は含めない
- レンズキット等の販売形態は含めない
- 付属品は含めない
- 回答はModelのみ
- 説明文は不要

商品タイトル:
{TITLE_JP}

商品説明:
{DESCRIPTION_JP}
"""
    raw = client.responses.create(
        model="gpt-4o-mini",
        input=prompt,
        temperature=0,
    )
    print(repr(raw.output_text))
    print()

    # extract_camera_model() の戻り値
    print("=" * 50)
    print("extract_camera_model() 戻り値")
    print("=" * 50)
    result = extract_camera_model(TITLE_JP, DESCRIPTION_JP)
    print(repr(result))
    print()
    print(f"→ payload[\"C:Model\"] = {result!r}")


if __name__ == "__main__":
    test_extract_camera_model()
