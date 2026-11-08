# -*- coding: utf-8 -*-
"""
偽物ポリシー（eBay Counterfeit items policy）対策（2026-11-08）。

背景: 2026-10-30〜11-05 に「Fake Pearl」（シャネル）・「FAKE NOT」（グッチ）の出品3件が
eBay に偽物ポリシー違反として削除された。英語タイトル・説明文の "Fake" は素材を表していても
偽ブランド品の表記と区別されない。

1) fix_faux_material_en: 素材を表す Fake 表現を正確な英語にする（Fake Pearl → Faux Pearl など）。
   フェイクパール・フェイクレザー等は「模造の素材」で、商品自体の真贋とは関係がない。
2) counterfeit_reason: 偽ブランド品の疑いがある商品を出品しない。
   - 日本語タイトル・説明文に、出品者自身が真贋を保証しない・本物ではないと書いている
     （「万一偽物の場合は返金」などの定型文は対象外）
   - 商品名に FAKE/NOT（グッチのコレクション名。eBay が同型番を2件偽物として削除した）
   - 素材の言い換え後も英語タイトルに fake / replica / counterfeit / knockoff が残る
   単語を消して出品することはしない（規制の回避になるため）。
"""

from __future__ import annotations

import re
from typing import Optional

# 素材を表す Fake（Fake の直後が素材名のものだけ。区切りの空白・ハイフン・スラッシュは許す）
_FAUX_MATERIALS = (
    r"pearls?|leather|fur|suede|feathers?|shearling|mouton|"
    r"crocodile|croc|alligator|python|snake(?:skin)?|ostrich|lizard|"
    r"wood|tortoise(?:shell)?"
)
_FAUX_RE = re.compile(r"\bfake[\s\-]*(" + _FAUX_MATERIALS + r")\b", re.IGNORECASE)
# 宝石・貴石の Fake は Simulated（FTC の宝飾品ガイドの表記）
_SIMULATED_RE = re.compile(
    r"\bfake[\s\-]*(diamonds?|gems?|gemstones?|stones?|rub(?:y|ies)|emeralds?|sapphires?|crystals?|jewels?)\b",
    re.IGNORECASE,
)
# 言い換え後に残っていれば偽物の疑いとする語（英語タイトル）
_EN_COUNTERFEIT_RE = re.compile(r"\b(?:fake|replica|counterfeit|knock[\s\-]?offs?|bootleg)\b", re.IGNORECASE)


def _keep_case(word: str, src: str) -> str:
    if src.isupper():
        return word.upper()
    if src.islower():
        return word.lower()
    return word


# 80文字で切られて末尾が "Fake" だけになった保存済みタイトル用（日本語の素材名から補う）
_JP_FAUX = [
    (re.compile(r"フェイク\s*パール|フェイクパール"), "Pearl"),
    (re.compile(r"フェイク\s*レザー"), "Leather"),
    (re.compile(r"フェイク\s*ファー"), "Fur"),
    (re.compile(r"フェイク\s*スエード|フェイク\s*スウェード"), "Suede"),
]
_TRAILING_FAKE_RE = re.compile(r"\bfake\s*$", re.IGNORECASE)


def fix_faux_material_en(text: str, title_jp: str = "") -> str:
    """
    素材を表す Fake を Faux / Simulated にする（Fake Pearl → Faux Pearl、Fake Diamond → Simulated Diamond）。
    title_jp を渡すと、末尾で切れた "... Metal Fake" も日本語の素材名（フェイクパール等）から Faux Pearl にする。
    """
    if not text:
        return text or ""
    s = _FAUX_RE.sub(lambda m: _keep_case("Faux", m.group(0)[:4]) + " " + m.group(1), text)
    s = _SIMULATED_RE.sub(lambda m: _keep_case("Simulated", m.group(0)[:4]) + " " + m.group(1), s)
    if title_jp and _TRAILING_FAKE_RE.search(s):
        for rx, mat in _JP_FAUX:
            if rx.search(title_jp):
                s = _TRAILING_FAKE_RE.sub(lambda m: _keep_case("Faux", m.group(0)[:4]) + " " + mat, s)
                break
    return s


# 日本語: 出品者自身が真贋を保証しない・本物ではないと書いている表現
_JP_ADMISSION_RE = re.compile(
    r"(?:本物|正規品|真贋|真偽)(?:か|かどうか)?(?:は|も)?"
    r"(?:わかりません|分かりません|わからない|分からない|不明です|不明の|不明な|"
    r"保証(?:は)?(?:できません|致しかねます|いたしかねます|しかねます|出来ません)|"
    r"確認(?:できて|出来て|して)(?:い|お)?(?:ません|りません))"
    r"|(?:偽物|コピー品|スーパーコピー|レプリカ)(?:の可能性|かもしれ|かも知れ|です|となります|になります)"
    r"|(?:本物|正規品)では(?:ありません|ない|御座いません|ございません)"
    r"|鑑定(?:は|を)?(?:して|受けて|出して)(?:い|お)?(?:ません|りません)"
    r"|スーパーコピー|N級品"
)
# 直前にあれば仮定の話（「万一偽物の場合は返金」等）として対象外にする語
_JP_CONDITIONAL_BEFORE = re.compile(r"(?:万が一|万一|もし|仮に)[、,\s　]*[^。！]{0,12}$")
# 直後が仮定・否定の続きなら対象外（「本物ではない場合」「正規品ではないと判断された場合」）
_JP_CONDITIONAL_AFTER = re.compile(r"^[^。！]{0,20}?(?:場合|際|とき|時|なら|であれば|と判断|と判明|と鑑定)")
_JP_TITLE_RE = re.compile(r"偽物|コピー品|スーパーコピー|レプリカ品|N級|ブランド風")
_FAKE_NOT_RE = re.compile(r"FAKE\s*[/／]?\s*NOT", re.IGNORECASE)


def _jp_admission(text: str) -> Optional[str]:
    for m in _JP_ADMISSION_RE.finditer(text or ""):
        before = text[max(0, m.start() - 20):m.start()]
        after = text[m.end():m.end() + 30]
        if _JP_CONDITIONAL_BEFORE.search(before) or _JP_CONDITIONAL_AFTER.search(after):
            continue
        return text[max(0, m.start() - 15):m.end() + 10].replace("\n", " ").strip()
    return None


def counterfeit_reason(title_jp: str, description: str, title_en: str = "") -> Optional[str]:
    """偽ブランド品の疑いがあれば理由（出品状況詳細に残す文字列）、無ければ None。"""
    title_jp = title_jp or ""
    description = description or ""
    if _FAKE_NOT_RE.search(title_jp) or _FAKE_NOT_RE.search(title_en or ""):
        return "商品名に FAKE/NOT（eBay が同型番を偽物ポリシー違反として削除済み）"
    m = _JP_TITLE_RE.search(title_jp)
    if m:
        return f"日本語タイトルに「{m.group(0)}」"
    hit = _jp_admission(title_jp + "\n" + description)
    if hit:
        return f"説明文に真贋を保証しない記載「{hit}」"
    m = _EN_COUNTERFEIT_RE.search(fix_faux_material_en(title_en or "", title_jp))
    if m:
        return f"英語タイトルに「{m.group(0)}」"
    return None
