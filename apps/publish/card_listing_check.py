# -*- coding: utf-8 -*-
"""
トレカ（トレカ・遊戯王カード）の出品前判定（2026-11-24）。

eBay のカテゴリー 183454 は単品カード（CCG Individual Cards）なので、BOX・パック・まとめ等を出すと
カテゴリー違い（LP_SBM_Miscat）になる。また鑑定品は Condition（2750 + 鑑定機関・グレード）を
正しく渡せないものは出品しない。

- 単品以外: 商品の「種別」属性（メルカリ・メルカリshops とも）が「シングルカード 1枚」以外なら NG。
  属性が無い（出品者が未入力）ときはタイトルで判定する。「スペシャルBOX」「BOX購入キャンペーン PROMO」の
  ような単品プロモがあるので、プロモ表記があれば BOX・パックの語では NG にしない。
- 鑑定品:
  - メルカリshops は「グレード」属性が「PSA8以下」のような区分しか無く、正確なグレードが分からないので、
    タイトルか属性に鑑定の表記があれば NG（当面）。
  - メルカリは「グレード」属性を publish_ebay.parse_card_grade_text で解析でき、鑑定機関・グレードが
    eBay の値に対応するときだけ出品する。タイトルに鑑定の表記があるのに属性が無い・解析できないときは NG。
説明文は見ない（「鑑定には出していません」等の記載で誤って止めないため）。
"""
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

CARD_CATEGORY_GROUPS = ("トレカ", "遊戯王カード")
SINGLE_CARD = "シングルカード 1枚"

NG_HEAD_NOT_SINGLE = "NG(トレカ単品以外)"
NG_HEAD_GRADE = "NG(トレカ鑑定情報)"

# 単品以外のタイトル表記（種別属性が無いときだけ使う）
_NOT_SINGLE_RE = re.compile(
    r"まとめ|セット|大量|引退|オリパ|デッキ|コンプ|シュリンク|未開封|カートン|ボックス|BOX|パック"
    r"|(?<!\d)(?:[2-9]|\d{2,})\s*枚",
    re.IGNORECASE,
)
# BOX・パックの語を含んでも単品のことがある表記（プロモ・拡張パック名など）
_PROMO_RE = re.compile(r"プロモ|PROMO", re.IGNORECASE)
# 単品カードのタイトルに入る収録商品名（「強化拡張パック ○○」「スタートデッキ100」など）は判定から外す
_PACK_NAME_RE = re.compile(
    r"(?:強化|拡張|ハイクラス|ブースター|スペシャル|プレミアム)\s*(?:拡張)?\s*(?:パック|BOX)|スタートデッキ",
    re.IGNORECASE,
)
_PROMO_SAFE_RE = re.compile(r"BOX|ボックス|パック", re.IGNORECASE)

# 鑑定の表記（タイトル）。鑑定機関は直後にグレードの数字があるときだけ（「PCG-P」のプロモ番号、
# 「ACE SPEC」等に当てない）。メルカリの「あんしん鑑定」（真贋鑑定サービス）は鑑定品ではないので除く。
_GRADE_RE = re.compile(
    r"(?<![A-Za-z])(?:PSA|BGS|BCCG|BVG|CGC|SGC|ARS|PCG|KSA|GMA|HGA)\s*\d|鑑定",
    re.IGNORECASE,
)


def shops_attributes_to_item_attributes(product_detail: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    メルカリshops の productDetail.variants[0].attributes（{"title", "values": {id: {"text"}}}）を
    メルカリの item_attributes と同じ形（{"text", "values": [{"text"}]}）にする。
    """
    variants = (product_detail or {}).get("variants") or []
    attrs = (variants[0] or {}).get("attributes") if variants else None
    out: List[Dict[str, Any]] = []
    for a in attrs or []:
        values = a.get("values") or {}
        vals = values.values() if isinstance(values, dict) else values
        out.append({"text": a.get("title"), "values": [{"text": (v or {}).get("text")} for v in vals]})
    return out


def _attr(attrs: Optional[List[Dict[str, Any]]], name: str) -> Optional[List[str]]:
    for a in attrs or []:
        if a.get("text") == name:
            return [str((v or {}).get("text") or "") for v in (a.get("values") or [])]
    return None


def _title_not_single(title: str) -> Optional[str]:
    t = title or ""
    if _PROMO_RE.search(t):
        # プロモ表記があれば BOX・パックの語は単品プロモの入手経路とみなし、ほかの語だけで判定する
        t = _PROMO_SAFE_RE.sub("", t)
    t = _PACK_NAME_RE.sub("", t)
    m = _NOT_SINGLE_RE.search(t)
    return m.group(0) if m else None


def card_listing_ng(
    vendor_name: str,
    title_jp: Optional[str],
    attrs: Optional[List[Dict[str, Any]]],
    parse_grade: Callable[[str], Tuple[Optional[str], Optional[str]]],
    grader_ids: Dict[str, str],
    grade_ids: Dict[str, str],
) -> Optional[Tuple[str, str]]:
    """
    出品しない場合は (listing_head, 理由) を返す。出品してよければ None。
    attrs はメルカリの item_attributes 形式（メルカリshops は shops_attributes_to_item_attributes で変換したもの）。
    """
    title = title_jp or ""

    kinds = _attr(attrs, "種別")
    if kinds:
        if SINGLE_CARD not in kinds:
            return NG_HEAD_NOT_SINGLE, f"種別={'/'.join(kinds)}"
    else:
        hit = _title_not_single(title)
        if hit:
            return NG_HEAD_NOT_SINGLE, f"タイトルに「{hit}」（種別の属性なし）"

    grades = _attr(attrs, "グレード")
    title_grade = _GRADE_RE.search(title.replace("あんしん鑑定", ""))
    if vendor_name == "メルカリshops":
        if grades or title_grade:
            g = "/".join(grades) if grades else title_grade.group(0)
            return NG_HEAD_GRADE, f"メルカリshopsの鑑定品は正確なグレードが取れないため出品しない（{g}）"
        return None

    if grades:
        grader, num = parse_grade(grades[0])
        if not (grader_ids.get(grader or "") and grade_ids.get(num or "")):
            return NG_HEAD_GRADE, f"グレードを解析できない（{grades[0]}）"
        return None
    if title_grade:
        return NG_HEAD_GRADE, f"タイトルに「{title_grade.group(0)}」があるがグレードの属性なし"
    return None
