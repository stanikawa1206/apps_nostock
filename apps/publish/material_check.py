# -*- coding: utf-8 -*-
"""
危険素材・規制素材の判定（2026-11-04）。

一次判定: 日本語タイトル・説明文・素材欄(item_attributes)のキーワードを前後の語で分類する。
  - REAL   : 「素材：〇〇」「本〇〇」「〇〇革/レザー/製」、エルメスの革素材名、毛皮の動物名 など明確な記載
  - STYLE  : 「〇〇型押し/柄/調/風/模様/モチーフ/プリント」「フェイク/エコ〇〇」「本〇〇ではなく」など
  - UNKNOWN: どちらでもない
二次判定: UNKNOWN があり、一次で NG・保留が決まらない商品だけ、写真と説明文を AI で確認する。

素材の方針（POLICY）:
  - NG   : 本物なら出品しない（CITES 附属書 I・II・III、eBay 禁止）
  - DECL : 本物なら保留。米国の輸入時に USFWS 申告(Form 3-177)・指定港通関・輸入ライセンスが必要で、
           日本からの直送では出品者側で対応できない（50 CFR 14.4/14.61/14.91。2026-11-04 確認）
  - FUR  : 本物の毛皮。未使用 → NG（加州 AB44 は新品の毛皮製品の販売を禁止、中古は除外）、
           使用歴不明 → 保留、中古 → 家畜種（ラビット）だけ OK、それ以外の動物は DECL と同じく保留

結果:
  - NG   : 出品しない（出品状況 NG(危険素材)）
  - HOLD : 判断不能・AIエラー・記載の矛盾・輸入申告が必要・使用歴不明（出品状況 保留(素材確認)）
  - OK   : 規制素材の語が無い、STYLE だけ、AI が全て対象外と高確信度で判定、中古のラビット毛皮
  一次の REAL は AI が対象外と答えても覆らない（AI は UNKNOWN の時だけ呼ぶ）。

判定結果は trx.material_check に保存し、同じ文面の商品は再判定しない（AI を呼ばない）。
手動で確認した結果は stage=9 で保存すると、文面が変わってもそのまま使われる。
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ------------------------------------------------------------------
# 素材の定義: (キー, 方針, 日本語の正規表現)
# ------------------------------------------------------------------
_FUR_RX = (
    r"ミンク(?!オイル)|フォックス(?:ファー|毛)?(?![ァ-ヴー])|セーブル|"
    r"(?<![A-Za-z])(?:FOX|Fox|MINK|Mink|SABLE|CHINCHILLA|Chinchilla|RABBIT|Rabbit)(?:[ 	]*(?:FUR|Fur))?(?![A-Za-z])|チンチラ|(?<!ピーター)ラビット(?:ファー|毛)?|レッキス|ラクーン(?:ファー|毛)?|"
    r"(?<!起)毛皮(?!革)|リアルファー|"
    # 単独の「ファー」（シェーファー・ファーバーカステル・タイムグラファー等のカタカナ語の一部は除く）
    r"(?<![ァ-ヴー])ファー(?=$|[^ァ-ヴー]|チャーム|コート|バッグ|ストール|マフラー|ティペット|ショール|ベスト|ポンポン|"
    r"キーホルダー|キーリング|ブレス|バングル|ブローチ|ピアス|イヤ|カフ|ジャケット|カラー|トリム|ポーチ)"
)
MATERIALS: List[Tuple[str, str, str]] = [
    ("象牙", "NG", r"象牙(?!ヤシ|椰子|色)|ぞうげ|マンモス(?:の)?牙|セイウチ(?:の)?牙"),
    ("べっ甲", "NG", r"べっ甲|鼈甲|べっこう|ベッコウ|タイマイ|トータス"),
    ("サンゴ", "NG", r"珊瑚(?!礁)|さんご(?!しょう)|サンゴ(?!礁)"),
    ("鯨", "NG", r"鯨(?:の)?(?:髭|ひげ|骨|歯)|クジラ(?:の)?(?:髭|ひげ|骨|歯)"),
    ("象皮", "NG", r"象革|ゾウ革|象皮|エレファント(?:レザー|革|スキン)"),
    ("ワニ", "NG", r"クロコダイル|クロコ(?!ス|ッ)|ワニ(?!ス)|鰐|アリゲーター|カイマン|ポロサス|ニロティカス"),
    ("トカゲ", "NG", r"リザード|トカゲ|蜥蜴"),
    ("ヘビ", "NG", r"パイソン|(?<!ー)ヘビ(?!ー|ィ)|蛇(?!腹|の目|口|行|足)|(?<!アイヤーズ)(?<!エイヤーズ)スネーク|コブラ(?!ー)|エラフェ"),
    ("アイヤーズ", "DECL", r"アイヤーズ|エイヤーズ|カロン"),
    ("エイ", "DECL", r"スティングレイ|ガルーシャ|シャグリーン|エイ(?:の)?(?:革|皮|レザー|スキン)"),
    ("サメ", "DECL", r"シャーク(?:スキン|革|レザー)|サメ革|鮫革|鮫皮"),
    ("オーストリッチ", "DECL", r"オーストリッチ|ダチョウ|駝鳥"),
    ("毛皮", "FUR", _FUR_RX),
]
POLICY: Dict[str, str] = {k: p for k, p, _ in MATERIALS}
_MAT_RE = [(k, p, re.compile(rx)) for k, p, rx in MATERIALS]
NG_MATERIALS = {k for k, p in POLICY.items() if p == "NG"}

# 輸入申告が必要な素材を保留にするか（False にすると DECL の素材と中古の毛皮は出品可になる）
HOLD_WILDLIFE_DECLARATION = True

# 毛皮の動物名（語 → 種）。家畜種は米国の申告・ライセンスの対象外（50 CFR 14.4 European rabbit）
_FUR_SPECIES = [
    (re.compile(r"ミンク|mink", re.I), "ミンク"), (re.compile(r"フォックス|fox", re.I), "フォックス"),
    (re.compile(r"セーブル|sable", re.I), "セーブル"), (re.compile(r"チンチラ|chinchilla", re.I), "チンチラ"),
    (re.compile(r"ラビット|レッキス|rabbit", re.I), "ラビット"), (re.compile(r"ラクーン"), "ラクーン"),
]
DOMESTIC_FUR = {"ラビット"}
_FUR_STYLE_AFTER = re.compile(r"^[\s　]*(?:の)?(?:調|柄|風|プリント|モチーフ|ライク|タッチ|フェイク|ではな|模様)")
_FUR_AFTER = re.compile(r"^[\s　・]*(?:の)?(?:毛|ファー|尻尾|しっぽ|テール|100|FUR|Fur)")
# 動物名だけの記載（ラビット・フォックス・英語の FOX 等）は、素材欄・毛の記載がある時だけ本物の毛皮とする
_FUR_BARE_SPECIES = re.compile(r"^(?:ラビット|フォックス|FOX|Fox|MINK|Mink|SABLE|CHINCHILLA|Chinchilla|RABBIT|Rabbit)$")
_FUR_MATERIAL_AFTER = re.compile(r"^[\s　・]*(?:の)?(?:製|使用|100|素材)")
# 羊・山羊など家畜の毛付き革（ムートン等）は毛皮として扱わない（AB44・米国の申告とも対象外）
_FUR_STYLE_BEFORE = re.compile(r"(?:ムートン|シープ(?:スキン)?|ボア|ラム)[\s　]*$")

# 前後の文脈（同じ行・句読点までの範囲で見る）
_CUT = "、。,，\n/／|｜"
_STYLE_AFTER = re.compile(
    r"(?:型押|かたおし|風|調|柄|模様|もよう|モチーフ|プリント|エンボス|デザイン|ライク|トーン|系|フェイク|"
    r"チェーン|ウッド|シェイプ|ロゴ|カラー|色|ではなく|ではありません|ではない|不使用|は使用して)"
)
_STYLE_BEFORE = re.compile(r"(?:フェイク|エコ|合皮|合成|型押し?|プリント|エンボス|イミテーション|人工|模造|ボア|フリース)[\s　]*$")
_REAL_AFTER = re.compile(r"^[\s　・]*(?:の)?(?:革|皮|レザー|スキン|製|素材|使用|本体)")
_REAL_BEFORE = re.compile(
    r"(?:(?:素材|材質|マテリアル|表革|アッパー|ベルト素材|本体素材|レザー素材)(?:[\s　]*[：:】]|[\s　]+)[^、。\n]{0,12}"
    r"|(?:(?<!日)本|天然|リアル|本物の|純正の)[\s　]*)$"
)
_REAL_BEFORE_STRICT = re.compile(r"(?:(?<!日)本|天然|リアル|本物の)[\s　]*$")
# エルメス等で素材名として使われる語（色欄・品名に書かれていても革素材）
_LEATHER_NAMES = re.compile(r"^(?:リザード|アリゲーター|クロコダイル|クロコ|パイソン|ポロサス|ニロティカス|オーストリッチ)$")
_HERMES = re.compile(r"エルメス|HERM[EÈ]S", re.IGNORECASE)
# 珊瑚と一緒に書かれた装身具（色名ではなく珊瑚の石を使った宝飾品）
_CORAL_JEWELRY = re.compile(r"ブローチ|ネックレス|ペンダント|リング|指輪|ピアス|イヤリング|ブレスレット|帯留|かんざし|簪|ルース|カボション|数珠|根付|ネクタイピン|タイピン|カフス")
_HASHTAG = re.compile(r"#\S+")

# 使用歴（毛皮のみ）
_RANK_LINE = re.compile(r"^\s*[\[【(（]?(?:[SABCDN]{1,2}[+\-]?|ランク)[\]】)）]?\s*[…:：.．\-ー]|ランク基準|状態ランクの目安")
_UNUSED = re.compile(
    r"(?<!ほぼ)(?<!ほとんど)(?:新品|未使用|未着用|展示品|デッドストック|タグ付き)"
    r"(?!に近|品に近|級|同様|近い|ではな|ではありません|と同様|で購入|購入|時|当時|価格|定価)"
)
_NEAR = re.compile(r"未使用に近|未使用品に近|ほぼ未使用|未使用級|新品同様|新品に近|ほぼ新品")
_USED = re.compile(
    r"(?:数回使用|数回着用|何度か使用|使用感|使用しました|USED品|着用しました|使用回数|使用に伴う|使用済|使用していました|愛用|使用による)"
    r"(?!はありません|なし|のない|はない|はほとんど)"
)


@dataclass
class Hit:
    material: str
    word: str
    kind: str  # REAL / STYLE / UNKNOWN
    context: str


@dataclass
class MaterialCheckResult:
    result: str  # OK / NG / HOLD
    stage: int  # 0=該当なし 1=キーワード 2=AI 9=手動
    reason: str = ""
    hits: List[Hit] = field(default_factory=list)
    ai_model: Optional[str] = None
    ai_result: Optional[Dict[str, Any]] = None
    ai_input_tokens: Optional[int] = None
    ai_output_tokens: Optional[int] = None
    ai_sec: Optional[float] = None
    cached: bool = False
    usage: str = "UNKNOWN"  # 毛皮の使用歴 UNUSED / USED / UNKNOWN

    @property
    def materials(self) -> List[str]:
        out: List[str] = []
        for h in self.hits:
            if h.material not in out:
                out.append(h.material)
        return out

    def style_only_materials(self) -> set:
        kinds: Dict[str, set] = {}
        for h in self.hits:
            kinds.setdefault(h.material, set()).add(h.kind)
        return {m for m, k in kinds.items() if k == {"STYLE"}}


def _attrs_text(attrs: Any) -> str:
    """item_attributes などの入れ子から文字列だけを集める。"""
    out: List[str] = []

    def walk(x: Any) -> None:
        if isinstance(x, str):
            out.append(x)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, (list, tuple)):
            for v in x:
                walk(v)

    walk(attrs)
    return " ".join(out)


def build_text(title_jp: str, description: str, attributes: Any = None) -> str:
    parts = [title_jp or "", description or ""]
    at = _attrs_text(attributes) if attributes else ""
    if at:
        parts.append("素材情報: " + at)
    return "\n".join(parts)


def text_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _is_keyword_list_line(line: str) -> bool:
    """説明文末尾の検索用キーワード羅列（短い語を空白で25個以上並べた行。【項目】の行は除く）。"""
    s = line.strip()
    if s.startswith("【") or "。" in s:
        return False
    toks = [t for t in re.split(r"[\s　]+", s) if t]
    return len(toks) >= 25 and sum(len(t) for t in toks) / len(toks) <= 6


def _window_after(text: str, end: int, n: int = 12) -> str:
    s = text[end:end + n]
    for i, ch in enumerate(s):
        if ch in _CUT:
            return s[:i]
    return s


def _window_before(text: str, start: int, n: int = 20) -> str:
    s = text[max(0, start - n):start]
    for i in range(len(s) - 1, -1, -1):
        if s[i] in _CUT:
            return s[i + 1:]
    return s


def detect_hits(title_jp: str, description: str, attributes: Any = None) -> List[Hit]:
    title_jp = _HASHTAG.sub(" ", title_jp or "")
    description = _HASHTAG.sub(" ", description or "")
    text = build_text(title_jp, description, attributes)
    hermes = bool(_HERMES.search(text))
    title_end = len(title_jp)
    hits: List[Hit] = []
    for key, policy, rx in _MAT_RE:
        for m in rx.finditer(text):
            ls = text.rfind("\n", 0, m.start()) + 1
            le = text.find("\n", m.end())
            line = text[ls:le if le >= 0 else len(text)]
            if m.start() > title_end and _is_keyword_list_line(line):
                continue
            a = _window_after(text, m.end())
            b = _window_before(text, m.start())
            word = m.group(0)
            if policy == "FUR":
                # 毛皮は直後の語だけで柄を判定する（「リアルファー ズッカ柄」の柄は毛皮のことではない）
                if _FUR_STYLE_AFTER.search(a) or _STYLE_BEFORE.search(b) or _FUR_STYLE_BEFORE.search(b):
                    kind = "STYLE"
                elif word == "毛皮":
                    # 「毛皮」だけの記載はブランドの沿革などにも出るため、素材として書かれている時だけ本物とする
                    kind = "REAL" if (_REAL_BEFORE.search(b) or _FUR_MATERIAL_AFTER.search(a)) else "UNKNOWN"
                elif word == "ファー":
                    kind = "REAL" if _REAL_BEFORE_STRICT.search(b) else "UNKNOWN"
                elif _FUR_BARE_SPECIES.match(word):
                    kind = "REAL" if (_REAL_BEFORE.search(b) or _FUR_AFTER.search(a)) else "UNKNOWN"
                else:
                    kind = "REAL"  # ミンク・フォックス・毛皮・リアルファー等は、柄・フェイクの記載が無ければ本物の毛皮
            elif _STYLE_AFTER.search(a) or _STYLE_BEFORE.search(b):
                kind = "STYLE"
            elif key == "サンゴ" and (_CORAL_JEWELRY.search(a) or _CORAL_JEWELRY.search(b)):
                kind = "REAL"
            elif policy == "DECL":
                kind = "REAL"  # エイ革・ガルーシャ・オーストリッチ等は素材名そのもの。型押し・柄の記載が無ければ本物
            elif _REAL_AFTER.search(a) or _REAL_BEFORE.search(b):
                kind = "REAL"
            elif hermes and _LEATHER_NAMES.match(word):
                kind = "REAL"
            else:
                kind = "UNKNOWN"
            hits.append(Hit(key, word, kind, (b + "【" + word + "】" + a).replace("\n", " ")))
    return hits


def fur_species(hits: Sequence[Hit]) -> set:
    out = set()
    for h in hits:
        if h.material == "毛皮" and h.kind == "REAL":
            for rx, sp in _FUR_SPECIES:
                if rx.search(h.word):
                    out.add(sp)
    return out


def usage_state(title_jp: str, description: str, item_condition_id: Optional[int] = None) -> str:
    """
    毛皮の使用歴。UNUSED（新品・未使用）/ USED（中古）/ UNKNOWN（未使用に近い・記載なし・矛盾）。
    出品状態(item_condition_id: 1=新品未使用 2=未使用に近い 3〜6=中古)を優先し、説明文の記載で補う。
    店のランク基準表（S…未使用品 など全ランクの説明）は使用歴の記載として扱わない。
    """
    lines = [ln for ln in ((title_jp or "") + "\n" + (description or "")).split("\n") if not _RANK_LINE.search(ln)]
    text = "\n".join(lines)
    near = bool(_NEAR.search(text))
    unused = bool(_UNUSED.search(_NEAR.sub(" ", text)))
    used = bool(_USED.search(text))
    try:
        cond = int(item_condition_id) if item_condition_id is not None else None
    except (TypeError, ValueError):
        cond = None
    if cond == 1:
        return "UNUSED"
    if cond == 2:
        return "UNKNOWN"
    if cond is not None and cond >= 3:
        return "UNKNOWN" if unused else "USED"
    if unused and not used:
        return "UNUSED"
    if near:
        return "UNKNOWN"
    if used:
        return "USED"
    return "UNKNOWN"


def _fur_rule(usage: str, species: set) -> Tuple[str, str]:
    sp = "・".join(sorted(species)) or "動物不明"
    if usage == "UNUSED":
        return "NG", f"毛皮({sp}): 未使用の本物の毛皮（加州AB44の対象）"
    if usage == "UNKNOWN":
        return "HOLD", f"毛皮({sp}): 本物の毛皮で使用歴が不明"
    if species and species <= DOMESTIC_FUR:
        return "OK", f"毛皮({sp}): 中古の家畜種の毛皮"
    if HOLD_WILDLIFE_DECLARATION:
        return "HOLD", f"毛皮({sp}): 中古だが米国輸入時にUSFWS申告・指定港通関が必要"
    return "OK", f"毛皮({sp}): 中古の毛皮"


def _decl_rule(material: str) -> Tuple[str, str]:
    if HOLD_WILDLIFE_DECLARATION:
        return "HOLD", f"{material}: 本物。米国輸入時にUSFWS申告(Form 3-177)・指定港通関・輸入ライセンスが必要"
    return "OK", f"{material}: 本物（出品可の素材）"


_RANK = {"NG": 3, "HOLD": 2, "AI": 1, "OK": 0}


def _worst(items: Sequence[Tuple[str, str]]) -> Tuple[str, str]:
    if not items:
        return "OK", ""
    best = max(items, key=lambda x: _RANK[x[0]])
    same = [r for s, r in items if s == best[0] and r]
    return best[0], " / ".join(dict.fromkeys(same))


def judge_stage1(hits: Sequence[Hit], usage: str = "UNKNOWN") -> Tuple[str, str]:
    """
    戻り値: (判定, 理由)。判定は OK / NG / HOLD / AI（二次判定が必要）。
    """
    decisions: List[Tuple[str, str]] = []
    by_mat: Dict[str, List[Hit]] = {}
    for h in hits:
        by_mat.setdefault(h.material, []).append(h)
    for mat, hs in by_mat.items():
        policy = POLICY[mat]
        kinds = {h.kind for h in hs}
        real = [h for h in hs if h.kind == "REAL"]
        if policy == "FUR":
            if real and "STYLE" in kinds and not fur_species(hs):
                decisions.append(("HOLD", f"毛皮: 本物とフェイク・柄の記載が矛盾 {real[0].context}"))
            elif real:
                decisions.append(_fur_rule(usage, fur_species(hs)))
            elif "UNKNOWN" in kinds:
                decisions.append(("AI", "毛皮: 本物の毛皮か判断できない記載"))
            else:
                decisions.append(("OK", "毛皮: フェイク・柄のみ"))
            continue
        if real and "STYLE" in kinds:
            # 例: タイトルは「型押し クロコ」、説明文は「クロコのレザーベルト」。本物とも柄とも決められないので保留
            decisions.append(("HOLD", f"{mat}: 本物と型押し・柄の記載が矛盾 {real[0].context}"))
        elif real:
            if policy == "NG":
                decisions.append(("NG", f"{mat}: 本物の記載 {real[0].context}"))
            else:
                decisions.append(_decl_rule(mat))
        elif "UNKNOWN" in kinds:
            decisions.append(("AI", f"{mat}: 本物か柄か判断できない記載"))
        else:
            decisions.append(("OK", f"{mat}: 型押し・柄・色名のみ"))
    return _worst(decisions)


def ai_materials(hits: Sequence[Hit]) -> List[str]:
    """AI に確認する素材（REAL が無く UNKNOWN がある素材）。"""
    out = set()
    by_mat: Dict[str, set] = {}
    for h in hits:
        by_mat.setdefault(h.material, set()).add(h.kind)
    for mat, kinds in by_mat.items():
        if "UNKNOWN" in kinds and "REAL" not in kinds:
            out.add(mat)
    return sorted(out)


# ------------------------------------------------------------------
# 二次判定（AI）
# ------------------------------------------------------------------
AI_MODEL = "gpt-4.1-mini"
AI_MAX_IMAGES = 4
AI_IMAGE_PX = 768
AI_MIN_CONFIDENCE = 0.8
AI_TIMEOUT_SEC = 60

_AI_PROMPT = """You check Japanese second-hand items before export to the US (eBay). Decide, for each suspected material, whether the item is ACTUALLY made of that animal material, or only uses the name as an embossed/printed pattern, a colour name, a model/motif name, or an imitation (resin, celluloid, metal, faux leather, faux/eco fur, synthetic boa).
Suspected materials (Japanese): {mats}
(象牙=ivory, べっ甲=hawksbill tortoiseshell, サンゴ=precious coral, 鯨=whale, 象皮=elephant skin, ワニ=crocodile/alligator/caiman, トカゲ=lizard, ヘビ=snake/python, アイヤーズ=Ayers/karung snake, エイ=stingray, サメ=shark, オーストリッチ=ostrich, 毛皮=real animal fur)
Use the Japanese title, description and material info, and the photos (texture, scale pattern, tags, stamps, certificates).
Rules: "型押し/〇〇柄/〇〇調/風/モチーフ/カラー" = not real. Hermès lizard/alligator/crocodile in any field = real. Tortoiseshell is real only for 本べっ甲/鼈甲 (hawksbill), not resin/celluloid "トータス". Coral is real for 珊瑚 stones in jewellery, not colour names. Fur: "フェイクファー/エコファー/ボア" = not real; for real fur give the animal (mink, fox, rabbit, sable, chinchilla, raccoon, unknown) in "species".
If text and photos are insufficient, answer "unknown" — do not guess.
Return ONLY JSON: {{"materials":[{{"material":"<one of the Japanese names above>","verdict":"real|not_real|unknown","species":"","evidence":"short, cite text or photo","confidence":0.0-1.0}}]}}

TITLE: {title}
DESCRIPTION:
{desc}
{attrs}"""

_AI_SPECIES = {"mink": "ミンク", "fox": "フォックス", "rabbit": "ラビット", "sable": "セーブル", "chinchilla": "チンチラ", "raccoon": "ラクーン"}


def _image_b64(url: str, max_px: int = AI_IMAGE_PX) -> Optional[str]:
    import requests
    from PIL import Image

    r = requests.get(url, timeout=15)
    r.raise_for_status()
    im = Image.open(io.BytesIO(r.content)).convert("RGB")
    im.thumbnail((max_px, max_px))
    b = io.BytesIO()
    im.save(b, "JPEG", quality=80)
    return base64.b64encode(b.getvalue()).decode()


def call_ai(client, materials: Sequence[str], title_jp: str, description: str, attributes: Any,
            image_urls: Sequence[str]) -> Tuple[Dict[str, Any], int, int, float]:
    at = _attrs_text(attributes) if attributes else ""
    prompt = _AI_PROMPT.format(mats="、".join(materials), title=title_jp or "", desc=(description or "")[:3000],
                               attrs=("MATERIAL INFO: " + at[:800]) if at else "")
    content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
    for u in list(image_urls or [])[:AI_MAX_IMAGES]:
        try:
            content.append({"type": "input_image", "image_url": "data:image/jpeg;base64," + _image_b64(u), "detail": "auto"})
        except Exception as e:
            print(f"[MATERIAL_AI] 画像取得失敗 {u}: {e}", flush=True)
    n_images = len(content) - 1
    t0 = time.time()
    resp = client.responses.create(model=AI_MODEL, input=[{"role": "user", "content": content}],
                                   temperature=0, timeout=AI_TIMEOUT_SEC)
    sec = round(time.time() - t0, 1)
    raw = (resp.output_text or "").strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    data = json.loads(raw)
    if isinstance(data, dict):
        data["_images"] = n_images
    usage = getattr(resp, "usage", None)
    return data, getattr(usage, "input_tokens", 0) or 0, getattr(usage, "output_tokens", 0) or 0, sec


def _ai_by_material(ai: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_mat: Dict[str, Dict[str, Any]] = {}
    for x in (ai or {}).get("materials") or []:
        if isinstance(x, dict) and x.get("material"):
            by_mat.setdefault(str(x["material"]), x)
    return by_mat


def _ai_species(x: Dict[str, Any]) -> set:
    s = str(x.get("species") or "").lower()
    return {jp for en, jp in _AI_SPECIES.items() if en in s}


def judge_ai(ai: Dict[str, Any], materials: Sequence[str], usage: str = "UNKNOWN") -> Tuple[str, str]:
    by_mat = _ai_by_material(ai)
    decisions: List[Tuple[str, str]] = []
    for mat, x in by_mat.items():
        if x.get("verdict") != "real" or mat not in POLICY:
            continue
        policy = POLICY[mat]
        if policy == "NG":
            decisions.append(("NG", f"AI: {mat} 本物 {x.get('evidence', '')}"))
        elif policy == "FUR":
            s, r = _fur_rule(usage, _ai_species(x))
            decisions.append((s, "AI: " + r))
        else:
            s, r = _decl_rule(mat)
            decisions.append((s, "AI: " + r))
    for m in materials:
        x = by_mat.get(m)
        if x is None:
            decisions.append(("HOLD", f"AI: {m} の回答なし"))
        elif x.get("verdict") == "real":
            continue
        elif x.get("verdict") != "not_real":
            decisions.append(("HOLD", f"AI: {m} 判断不能 {x.get('evidence', '')}"))
        else:
            try:
                conf = float(x.get("confidence") or 0)
            except (TypeError, ValueError):
                conf = 0.0
            if conf < AI_MIN_CONFIDENCE:
                decisions.append(("HOLD", f"AI: {m} 対象外だが確信度{conf:.2f} {x.get('evidence', '')}"))
            elif (ai or {}).get("_images") == 0:
                # 写真を確認できていない（仕入先の画像が取れない等）。文面だけでは対象外と決めない
                decisions.append(("HOLD", f"AI: {m} 対象外だが写真を確認できていない {x.get('evidence', '')}"))
            else:
                decisions.append(("OK", f"AI: {m} 対象外 {x.get('evidence', '')}"))
    return _worst(decisions)


def check_material(title_jp: str, description: str, attributes: Any = None, image_urls: Sequence[str] = (),
                   client_factory=None, item_condition_id: Optional[int] = None) -> MaterialCheckResult:
    hits = detect_hits(title_jp, description, attributes)
    usage = usage_state(title_jp, description, item_condition_id) if any(h.material == "毛皮" for h in hits) else "UNKNOWN"
    s1, reason = judge_stage1(hits, usage)
    if s1 in ("OK", "NG", "HOLD"):
        return MaterialCheckResult(s1, 1 if hits else 0, reason, hits, usage=usage)

    mats = ai_materials(hits)
    res = MaterialCheckResult("HOLD", 2, reason, hits, ai_model=AI_MODEL, usage=usage)
    last_err = ""
    for _attempt in range(2):
        try:
            if client_factory is None:
                from apps.common.utils import get_openai_client as client_factory  # noqa: N813
            data, tin, tout, sec = call_ai(client_factory(), mats, title_jp, description, attributes, image_urls)
            res.ai_result, res.ai_input_tokens, res.ai_output_tokens, res.ai_sec = data, tin, tout, sec
            # 一次で OK だった素材（柄のみ等）と AI の結果を合わせる
            s1_ok = judge_stage1([h for h in hits if h.material not in mats], usage)
            res.result, res.reason = _worst([s1_ok, judge_ai(data, mats, usage)])
            return res
        except Exception as e:
            last_err = repr(e)[:300]
            print(f"[MATERIAL_AI] エラー: {last_err}", flush=True)
    res.result, res.reason = "HOLD", f"AIエラー: {last_err}"
    return res


# ------------------------------------------------------------------
# DB（trx.material_check）
# ------------------------------------------------------------------
def load_cached(conn, vendor_name: str, vendor_item_id: str, thash: str) -> Optional[MaterialCheckResult]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT text_hash, result, stage, reason, ai_result FROM trx.material_check WHERE vendor_name=? AND vendor_item_id=?",
            (vendor_name, vendor_item_id),
        )
        row = cur.fetchone()
    if not row:
        return None
    h, result, stage, reason = row[0], row[1], int(row[2]), row[3] or ""
    try:
        ai = json.loads(row[4]) if row[4] else None
    except (TypeError, ValueError):
        ai = None
    if stage == 9 or h == thash:
        return MaterialCheckResult(result, stage, reason, ai_result=ai, cached=True)
    if result == "NG":
        # 文面が変わっても、一度本物と判定した商品は出品しない
        return MaterialCheckResult("NG", stage, "前回NG: " + reason, cached=True)
    return None


_SQL_SAVE = """
MERGE trx.material_check AS t
USING (SELECT ? AS vendor_name, ? AS vendor_item_id) AS s
   ON t.vendor_name = s.vendor_name AND t.vendor_item_id = s.vendor_item_id
WHEN MATCHED THEN UPDATE SET text_hash=?, result=?, stage=?, materials=?, reason=?, hits=?,
     ai_model=?, ai_result=?, ai_input_tokens=?, ai_output_tokens=?, ai_sec=?, checked_at=SYSDATETIME()
WHEN NOT MATCHED THEN INSERT (vendor_name, vendor_item_id, text_hash, result, stage, materials, reason, hits,
     ai_model, ai_result, ai_input_tokens, ai_output_tokens, ai_sec, checked_at)
     VALUES (s.vendor_name, s.vendor_item_id, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATETIME());
"""


def save_result(conn, vendor_name: str, vendor_item_id: str, thash: str, r: MaterialCheckResult) -> None:
    vals = [
        thash, r.result, r.stage, "、".join(r.materials)[:200] or None, (r.reason or "")[:1000],
        json.dumps([[h.material, h.kind, h.context] for h in r.hits], ensure_ascii=False)[:4000] or None,
        r.ai_model if r.stage == 2 else None,
        json.dumps(r.ai_result, ensure_ascii=False) if r.ai_result is not None else None,
        r.ai_input_tokens, r.ai_output_tokens, r.ai_sec,
    ]
    with conn.cursor() as cur:
        cur.execute(_SQL_SAVE, [vendor_name, vendor_item_id] + vals + vals)


def check_material_with_cache(conn, vendor_name: str, vendor_item_id: str, title_jp: str, description: str,
                              attributes: Any = None, image_urls: Sequence[str] = (),
                              item_condition_id: Optional[int] = None) -> MaterialCheckResult:
    """
    規制素材の語が無い商品は DB を読み書きしない（大半の商品はここで終わる）。
    語がある商品は、同じ文面の判定結果があればそれを使い、無ければ判定して保存する。
    """
    hits = detect_hits(title_jp, description, attributes)
    if not hits:
        return MaterialCheckResult("OK", 0, "", [])
    thash = text_hash(build_text(title_jp, description, attributes))
    cached = load_cached(conn, vendor_name, vendor_item_id, thash)
    if cached is not None:
        cached.hits = hits
        if any(h.material == "毛皮" for h in hits):
            cached.usage = usage_state(title_jp, description, item_condition_id)
        return cached
    r = check_material(title_jp, description, attributes, image_urls, item_condition_id=item_condition_id)
    save_result(conn, vendor_name, vendor_item_id, thash, r)
    return r


# ------------------------------------------------------------------
# 英語タイトル・説明文の素材名
#   従来は crocodile/python 等を一律に消していた（本物でも柄でも区別せず、素材名を隠して出品していた）。
#   本物の規制素材は出品前に止まるので、ここに来るのは型押し・柄の商品、誤訳、出品可の本物の動物素材。
#   - 日本語で柄・型押しと分かる素材: 単独の素材名に "Pattern" を付ける（例: Crocodile Leather → Crocodile Pattern Leather）
#   - 日本語に記載が無いのに「〇〇 Leather/Skin」とある誤訳: 素材名を消す（例: Elephant Leather → Leather）
#   - 出品可の本物の動物素材: 英語タイトルに動物名を入れ、説明文に素材・学名・毛皮の原産国を書く
#     （eBay 動物製品ポリシーは種の明記、米国の毛皮製品表示法は動物名と原産国の表示を求める）
#   - 象牙(Ivory) は色名のため変更しない
# ------------------------------------------------------------------
_EN_WORDS = [
    (r"Crocodile|Croc|Alligator|Caiman", "ワニ"),
    (r"Lizard", "トカゲ"),
    (r"Python|Snakeskin|Snake\s*Skin", "ヘビ"),
    (r"Ostrich", "オーストリッチ"),
    (r"Stingray|Galuchat", "エイ"),
    (r"Elephant", "象皮"),
    (r"Shark", "サメ"),
]
_EN_STYLE_FOLLOW = r"(?![\s-]*(?:Embossed|Emboss|Pattern|Print(?:ed)?|Effect|Motif|Style|Look|Design|Texture|Stamped|Grain))"

# 出品可の本物の動物素材 → (タイトルに必要な語, タイトルに付ける語, 説明文に書く文)
_LABELS: Dict[str, Tuple[str, str, str]] = {
    "ラビット": (r"\bRabbit\b.*\bFur\b|\bFur\b.*\bRabbit\b", "Rabbit Fur",
              "Material: Genuine rabbit fur (Oryctolagus cuniculus, domestic rabbit). Fur origin: Unknown."),
    "ミンク": (r"\bMink\b", "Mink Fur", "Material: Genuine mink fur (Neovison vison). Fur origin: Unknown."),
    "フォックス": (r"\bFox\b", "Fox Fur", "Material: Genuine fox fur (Vulpes). Fur origin: Unknown."),
    "セーブル": (r"\bSable\b", "Sable", "Material: Contains genuine sable (Martes zibellina). Fur origin: Unknown."),
    "チンチラ": (r"\bChinchilla\b", "Chinchilla Fur", "Material: Genuine chinchilla fur (Chinchilla lanigera). Fur origin: Unknown."),
    "ラクーン": (r"\bRaccoon\b", "Raccoon Fur", "Material: Genuine raccoon fur. Fur origin: Unknown."),
    "オーストリッチ": (r"\bOstrich\b", "Ostrich Leather", "Material: Genuine ostrich leather (Struthio camelus)."),
    "エイ": (r"\bStingray\b|\bGaluchat\b", "Stingray Leather", "Material: Genuine stingray leather (galuchat)."),
    "サメ": (r"\bShark\b", "Shark Leather", "Material: Genuine shark leather."),
    "アイヤーズ": (r"\bAyers\b|\bKarung\b", "Ayers Snake Leather", "Material: Genuine Ayers snake leather (Acrochordus)."),
}


def adjust_title_materials(title_en: str, r: Optional[MaterialCheckResult]) -> str:
    s = title_en or ""
    found = set(r.materials) if r else set()
    style_only = r.style_only_materials() if r else set()
    for words, mat in _EN_WORDS:
        if mat in style_only:
            s = re.sub(rf"\b({words})\b{_EN_STYLE_FOLLOW}", r"\1 Pattern", s, flags=re.IGNORECASE)
        elif mat not in found:
            s = re.sub(rf"\b(?:{words})\b[\s-]*(?=(?:Leather|Skin|Hide)\b)", "", s, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", s).strip()


def material_labels(r: Optional[MaterialCheckResult]) -> List[str]:
    """出品する（OK の）商品に含まれる本物の動物素材のラベルキー。"""
    if r is None or r.result != "OK":
        return []
    keys: List[str] = []
    real_mats = {h.material for h in r.hits if h.kind == "REAL"}
    for mat, x in _ai_by_material(r.ai_result).items():
        if x.get("verdict") == "real":
            real_mats.add(mat)
    for mat in real_mats:
        if POLICY.get(mat) == "DECL" and mat in _LABELS:
            keys.append(mat)
    if "毛皮" in real_mats:
        sp = fur_species(r.hits)
        for x in _ai_by_material(r.ai_result).values():
            if x.get("material") == "毛皮" and x.get("verdict") == "real":
                sp |= _ai_species(x)
        keys.extend(sorted(s for s in sp if s in _LABELS))
    return sorted(set(keys))


def _trim_words(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    cut = s[:limit]
    if " " in cut and not s[limit:limit + 1] in ("", " "):
        cut = cut[: cut.rfind(" ")]
    return cut.rstrip(" ,-/")


def ensure_title_labels(title_en: str, keys: Sequence[str], limit: int = 80) -> str:
    s = (title_en or "").strip()
    for k in keys:
        need, add, _ = _LABELS[k]
        if not re.search(need, s, flags=re.IGNORECASE):
            s = _trim_words(s, limit - len(add) - 1) + " " + add
    return s.strip()


def description_material_note(keys: Sequence[str]) -> str:
    return "\n".join(_LABELS[k][2] for k in keys)
