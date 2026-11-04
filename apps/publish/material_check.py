# -*- coding: utf-8 -*-
"""
危険素材・規制素材の判定（2026-11-04）。

一次判定: 日本語タイトル・説明文・素材欄(item_attributes)のキーワードを前後の語で分類する。
  - REAL   : 「素材：〇〇」「本〇〇」「〇〇革/レザー/製」、エルメスの革素材名 など明確な記載
  - STYLE  : 「〇〇型押し/柄/調/風/模様/モチーフ/プリント」「本〇〇ではなく」など
  - UNKNOWN: どちらでもない
二次判定: NG対象の素材に UNKNOWN があり REAL が無い商品だけ、写真と説明文を AI で確認する。

結果:
  - NG   : 本物の規制素材（一次の REAL、または AI が real）。AI が対象外と答えても一次の REAL は覆らない
  - HOLD : AI が unknown・確信度不足・エラー・回答の欠落（安全側で出品しない）
  - OK   : 規制素材の語が無い、STYLE だけ、または AI が全て対象外と高確信度で判定

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
#   方針 NG    : 本物なら出品しない（CITES 附属書 I・II・III、eBay 禁止）
#        ALLOW : 出品は止めない（CITES 非掲載。判定結果は記録する）
# ------------------------------------------------------------------
MATERIALS: List[Tuple[str, str, str]] = [
    ("象牙", "NG", r"象牙(?!ヤシ|椰子|色)|ぞうげ|マンモス(?:の)?牙|セイウチ(?:の)?牙"),
    ("べっ甲", "NG", r"べっ甲|鼈甲|べっこう|ベッコウ|タイマイ|トータス"),
    ("サンゴ", "NG", r"珊瑚(?!礁)|さんご(?!しょう)|サンゴ(?!礁)"),
    ("鯨", "NG", r"鯨(?:の)?(?:髭|ひげ|骨|歯)|クジラ(?:の)?(?:髭|ひげ|骨|歯)"),
    ("象皮", "NG", r"象革|ゾウ革|象皮|エレファント(?:レザー|革|スキン)"),
    ("ワニ", "NG", r"クロコダイル|クロコ(?!ス|ッ)|ワニ(?!ス)|鰐|アリゲーター|カイマン|ポロサス|ニロティカス"),
    ("トカゲ", "NG", r"リザード|トカゲ|蜥蜴"),
    ("ヘビ", "NG", r"パイソン|(?<!ー)ヘビ(?!ー|ィ)|蛇(?!腹|の目|口|行|足)|スネーク|コブラ(?!ー)|エラフェ|カロン"),
    ("エイ", "ALLOW", r"スティングレイ|ガルーシャ|シャグリーン|エイ(?:の)?(?:革|皮|レザー|スキン)"),
    ("サメ", "ALLOW", r"シャーク(?:スキン|革|レザー)|サメ革|鮫革|鮫皮"),
    ("毛皮", "ALLOW", r"ミンク(?!オイル)|フォックス(?:ファー|毛)|セーブル|チンチラ|ラビットファー|毛皮|リアルファー|ラクーン"),
    ("オーストリッチ", "ALLOW", r"オーストリッチ|ダチョウ|駝鳥"),
]
_MAT_RE = [(k, p, re.compile(rx)) for k, p, rx in MATERIALS]
NG_MATERIALS = {k for k, p, _ in MATERIALS if p == "NG"}

# 前後の文脈（同じ行・句読点までの範囲で見る）
_CUT = "、。,，\n/／|｜"
_STYLE_AFTER = re.compile(
    r"(?:型押|かたおし|風|調|柄|模様|もよう|モチーフ|プリント|エンボス|デザイン|ライク|トーン|系|フェイク|"
    r"チェーン|ウッド|シェイプ|ロゴ|カラー|色|ではなく|ではありません|ではない|不使用|は使用して)"
)
_STYLE_BEFORE = re.compile(r"(?:フェイク|エコ|合皮|合成|型押し?|プリント|エンボス|イミテーション|人工|模造)[\s　]*$")
_REAL_AFTER = re.compile(r"^[\s　・]*(?:の)?(?:革|皮|レザー|スキン|製|素材|使用|本体)")
_REAL_BEFORE = re.compile(
    r"(?:(?:素材|材質|マテリアル|表革|アッパー|ベルト素材|本体素材|レザー素材)(?:[\s　]*[：:]|[\s　]+)[^、。\n]{0,12}"
    r"|(?:(?<!日)本|天然|リアル|本物の|純正の)[\s　]*)$"
)
# エルメス等で素材名として使われる語（色欄・品名に書かれていても革素材）
_LEATHER_NAMES = re.compile(r"^(?:リザード|アリゲーター|クロコダイル|クロコ|パイソン|ポロサス|ニロティカス|オーストリッチ)$")
_HERMES = re.compile(r"エルメス|HERM[EÈ]S", re.IGNORECASE)
_HASHTAG = re.compile(r"#\S+")


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
    for key, _policy, rx in _MAT_RE:
        for m in rx.finditer(text):
            ls = text.rfind("\n", 0, m.start()) + 1
            le = text.find("\n", m.end())
            line = text[ls:le if le >= 0 else len(text)]
            if m.start() > title_end and _is_keyword_list_line(line):
                continue
            a = _window_after(text, m.end())
            b = _window_before(text, m.start())
            if _STYLE_AFTER.search(a) or _STYLE_BEFORE.search(b):
                kind = "STYLE"
            elif _REAL_AFTER.search(a) or _REAL_BEFORE.search(b):
                kind = "REAL"
            elif hermes and _LEATHER_NAMES.match(m.group(0)):
                kind = "REAL"
            else:
                kind = "UNKNOWN"
            hits.append(Hit(key, m.group(0), kind, (b + "【" + m.group(0) + "】" + a).replace("\n", " ")))
    return hits


def judge_stage1(hits: Sequence[Hit]) -> Tuple[str, str]:
    """
    戻り値: (判定, 理由)。判定は OK / NG / HOLD（本物と柄の記載が矛盾）/ AI（二次判定が必要）。
    """
    ng_hits = [h for h in hits if h.material in NG_MATERIALS]
    real = [h for h in ng_hits if h.kind == "REAL"]
    style_mats = {h.material for h in ng_hits if h.kind == "STYLE"}
    conflict = [h for h in real if h.material in style_mats]
    if conflict:
        # 例: タイトルは「型押し クロコ」、説明文は「クロコのレザーベルト」。本物とも柄とも決められないので保留
        h = conflict[0]
        return "HOLD", f"{h.material}: 本物と型押し・柄の記載が矛盾 {h.context}"
    if real:
        h = real[0]
        return "NG", f"{h.material}: 本物の記載 {h.context}"
    unknown = [h for h in ng_hits if h.kind == "UNKNOWN"]
    if unknown:
        return "AI", "、".join(sorted({h.material for h in unknown})) + ": 本物か柄か判断できない記載"
    if ng_hits:
        return "OK", "、".join(sorted({h.material for h in ng_hits})) + ": 型押し・柄・色名のみ"
    if hits:
        return "OK", "、".join(sorted({h.material for h in hits})) + ": 出品可の素材（記録のみ）"
    return "OK", ""


# ------------------------------------------------------------------
# 二次判定（AI）
# ------------------------------------------------------------------
AI_MODEL = "gpt-4.1-mini"
AI_MAX_IMAGES = 4
AI_IMAGE_PX = 768
AI_MIN_CONFIDENCE = 0.8
AI_TIMEOUT_SEC = 60

_AI_PROMPT = """You check Japanese second-hand items before export to the US (eBay). Decide, for each suspected material, whether the item is ACTUALLY made of that animal material, or only uses the name as an embossed/printed pattern, a colour name, a model/motif name, or an imitation (resin, celluloid, metal, faux leather).
Suspected materials (Japanese): {mats}
(象牙=ivory, べっ甲=hawksbill tortoiseshell, サンゴ=precious coral, 鯨=whale, 象皮=elephant skin, ワニ=crocodile/alligator/caiman, トカゲ=lizard, ヘビ=snake/python)
Use the Japanese title, description and material info, and the photos (texture, scale pattern, tags, stamps, certificates).
Rules: "型押し/〇〇柄/〇〇調/風/モチーフ/カラー" = not real. Hermès lizard/alligator/crocodile in any field = real. Tortoiseshell is real only for 本べっ甲/鼈甲 (hawksbill), not resin/celluloid "トータス". Coral is real for 珊瑚 stones in jewellery, not colour names.
If text and photos are insufficient, answer "unknown" — do not guess.
Return ONLY JSON: {{"materials":[{{"material":"<one of the Japanese names above>","verdict":"real|not_real|unknown","evidence":"short, cite text or photo","confidence":0.0-1.0}}]}}

TITLE: {title}
DESCRIPTION:
{desc}
{attrs}"""


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
    t0 = time.time()
    resp = client.responses.create(model=AI_MODEL, input=[{"role": "user", "content": content}],
                                   temperature=0, timeout=AI_TIMEOUT_SEC)
    sec = round(time.time() - t0, 1)
    raw = (resp.output_text or "").strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    data = json.loads(raw)
    usage = getattr(resp, "usage", None)
    return data, getattr(usage, "input_tokens", 0) or 0, getattr(usage, "output_tokens", 0) or 0, sec


def judge_ai(ai: Dict[str, Any], materials: Sequence[str]) -> Tuple[str, str]:
    by_mat: Dict[str, Dict[str, Any]] = {}
    for x in (ai or {}).get("materials") or []:
        if isinstance(x, dict) and x.get("material"):
            by_mat.setdefault(str(x["material"]), x)
    for x in by_mat.values():
        if x.get("verdict") == "real":
            return "NG", f"AI: {x.get('material')} 本物 {x.get('evidence', '')}"
    for m in materials:
        x = by_mat.get(m)
        if x is None:
            return "HOLD", f"AI: {m} の回答なし"
        if x.get("verdict") != "not_real":
            return "HOLD", f"AI: {m} 判断不能 {x.get('evidence', '')}"
        try:
            conf = float(x.get("confidence") or 0)
        except (TypeError, ValueError):
            conf = 0.0
        if conf < AI_MIN_CONFIDENCE:
            return "HOLD", f"AI: {m} 対象外だが確信度{conf:.2f} {x.get('evidence', '')}"
    return "OK", "AI: " + " / ".join(f"{m} 対象外 {by_mat[m].get('evidence', '')}" for m in materials)


def check_material(title_jp: str, description: str, attributes: Any = None, image_urls: Sequence[str] = (),
                   client_factory=None) -> MaterialCheckResult:
    hits = detect_hits(title_jp, description, attributes)
    s1, reason = judge_stage1(hits)
    if s1 in ("OK", "NG", "HOLD"):
        return MaterialCheckResult(s1, 1 if hits else 0, reason, hits)

    mats = sorted({h.material for h in hits if h.material in NG_MATERIALS and h.kind == "UNKNOWN"})
    res = MaterialCheckResult("HOLD", 2, reason, hits, ai_model=AI_MODEL)
    last_err = ""
    for _attempt in range(2):
        try:
            if client_factory is None:
                from apps.common.utils import get_openai_client as client_factory  # noqa: N813
            data, tin, tout, sec = call_ai(client_factory(), mats, title_jp, description, attributes, image_urls)
            res.ai_result, res.ai_input_tokens, res.ai_output_tokens, res.ai_sec = data, tin, tout, sec
            res.result, res.reason = judge_ai(data, mats)
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
            "SELECT text_hash, result, stage, reason FROM trx.material_check WHERE vendor_name=? AND vendor_item_id=?",
            (vendor_name, vendor_item_id),
        )
        row = cur.fetchone()
    if not row:
        return None
    h, result, stage, reason = row[0], row[1], int(row[2]), row[3] or ""
    if stage == 9 or h == thash:
        return MaterialCheckResult(result, stage, reason, cached=True)
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
                              attributes: Any = None, image_urls: Sequence[str] = ()) -> MaterialCheckResult:
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
        return cached
    r = check_material(title_jp, description, attributes, image_urls)
    save_result(conn, vendor_name, vendor_item_id, thash, r)
    return r


# ------------------------------------------------------------------
# 英語タイトルの素材名
#   従来は crocodile/python 等を一律に消していた（本物でも柄でも区別せず、素材名を隠して出品していた）。
#   本物の規制素材は出品前に止まるので、ここに来るのは型押し・柄の商品か、日本語に記載の無い誤訳。
#   - 日本語で柄・型押しと分かる素材: 単独の素材名に "Pattern" を付ける（例: Crocodile Leather → Crocodile Pattern Leather）
#   - 日本語に記載が無いのに「〇〇 Leather/Skin」とある誤訳: 素材名を消す（例: Elephant Leather → Leather）
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
