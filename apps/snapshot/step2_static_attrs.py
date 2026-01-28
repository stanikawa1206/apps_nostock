# -*- coding: utf-8 -*-
"""
step2_static_attrs.py — Keepa/カタログ静的属性の抽出（category_pathは人間可読のパンくず文字列で返す）

返却フィールド:
- category_path: str | None        ← 例) "ビューティー › スキンケア・ボディケア › スキンケア・基礎化粧品 › フェイスパック"
- parent_asin: str | None
- images: List[str] | None         （各バリアントごとに最大1枚、最大10件）
- package_length_millimeters: int | None
- package_width_millimeters:  int | None
- package_height_millimeters: int | None
- package_weight_grams:       int | None
- isAdultProduct:             bool | None
"""

from __future__ import annotations
from typing import Any, Dict, Optional, List, Tuple

# ============ helpers: 汎用 ============

def _safe_first(x, default=None):
    if isinstance(x, list) and x:
        return x[0]
    return x if x is not None else default

def _to_int(v) -> Optional[int]:
    try:
        return int(round(float(v)))
    except Exception:
        return None

def _dim_to_mm(val) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, list) and val:
        val = _safe_first(val)
    if isinstance(val, dict):
        unit = (val.get("unit") or "").lower()
        v    = val.get("value")
        try:
            f = float(v)
        except Exception:
            return None
        if unit in ("mm", "millimeter", "millimeters"): return _to_int(f)
        if unit in ("cm", "centimeter", "centimeters"):  return _to_int(f * 10.0)
        if unit in ("m", "meter", "meters"):             return _to_int(f * 1000.0)
        return _to_int(f)
    return _to_int(val)

def _wt_to_g(val) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, list) and val:
        val = _safe_first(val)
    if isinstance(val, dict):
        unit = (val.get("unit") or "").lower()
        v    = val.get("value")
        try:
            f = float(v)
        except Exception:
            return None
        if unit in ("g", "gram", "grams"):              return _to_int(f)
        if unit in ("kg", "kilogram", "kilograms"):     return _to_int(f * 1000.0)
        if unit in ("mg", "milligram", "milligrams"):   return _to_int(f / 1000.0)
        return _to_int(f)
    return _to_int(val)

# ============ helpers: 画像関連 ============

def _is_fullsize(link: str) -> bool:
    # Amazonのサムネイルは "._SL" を含むことが多い → それを避ける
    return isinstance(link, str) and "._SL" not in link

def _area(im: Dict[str, Any]) -> int:
    try:
        return int(im.get("width", 0)) * int(im.get("height", 0))
    except Exception:
        return 0

def _pick_best_per_variant_any_shape(images_any: Any, marketplace_id: str) -> List[str]:
    """
    画像構造のゆらぎを吸収して、バリアントごとに最大1枚を選抜。
    - 形1: [{"marketplaceId": "...", "images":[{link,...},...]}, ...]
    - 形2: [{link,...}, ...]
    - 形3: {"marketplaceId": "...", "images":[...]}
    選び方: フルサイズ優先 → 面積最大 → MAIN/PT順で並べる
    """
    flat: List[Dict[str, Any]] = []
    if isinstance(images_any, list):
        # ブロック配列
        if images_any and isinstance(images_any[0], dict) and "images" in images_any[0] and "marketplaceId" in images_any[0]:
            chosen = None
            for b in images_any:
                if b.get("marketplaceId") == marketplace_id:
                    chosen = b
                    break
            if not chosen:
                chosen = images_any[0]
            flat = list(chosen.get("images") or [])
        else:
            # そのまま画像配列
            flat = images_any[:]
    elif isinstance(images_any, dict) and "images" in images_any:
        # 単一ブロック
        flat = list(images_any.get("images") or [])

    by_variant: Dict[str, List[Dict[str, Any]]] = {}
    for im in flat:
        if not isinstance(im, dict):
            continue
        link = im.get("link")
        if not isinstance(link, str):
            continue
        var = im.get("variant") or "UNKNOWN"
        by_variant.setdefault(var, []).append(im)

    selected: List[Tuple[str, Dict[str, Any]]] = []
    for var, lst in by_variant.items():
        fulls = [im for im in lst if _is_fullsize(im.get("link", ""))]
        pool  = fulls if fulls else lst
        best  = max(pool, key=_area, default=None)
        if best:
            selected.append((var, best))

    def var_key(v: str) -> Tuple[int, str]:
        if v == "MAIN":
            return (0, v)
        if isinstance(v, str) and v.startswith("PT"):
            try:
                return (1, f"{int(v[2:]):04d}")
            except Exception:
                return (1, v)
        return (2, v)

    selected.sort(key=lambda t: var_key(t[0]))
    return [im["link"] for _, im in selected]

# ============ helpers: マーケットブロック ============

def _pick_market_block(blocks: Any, marketplace_id: str) -> Optional[Dict[str, Any]]:
    """
    Keepa/SP-APIの応答で、marketplaceId対応ブロックを抽出。
    """
    if isinstance(blocks, list) and blocks:
        for b in blocks:
            if isinstance(b, dict) and b.get("marketplaceId") == marketplace_id:
                return b
        return blocks[0] if isinstance(blocks[0], dict) else None
    return blocks if isinstance(blocks, dict) else None

# ============ 表示用（パンくず作成） ============

def _category_path_to_string(cat_path_list: List[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(cat_path_list, list):
        return None
    names: List[str] = []
    for node in cat_path_list:
        if isinstance(node, dict):
            name = node.get("name")
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
    return " > ".join(names) if names else None


# ============ public API ============

def extract_step2(payload: Dict[str, Any], marketplace_id: str) -> Dict[str, Any]:
    """
    ASIN単位の catalog/keepa ペイロードから静的属性を抽出。
    ※ category_path は人間可読のパンくず文字列で返す。
    """
    item = payload or {}
    attrs = item.get("attributes") or {}

    # ---- classifications → category_path(list) を構築 ----
    category_path_list: Optional[List[Dict[str, Any]]] = None
    cls_block = _pick_market_block(item.get("classifications") or item.get("classification"), marketplace_id)
    if cls_block:
        nodes = cls_block.get("classifications") or cls_block.get("nodes") or []
        if isinstance(nodes, list):
            for node in nodes:
                path: List[Dict[str, Any]] = []
                cur = node
                while isinstance(cur, dict):
                    name = cur.get("displayName") or cur.get("name")
                    cid  = cur.get("classificationId") or cur.get("id")
                    if name or cid:
                        path.insert(0, {"catId": cid, "name": name})
                    cur = cur.get("parent")
                if path:
                    category_path_list = path
                    break

    # 最終的に category_path はパンくず文字列で返す
    category_path_str = _category_path_to_string(category_path_list) if category_path_list else None

    # ---- relationships → parent_asin ----
    parent_asin = None
    rel_block = _pick_market_block(item.get("relationships"), marketplace_id)
    if rel_block:
        rels = rel_block.get("relationships") or rel_block.get("Relations") or []
        for rel in rels:
            if rel.get("type") == "VARIATION":
                pas = rel.get("parentAsins") or rel.get("parentAsin")
                if isinstance(pas, list) and pas:
                    parent_asin = pas[0]
                    break
                if isinstance(pas, str) and pas:
                    parent_asin = pas
                    break

    # ---- images ----
    images = None
    img_any = item.get("images")
    if img_any:
        picked = _pick_best_per_variant_any_shape(img_any, marketplace_id)
        if picked:
            images = picked[:10]

    # ---- package dimensions ----
    pkg_dims = attrs.get("itemPackageDimensions") or attrs.get("item_package_dimensions") or attrs.get("package_dimensions")
    pkg_dims = _safe_first(pkg_dims, {}) if isinstance(pkg_dims, list) else (pkg_dims or {})
    length_mm = _dim_to_mm(pkg_dims.get("length"))
    width_mm  = _dim_to_mm(pkg_dims.get("width"))
    height_mm = _dim_to_mm(pkg_dims.get("height"))

    # ---- weight ----
    weight_g: Optional[int] = None
    for k in ("itemPackageWeight", "itemWeight", "item_package_weight", "item_weight", "package_weight"):
        v = attrs.get(k)
        if v is not None:
            weight_g = _wt_to_g(v)
            if weight_g is not None:
                break

    # ---- isAdult ----
    is_adult = None
    sm_block = _pick_market_block(item.get("summaries"), marketplace_id)
    if sm_block and "adultProduct" in sm_block:
        is_adult = bool(sm_block.get("adultProduct"))
    else:
        ia = attrs.get("isAdultProduct") or attrs.get("is_adult_product") or attrs.get("adult_product")
        if isinstance(ia, dict) and "value" in ia:
            is_adult = bool(ia["value"])
        elif isinstance(ia, bool):
            is_adult = ia

    # ---- return (category_path はパンくず文字列) ----
    return {
        "category_path": category_path_str,
        "parent_asin": parent_asin,
        "images": images,
        "package_length_millimeters": length_mm,
        "package_width_millimeters":  width_mm,
        "package_height_millimeters": height_mm,
        "package_weight_grams":       weight_g,
        "isAdultProduct":             is_adult,
    }
