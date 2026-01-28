# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys, os
from typing import Any, Dict, List, Optional
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from apps.common.spapi import SpapiSession


def sp_get_catalog_item(sp: SpapiSession, asin: str) -> Dict[str, Any]:
    """
    SP-API Catalog Items API を叩いて、ASINの商品情報(payload)を取得して返す。
    """
    r = sp.get(
        f"/catalog/2022-04-01/items/{asin}",
        {
            "marketplaceIds": sp.MARKETPLACE_ID,
            "includedData": "summaries,attributes,identifiers",
            "locale": "ja_JP",
        },
    )
    if r.status_code != 200:
        raise SystemExit(f"[SPAPI Catalog] {r.status_code} {r.text}")

    j = r.json()
    return (j.get("payload") or j.get("Payload") or j) or {}


def _pick_market_block(blocks: Any, marketplace_id: str) -> Optional[Dict[str, Any]]:
    """
    list[{marketplaceId:..., ...}] の形なら該当marketplaceIdを優先して1件選ぶ。
    なければ先頭、dictならそのまま返す。
    """
    if isinstance(blocks, list) and blocks:
        for b in blocks:
            if isinstance(b, dict) and b.get("marketplaceId") == marketplace_id:
                return b
        return blocks[0] if isinstance(blocks[0], dict) else None
    return blocks if isinstance(blocks, dict) else None


def _attr_values_text(attrs: Dict[str, Any], key: str, marketplace_id: str) -> List[str]:
    """
    attrs[key] はだいたい list[dict{value, language_tag, marketplace_id}]。
    marketplace_id を優先して拾い、無ければ marketplace 無視で全部拾う。
    """
    if not isinstance(attrs, dict):
        return []
    x = attrs.get(key)
    if not isinstance(x, list) or not x:
        return []

    picked: List[str] = []
    for it in x:
        if not isinstance(it, dict):
            continue
        mp = it.get("marketplace_id") or it.get("marketplaceId")
        if mp and marketplace_id and mp != marketplace_id:
            continue
        v = it.get("value")
        if isinstance(v, str) and v.strip():
            picked.append(v.strip())

    # marketplace一致が0なら、mp無視で拾う
    if not picked:
        for it in x:
            if not isinstance(it, dict):
                continue
            v = it.get("value")
            if isinstance(v, str) and v.strip():
                picked.append(v.strip())

    return picked


def _extract_identifiers_upc_or_ean(payload: Dict[str, Any], marketplace_id: str) -> Optional[str]:
    """
    payload["identifiers"] から UPC/GTIN/EAN を拾う（優先順：UPC → GTIN → EAN）。
    """
    blk = _pick_market_block(payload.get("identifiers"), marketplace_id)
    if not isinstance(blk, dict):
        return None
    ids = blk.get("identifiers")
    if not isinstance(ids, list):
        return None

    for want in ("UPC", "GTIN", "EAN"):
        for it in ids:
            if not isinstance(it, dict):
                continue
            t = (it.get("identifierType") or "").upper()
            if t == want:
                v = it.get("identifier")
                if isinstance(v, str) and v.strip():
                    return v.strip()

    return None


def extract_step1(payload: Dict[str, Any], marketplace_id: str) -> Dict[str, Any]:
    """
    payloadから、title/brand/upc と bullet_points/description(bullet結合) を抽出して返す。
    """
    out: Dict[str, Any] = {
        "title": None,
        "brand": None,
        "upc": None,
        "bullet_points": None,  # List[str] or None
        "description": None,    # str or None（bulletを結合したもの）
    }

    # --- summaries（最優先）
    sm = _pick_market_block(payload.get("summaries"), marketplace_id)
    if isinstance(sm, dict):
        out["title"] = sm.get("itemName") or sm.get("displayName") or out["title"]
        out["brand"] = sm.get("brand") or out["brand"]

    # --- attributes fallback
    attrs = payload.get("attributes") or {}
    if isinstance(attrs, dict):
        if not out["title"]:
            item_name = _attr_values_text(attrs, "item_name", marketplace_id)
            if not item_name:
                item_name = _attr_values_text(attrs, "itemName", marketplace_id)
            out["title"] = item_name[0] if item_name else out["title"]

        if not out["brand"]:
            brand = _attr_values_text(attrs, "brand", marketplace_id)
            if not brand:
                brand = _attr_values_text(attrs, "manufacturer", marketplace_id)
            out["brand"] = brand[0] if brand else out["brand"]

        # bullet: bullet_point が基本。念のため bullet_points も見る
        bullets = _attr_values_text(attrs, "bullet_point", marketplace_id)
        if not bullets:
            bullets = _attr_values_text(attrs, "bullet_points", marketplace_id)

        if bullets:
            out["bullet_points"] = bullets
            out["description"] = "\n".join(bullets)

    # --- identifiers
    out["upc"] = _extract_identifiers_upc_or_ean(payload, marketplace_id)

    return out

def extract_title_brand_upc(payload: Dict[str, Any], marketplace_id: str) -> Dict[str, Any]:
    d = extract_step1(payload, marketplace_id)
    return {
        "title": d.get("title"),
        "brand": d.get("brand"),
        "upc": d.get("upc"),
        "bullet_points": d.get("bullet_points"),
        "description": d.get("description"),
    }

def fetch_step1(asin: str) -> Dict[str, Any]:
    """
    SP-APIで取得 → extract_step1 で抽出 → asinを付けて返す。
    """
    sp = SpapiSession()
    payload = sp_get_catalog_item(sp, asin)
    out = extract_step1(payload, sp.MARKETPLACE_ID)
    out["asin"] = asin
    return out


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("usage: python step1_catalog_core.py <ASIN>")

    asin = sys.argv[1].strip()
    out = fetch_step1(asin)
    print(json.dumps(out, ensure_ascii=False, indent=2))
