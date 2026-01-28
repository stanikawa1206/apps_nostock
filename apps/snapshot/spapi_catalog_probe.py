# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

ASIN = "B08LGN5VQZ"
MARKETPLACE_ID = "A1VC38T7YXB528"

# ★ここをあなたの実ファイル名に合わせて固定する
DUMP_PATH = Path(r"D:\apps_nostock\apps\snapshot\catalog_payload_B08LGN5VQZ_A1VC38T7YXB528.json")


def _pick_market_block(blocks: Any, marketplace_id: str) -> Optional[Dict[str, Any]]:
    if isinstance(blocks, list) and blocks:
        for b in blocks:
            if isinstance(b, dict) and b.get("marketplaceId") == marketplace_id:
                return b
        return blocks[0] if isinstance(blocks[0], dict) else None
    return blocks if isinstance(blocks, dict) else None


def _attr_val(attrs: Dict[str, Any], key: str):
    if not isinstance(attrs, dict):
        return None
    x = attrs.get(key)
    if isinstance(x, dict):
        return x.get("value") or x.get("name") or x.get("displayName")
    if isinstance(x, list) and x:
        xx = x[0]
        if isinstance(xx, dict):
            return xx.get("value") or xx.get("name") or xx.get("displayName")
        return xx
    return x


def _extract_identifiers(payload: Dict[str, Any]) -> Optional[str]:
    blocks = payload.get("identifiers")
    block = _pick_market_block(blocks, MARKETPLACE_ID) if blocks else None
    if not isinstance(block, dict):
        return None

    idlist = block.get("identifiers")
    if not isinstance(idlist, list):
        return None

    prefer = {"UPC", "GTIN", "EAN", "JAN"}
    for ident in idlist:
        if not isinstance(ident, dict):
            continue
        t = (ident.get("identifierType") or ident.get("type") or "").upper()
        v = ident.get("identifier") or ident.get("value")
        if v and t in prefer:
            return str(v)
    return None


def extract_title_brand_upc(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = {"title": None, "brand": None, "upc": None}

    sm = _pick_market_block(payload.get("summaries"), MARKETPLACE_ID)
    if isinstance(sm, dict):
        out["title"] = sm.get("itemName") or sm.get("displayName") or out["title"]
        out["brand"] = sm.get("brand") or out["brand"]

    attrs = payload.get("attributes") or {}
    if not out["title"]:
        out["title"] = _attr_val(attrs, "item_name") or _attr_val(attrs, "itemName") or _attr_val(attrs, "title")
        if isinstance(out["title"], list) and out["title"]:
            out["title"] = out["title"][0]
    if not out["brand"]:
        out["brand"] = _attr_val(attrs, "brand") or _attr_val(attrs, "manufacturer")

    out["upc"] = _extract_identifiers(payload)

    return out


def main() -> None:
    if not DUMP_PATH.exists():
        raise SystemExit(f"[ERROR] Dump not found: {DUMP_PATH}")

    with DUMP_PATH.open(encoding="utf-8") as f:
        payload = json.load(f)

    out = extract_title_brand_upc(payload)
    out["asin"] = ASIN
    out["_dump_used"] = str(DUMP_PATH)

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
