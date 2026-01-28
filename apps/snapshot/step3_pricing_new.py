# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Any, Dict
from apps.common.spapi import SpapiSession

def sp_get_item_offers_new(sp: SpapiSession, asin: str) -> Dict[str, Any]:
    r = sp.get(f"/products/pricing/v0/items/{asin}/offers", {
        "MarketplaceId": sp.MARKETPLACE_ID,
        "ItemCondition":"New"
    })
    if r.status_code != 200:
        raise SystemExit(f"[SPAPI Pricing] {r.status_code} {r.text}")
    return r.json()

def _yen_int(x) -> int:
    return max(int(round(float(x))), 0)

def parse_pricing(pr: Dict[str,Any]) -> Dict[str,Any]:
    payload = pr.get("payload") or {}
    summary = payload.get("Summary") or {}
    offers  = payload.get("Offers", []) or []

    # BB(New)がなければ件数だけ返す
    bbs_all = summary.get("BuyBoxPrices", []) or []
    bbs_new = [bb for bb in bbs_all if bb.get("condition") == "New"]
    if not (bbs_new and offers):
        cnt_total = len(offers)
        cnt_fba   = sum(1 for o in offers if o.get("IsFulfilledByAmazon"))
        cnt_fbm   = cnt_total - cnt_fba
        return {
            "buybox_price_jpy": None,
            "buybox_seller_id": None,
            "buybox_is_fba": None,
            "buybox_is_backorder": None,
            "buybox_availability_message": None,
            "count_new_total": cnt_total,
            "count_new_fba": cnt_fba,
            "count_new_fbm": cnt_fbm,
            "new_current_price": None,
        }

    # AFN優先でBB決定
    bbs_new.sort(key=lambda x: 0 if (x.get("fulfillmentType")=="AFN") else 1)
    bb = bbs_new[0]
    bb_price_jpy = _yen_int((bb.get("LandedPrice") or {}).get("Amount"))

    # 勝者
    winner = next((o for o in offers if o.get("IsBuyBoxWinner")), None)
    if not winner:  # 念のため
        winner = offers[0]

    bb_is_fba = bool(winner.get("IsFulfilledByAmazon"))

    ship_av = (winner.get("ShippingTime") or {}).get("availabilityType")  # NOW/FUTURE...
    list_av = ((winner.get("Listing") or {}).get("Availability") or {}).get("Type")   # BACK_ORDER
    if ship_av in ("FUTURE", "FUTURE_WITH_DATE") or list_av == "BACK_ORDER":
        is_backorder = True
    elif ship_av == "NOW":
        is_backorder = False
    else:
        is_backorder = None

    cnt_total = len(offers)
    cnt_fba   = sum(1 for o in offers if o.get("IsFulfilledByAmazon"))
    cnt_fbm   = cnt_total - cnt_fba

    return {
        "buybox_price_jpy": bb_price_jpy,
        "buybox_seller_id": winner.get("SellerId"),
        "buybox_is_fba": bb_is_fba,
        "buybox_is_backorder": is_backorder,
        "buybox_availability_message": None,
        "count_new_total": cnt_total,
        "count_new_fba": cnt_fba,
        "count_new_fbm": cnt_fbm,
        "new_current_price": bb_price_jpy,
    }
