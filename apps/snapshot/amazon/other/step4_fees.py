# -*- coding: utf-8 -*-

#   4. 販売手数料のシミュレーション (step4_fees.py)
#   SP-API（Fees Estimate API）を利用して、Amazonで販売した際の手数料を見積もるプログラムです。  
#   対象のASINと想定販売価格（日本円）をAPIに送信します。  
#   FBA手数料、カテゴリーごとの販売手数料（Referral Fee）、手数料の合計額を計算します。  
#   販売価格からトータル手数料を差し引いた「純利益（Net）」を算出して返却します。  

from __future__ import annotations
import json
from typing import Any, Dict, Optional, Tuple
from apps.common.spapi import SpapiSession

def get_fees_estimate(sp: SpapiSession, asin: str, price_jpy: int) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int], Dict[str,Any]]:
    path = f"/products/fees/v0/items/{asin}/feesEstimate"
    payload = {
        "FeesEstimateRequest":{
            "MarketplaceId": sp.MARKETPLACE_ID,
            "Identifier": asin,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees":{
                "ListingPrice":{"CurrencyCode":"JPY","Amount":float(price_jpy)},
                "Shipping":{"CurrencyCode":"JPY","Amount":0.0},
                "Points":{"PointsNumber":0}
            }
        }
    }
    body = json.dumps(payload, ensure_ascii=False, separators=(",",":"))
    r = sp.post(path, body)
    meta = {"status": r.status_code}
    try:
        js = r.json()
        meta["raw"] = js
        if r.status_code != 200:
            return None, None, None, None, meta
        fees = js["payload"]["FeesEstimateResult"]["FeesEstimate"]
        fee_list = fees["FeeDetailList"]

        fba_total = sum(float(f["FinalFee"]["Amount"])
                        for f in fee_list if f.get("FeeType","").upper().startswith("FBA"))
        ref_items = [f for f in fee_list if f.get("FeeType")=="ReferralFee"]
        referral_amt = float(ref_items[0]["FinalFee"]["Amount"]) if ref_items else 0.0
        total_fees = float(fees["TotalFeesEstimate"]["Amount"])
        net = float(price_jpy) - total_fees

        return round(fba_total), round(referral_amt), round(total_fees), round(net), meta
    except Exception:
        meta["raw"] = r.text
        return None, None, None, None, meta
