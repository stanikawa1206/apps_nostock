# -*- coding: utf-8 -*-
"""
step5_keepa_supplement_strict_v2.py — 
“フォールバックなし＋構造確認ダンプ付き”版 for Keepa JSON抽出

仕様：
- 新品90日在庫切れ率は stats.outOfStockPercentage90 の
  “index=1（New）”のみを採用。その他キーは無視。
- 値が存在しない／型が不正／-1（不明値）なら None。
- それ以外のフィールドも、指定キー／指定型のみを採用。
- さらに “キー一覧ダンプ” 機能付きで、どのキーが入っているか確認可能。
"""

from __future__ import annotations
from typing import Any, Dict, Optional
from apps.common.keepa import KeepaClient
import json
import argparse
import os
import sys

def _as_product(raw: dict) -> dict:
    if isinstance(raw, dict) and isinstance(raw.get("products"), list) and raw["products"]:
        p = raw["products"][0]
        return p if isinstance(p, dict) else {}
    return raw if isinstance(raw, dict) else {}

def _num(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    return None

def _int(x: Any) -> Optional[int]:
    if isinstance(x, (int, float)):
        return int(x)
    return None

def _bool(x: Any) -> Optional[bool]:
    return x if isinstance(x, bool) else None

def _str(x: Any) -> Optional[str]:
    return x if isinstance(x, str) else None

def _get_oos90_new_pct(stats: dict) -> Optional[float]:
    # 本線：stats.outOfStockPercentage90 の配列 index=1 (New)
    arr = stats.get("outOfStockPercentage90")
    if isinstance(arr, list) and len(arr) > 1:
        v = _num(arr[1])
        if v is not None and v >= 0:
            return v
    return None

def dump_stats_keys(p: dict) -> None:
    print("==== stats keys dump ====")
    stats = p.get("stats") if isinstance(p.get("stats"), dict) else {}
    for k, v in stats.items():
        print(f"{k} : {v!r}")
    print("==========================")

def extract_keepa_fields_strict(raw: dict, show_dump: bool=False) -> Dict[str,Any]:
    p = _as_product(raw)
    stats = p.get("stats") if isinstance(p.get("stats"), dict) else {}

    if show_dump:
        dump_stats_keys(p)

    # release_date: int (YYYYMMDD) だけ採用
    release_date = p.get("releaseDate")
    if not isinstance(release_date, int):
        release_date = None

    # monthly_sold: Product直下の monthlySold
    ms = _num(p.get("monthlySold"))
    monthly_sold = int(ms) if ms is not None else None

    # current_sales_rank: stats.current[3]
    current_sales_rank = None
    cur = stats.get("current")
    if isinstance(cur, list) and len(cur) > 3:
        val = _int(cur[3])
        if val is not None and val > 0:
            current_sales_rank = val

    # oos90_new_pct: 上記関数のみ参照
    oos90_new_pct = _get_oos90_new_pct(stats)

    # current_review_count: Product直下の reviewCountCurrent
    rc = p.get("reviewCountCurrent")
    current_review_count = int(rc) if isinstance(rc, (int, float)) else None

    # super_saver_shipping: Product直下の isEligibleForSuperSaverShipping
    super_saver_shipping = _bool(p.get("isEligibleForSuperSaverShipping"))

    # availability_type: stats.buyBoxAvailabilityMessage
    availability_type = _str(stats.get("buyBoxAvailabilityMessage"))

    # last_updated and price change: stats.lastOffersUpdate / stats.lastBuyBoxUpdate
    last_updated_at = _int(stats.get("lastOffersUpdate"))
    last_price_changed_at = _int(stats.get("lastBuyBoxUpdate"))

    return {
        "release_date": release_date,
        "monthly_sold": monthly_sold,
        "oos90_new_pct": oos90_new_pct,
        "current_sales_rank": current_sales_rank,
        "current_review_count": current_review_count,
        "super_saver_shipping": super_saver_shipping,
        "current_availability_type": availability_type,
        "delivery_delay_days_estimate": None,
        "last_updated_at": last_updated_at,
        "last_price_changed_at": last_price_changed_at,
    }


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_dump = sub.add_parser("dump", help="Keepa raw JSON を保存")
    p_dump.add_argument("asin")
    p_dump.add_argument("-o", "--out", default="keepa_raw.json")

    p_strict = sub.add_parser("strict", help="厳格抽出（フォールバックなし）")
    p_strict.add_argument("asin")
    p_strict.add_argument("--dump-keys", action="store_true",
                          help="stats 内のキーを全部吐き出して確認")

    args = ap.parse_args()
    kp = KeepaClient()

    if args.cmd == "dump":
        raw = kp.fetch_product(args.asin)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
        print(f"saved: {os.path.abspath(args.out)}")
        sys.exit(0)

    elif args.cmd == "strict":
        raw = kp.fetch_product(args.asin)
        out = extract_keepa_fields_strict(raw, show_dump=args.dump_keys)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        sys.exit(0)

    sys.exit(1)

# === 互換レイヤ（amazon_snapshot からの import 対応） ===
from typing import Any, Dict
from apps.common.keepa import KeepaClient

def keepa_fetch_product(kp: KeepaClient, asin: str) -> Dict[str, Any]:
    """薄いラッパー。既存コードの呼び出し互換用"""
    return kp.fetch_product(asin)

def extract_keepa_fields(raw: dict) -> dict:
    """
    旧コード互換: いまは厳格版をそのまま返す。
    もし“ゆるい版”が必要になったら、ここに lenient 実装を置く。
    """
    return extract_keepa_fields_strict(raw)


if __name__ == "__main__":
    main()
