# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, json
from typing import Any, Dict, List, Optional
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # D:/apps_nostock
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# .env 読み込み（作業ディレクトリ=D:\apps_nostock を前提）
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ==== 内部モジュール ====
from apps.common.spapi import SpapiSession
from apps.common.keepa import KeepaClient

from apps.snapshot.step1_catalog_core import sp_get_catalog_item, extract_title_brand_upc
from apps.snapshot.step2_static_attrs import extract_step2
from apps.snapshot.step3_pricing_new import sp_get_item_offers_new, parse_pricing
from apps.snapshot.step4_fees import get_fees_estimate
from apps.snapshot.step5_keepa_supplement import keepa_fetch_product, extract_keepa_fields
from apps.common.utils import get_sql_server_connection

# ------------------------------------------------------------
# スナップショット本体（STEP1〜STEP5 結合）
# ------------------------------------------------------------
def run_snapshot(asin: str) -> Dict[str, Any]:
    sp = SpapiSession()     # LWA/署名などは内部で
    kp = KeepaClient()

    # STEP1/2: Catalog
    catalog_payload = sp_get_catalog_item(sp, asin)
    step1 = extract_title_brand_upc(catalog_payload, sp.MARKETPLACE_ID)
    step2 = extract_step2(catalog_payload, sp.MARKETPLACE_ID)

    # STEP3: Pricing
    pricing_raw = sp_get_item_offers_new(sp, asin)
    step3 = parse_pricing(pricing_raw)

    # STEP4: Fees（BuyBoxがある時のみ）
    step4 = {"referral_fee_percentage": None, "fba_fee": None}
    bb = step3.get("buybox_price_jpy")
    if bb:
        fba_fee, ref_amt, total_fees, net, _ = get_fees_estimate(sp, asin, bb)
        step4["referral_fee_percentage"] = round(ref_amt / bb * 100.0, 2) if ref_amt else None
        step4["fba_fee"] = fba_fee

    # STEP5: Keepa 補完
    keepa_raw = keepa_fetch_product(kp, asin)
    step5 = extract_keepa_fields(keepa_raw)
    # rating 系は出力しない方針
    step5.pop("current_rating", None)

    return {"asin": asin, **step1, **step2, **step3, **step4, **step5}

def norm_date(val):
    if not val: return None
    s = str(val).replace("-", "").replace("/", "")
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None

# ------------------------------------------------------------
# DB upsert
# ------------------------------------------------------------
def upsert_vendor_item_amazon(conn, snap: Dict[str, Any]):
    sql = """MERGE trx.vendor_item_amazon AS T
    USING (SELECT
        ? AS vendor_item_id, ? AS product_title, ? AS brand_name, ? AS upc_code, ? AS description,
        ? AS category_path, ? AS parent_vendor_item_id,
        TRY_CONVERT(date, ?) AS release_date,            -- ★ release_date整形
        ? AS buybox_price_jpy, ? AS buybox_seller_id, ? AS buybox_is_fba,
        ? AS buybox_is_backorder, ? AS buybox_availability_message,
        ? AS new_current_price, ? AS count_new_total, ? AS count_new_fba, ? AS count_new_fbm,
        ? AS monthly_sold, ? AS super_saver_shipping, ? AS referral_fee_percentage, ? AS fba_fee,
        ? AS current_sales_rank, ? AS current_review_count, ? AS oos90_new_pct, ? AS current_availability_type,
        ? AS package_weight_grams, ? AS package_length_millimeters, ? AS package_width_millimeters, ? AS package_height_millimeters,
        ? AS delivery_delay_days_estimate,
        DATEADD(MINUTE, ?, '2011-01-01') AS last_updated_at,        -- ★ Keepa分→日時
        DATEADD(MINUTE, ?, '2011-01-01') AS last_price_changed_at,  -- ★ 同上
        ? AS is_adult_product,
        ? AS image_url1, ? AS image_url2, ? AS image_url3, ? AS image_url4, ? AS image_url5,
        ? AS image_url6, ? AS image_url7, ? AS image_url8, ? AS image_url9, ? AS image_url10
    ) AS S
    ON (T.vendor_item_id = S.vendor_item_id)
    WHEN MATCHED THEN UPDATE SET
      product_title=S.product_title,
      brand_name=S.brand_name,
      upc_code=S.upc_code,
      description=S.description,
      category_path=S.category_path,
      parent_vendor_item_id=S.parent_vendor_item_id,
      release_date=S.release_date,
      buybox_price_jpy=S.buybox_price_jpy,
      buybox_seller_id=S.buybox_seller_id,
      buybox_is_fba=S.buybox_is_fba,
      buybox_is_backorder=S.buybox_is_backorder,
      buybox_availability_message=S.buybox_availability_message,
      new_current_price=S.new_current_price,
      count_new_total=S.count_new_total,
      count_new_fba=S.count_new_fba,
      count_new_fbm=S.count_new_fbm,
      monthly_sold=S.monthly_sold,
      super_saver_shipping=S.super_saver_shipping,
      referral_fee_percentage=S.referral_fee_percentage,
      fba_fee=S.fba_fee,
      current_sales_rank=S.current_sales_rank,
      current_review_count=S.current_review_count,
      oos90_new_pct=S.oos90_new_pct,
      current_availability_type=S.current_availability_type,
      package_weight_grams=S.package_weight_grams,
      package_length_millimeters=S.package_length_millimeters,
      package_width_millimeters=S.package_width_millimeters,
      package_height_millimeters=S.package_height_millimeters,
      delivery_delay_days_estimate=S.delivery_delay_days_estimate,
      last_updated_at=S.last_updated_at,
      last_price_changed_at=S.last_price_changed_at,
      is_adult_product=S.is_adult_product,
      image_url1=S.image_url1,
      image_url2=S.image_url2,
      image_url3=S.image_url3,
      image_url4=S.image_url4,
      image_url5=S.image_url5,
      image_url6=S.image_url6,
      image_url7=S.image_url7,
      image_url8=S.image_url8,
      image_url9=S.image_url9,
      image_url10=S.image_url10
    WHEN NOT MATCHED THEN
      INSERT (
        vendor_item_id, product_title, brand_name, upc_code, description,
        category_path, parent_vendor_item_id, release_date,
        buybox_price_jpy, buybox_seller_id, buybox_is_fba, buybox_is_backorder, buybox_availability_message,
        new_current_price, count_new_total, count_new_fba, count_new_fbm,
        monthly_sold, super_saver_shipping, referral_fee_percentage, fba_fee,
        current_sales_rank, current_review_count, oos90_new_pct, current_availability_type,
        package_weight_grams, package_length_millimeters, package_width_millimeters, package_height_millimeters,
        delivery_delay_days_estimate, last_updated_at, last_price_changed_at, is_adult_product,
        image_url1, image_url2, image_url3, image_url4, image_url5,
        image_url6, image_url7, image_url8, image_url9, image_url10,
        created_at
      )
      VALUES (
        S.vendor_item_id, S.product_title, S.brand_name, S.upc_code, S.description,
        S.category_path, S.parent_vendor_item_id, S.release_date,
        S.buybox_price_jpy, S.buybox_seller_id, S.buybox_is_fba, S.buybox_is_backorder, S.buybox_availability_message,
        S.new_current_price, S.count_new_total, S.count_new_fba, S.count_new_fbm,
        S.monthly_sold, S.super_saver_shipping, S.referral_fee_percentage, S.fba_fee,
        S.current_sales_rank, S.current_review_count, S.oos90_new_pct, S.current_availability_type,
        S.package_weight_grams, S.package_length_millimeters, S.package_width_millimeters, S.package_height_millimeters,
        S.delivery_delay_days_estimate, S.last_updated_at, S.last_price_changed_at, S.is_adult_product,
        S.image_url1, S.image_url2, S.image_url3, S.image_url4, S.image_url5,
        S.image_url6, S.image_url7, S.image_url8, S.image_url9, S.image_url10,
        SYSDATETIME()
      );
    """

    # 画像10本を整形
    imgs: List[Optional[str]] = (snap.get("images") or [])[:10]
    imgs += [None] * (10 - len(imgs))

    params = [
        snap.get("asin"),
        snap.get("title"),
        snap.get("brand"),
        snap.get("upc"),
        snap.get("description"),  # ★追加：step1由来のdescription（bullet結合済み想定）
        snap.get("category_path") if snap.get("category_path") is not None else None,
        snap.get("parent_asin"),
        norm_date(snap.get("release_date")),
        snap.get("buybox_price_jpy"),
        snap.get("buybox_seller_id"),
        snap.get("buybox_is_fba"),
        snap.get("buybox_is_backorder"),
        snap.get("buybox_availability_message"),
        snap.get("new_current_price"),
        snap.get("count_new_total"),
        snap.get("count_new_fba"),
        snap.get("count_new_fbm"),
        snap.get("monthly_sold"),
        snap.get("super_saver_shipping"),
        snap.get("referral_fee_percentage"),
        snap.get("fba_fee"),
        snap.get("current_sales_rank"),
        snap.get("current_review_count"),
        snap.get("oos90_new_pct"),
        snap.get("current_availability_type"),
        snap.get("package_weight_grams"),
        snap.get("package_length_millimeters"),
        snap.get("package_width_millimeters"),
        snap.get("package_height_millimeters"),
        snap.get("delivery_delay_days_estimate"),
        snap.get("last_updated_at"),
        snap.get("last_price_changed_at"),
        snap.get("isAdultProduct") if "isAdultProduct" in snap else snap.get("is_adult_product"),
        *imgs,
    ]

    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()


from datetime import date

def _parse_release_date_ymd(val: str | None) -> date | None:
    """
    release_date は 'YYYY-MM-DD' のみを許容。
    None/空なら None。
    それ以外が来たら ValueError で落とす（原因を明確化する方針）。
    """
    if val is None:
        return None
    s = val.strip()
    if s == "":
        return None

    # 厳密に 'YYYY-MM-DD'
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"release_date must be 'YYYY-MM-DD' but got: {val}")

    y = int(s[0:4])
    m = int(s[5:7])
    d = int(s[8:10])
    return date(y, m, d)


from datetime import date

def judge_amazon_in_stock(snap: dict, today: date | None = None) -> tuple[bool, str]:
    """
    在庫あり判定＋理由を返す

    return:
      (True,  "reason")  : 在庫あり
      (False, "reason")  : 在庫なし
    """
    if today is None:
        today = date.today()

    # release_date: int (YYYYMMDD) 前提
    rel = snap.get("release_date")
    rd = date(rel // 10000, (rel // 100) % 100, rel % 100) if rel else None

    if rd is not None and rd > today:
        return False, f"予約商品 (release_date={rd})"

    if snap.get("buybox_is_backorder") is True:
        return False, "BuyBoxがバックオーダー"

    count_new_fba = int(snap.get("count_new_fba") or 0)

    if count_new_fba >= 2:
        return True, f"FBA新品出品者が2以上 ({count_new_fba})"

    if count_new_fba == 1:
        av = (snap.get("current_availability_type") or "").strip()
        if av == "在庫あり。":
            return True, "FBA新品1・表示=在庫あり。"
        if av == "NOW":
            return True, "FBA新品1・表示=NOW"

        return False, f"FBA新品1だが表示が在庫あり系でない ({av})"

    return False, "FBA新品出品者が0"



# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: python -m apps.snapshot.amazon_snapshot <ASIN>")

    asin = sys.argv[1].strip()

    # STEP1〜5
    snap = run_snapshot(asin)

    # JSONは常に出す（必要なければこの1行を消してOK）
    sys.stdout.write(json.dumps(snap, ensure_ascii=False) + "\n")

    # DB upsert（接続文字列は使わない）
    conn = get_sql_server_connection()
    try:
        upsert_vendor_item_amazon(conn, snap)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
