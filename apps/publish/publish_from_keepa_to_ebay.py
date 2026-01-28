#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
keepa_finder_to_all.py
- Keepa Product Finder でASIN抽出（FINDER_JSON）
- fetch_keepa_product_snapshot() でスナップショット取得
- trx.vendor_item_amazon に upsert
- 在庫判定（4条件＋予約/バックオーダー）
- ダウンロード版／バッテリー（日本語）NG → 出品不可
- title_en が空なら utils.translate_to_english で英訳＆80字整形
- 代表＋全画像(1..10)を trx.vendor_item に upsert
- 出品OKのみ、価格DDP計算（国際送料3,000円＋関税15%＋手数料＋利益）して
  trx.listings に DRAFT 行を書き出し（listing_id='DRAFT-{ASIN}'）
"""

import os, sys, json, re, time, random
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal, ROUND_HALF_UP

import keepa
import pyodbc

# ====== utils をインポート（定数・関数を利用） ======
sys.path.append(r"D:\apps_nostock\common")
from utils import (
    get_sql_server_connection,
    fetch_keepa_product_snapshot,
    translate_to_english,
    USD_JPY_RATE, PROFIT_RATE, EBAY_FEE_RATE, DUTY_RATE,
)

# === 国際送料（ゲームはGA対象外なので buyer への送料を負担） ===
try:
    # utils に SHIPPING_JPY があれば流用。なければ 3000
    from utils import SHIPPING_JPY as INTERNATIONAL_SHIPPING_JPY  # type: ignore
except Exception:
    INTERNATIONAL_SHIPPING_JPY = 3000

# ====== Keepa Finder 条件 ======
FINDER_JSON = r'''
{
  "avg90_SALES_gte": 1,
  "avg90_SALES_lte": 30000,
  "buyBoxIsPreorder": false,
  "current_NEW_gte": 2000,
  "current_NEW_lte": 10000,
  "outOfStockPercentage90_NEW_gte": 0,
  "outOfStockPercentage90_NEW_lte": 60,
  "fbaFees_gte": 0,
  "fbaFees_lte": 600,
  "rootCategory": ["637394"],
  "packageWeight_gte": 1,
  "packageWeight_lte": 800,
  "sort": [["current_SALES","asc"],["monthlySold","desc"]],
  "productType": [0,1,2],
  "page": 0,
  "perPage": 200
}
'''
FINDER_BASE: Dict[str, Any] = json.loads(FINDER_JSON)

# ====== Keepa 設定 ======
DEFAULT_KEY = "13b6942juaqrentk46epa7jkgokpl3fuhd21vf36h70iefsln2cr9q73i9jh31ui"
API_KEY = os.getenv("KEEPA_API_KEY", DEFAULT_KEY)
DOMAIN = "JP"
PER_PAGE = 200
FETCH_LIMIT = 200  # テスト用。無制限なら 0/None

# ====== DB テーブル名 ======
SCHEMA = "trx"
TABLE_AMZ = "vendor_item_amazon"       # [ebay].[trx].[vendor_item_amazon]
TABLE_VI  = "vendor_item"              # [ebay].[trx].[vendor_item]
TABLE_LST = "listings"                 # [ebay].[trx].[listings]

# ====== ユーティリティ ======
def jst_now_str() -> str:
    jst = timezone(timedelta(hours=9))
    return datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")

def _sanitize_finder_for_keepa(f: Dict[str, Any], per_page: int, start_page: int = 0) -> Dict[str, Any]:
    g = dict(f)
    if isinstance(g.get("productType"), list):
        g.pop("productType", None)
    rc = g.get("rootCategory")
    if isinstance(rc, list) and rc:
        g["rootCategory"] = int(rc[0])
    int_keys = [
        "current_SALES_gte","current_SALES_lte",
        "avg90_SALES_gte","avg90_SALES_lte",
        "current_NEW_gte","current_NEW_lte",
        "outOfStockPercentage90_NEW_gte","outOfStockPercentage90_NEW_lte",
        "fbaFees_gte","fbaFees_lte",
        "packageWeight_gte","packageWeight_lte",
        "perPage","page"
    ]
    for k in int_keys:
        if k in g and isinstance(g[k], str) and g[k].isdigit():
            g[k] = int(g[k])
    g["perPage"] = int(per_page)
    g["page"] = int(start_page)
    return g

def _parse_release_date(date_str: Optional[str]) -> Optional[str]:
    if not date_str: return None
    s = str(date_str)
    try:
        if len(s) == 10 and s[4] == '-':
            return s
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    except Exception:
        pass
    return None

def _to_datetime(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str: return None
    s = str(dt_str)
    try:
        if "T" in s:
            date_part, rest = s.split("T", 1)
            time_part = rest.split("+")[0].split("-")[0]
            return f"{date_part} {time_part}"
        return s[:19]
    except Exception:
        return None

def _is_ok_release(release_date_str: Optional[str]) -> bool:
    if not release_date_str:
        return True
    try:
        y, m, d = map(int, release_date_str.split("-"))
        return date(y, m, d) <= date.today()
    except Exception:
        return True

# ====== 在庫判定（4条件） ======
def evaluate_stock_simple(snapshot_row: Dict[str, Any]) -> Dict[str, Any]:
    """
    返り値:
      {"status": "在庫あり"|"在庫なし", "reason": str|None}
    """
    oos90 = snapshot_row.get("oos90_new_pct")
    cnt_fba = snapshot_row.get("count_new_fba")
    cnt_new_total = snapshot_row.get("count_new_total")
    bb_price = snapshot_row.get("buybox_price_jpy")
    is_backorder = snapshot_row.get("buybox_is_backorder")
    rel = _parse_release_date(snapshot_row.get("release_date"))

    reasons = []

    # 1) OOS90 < 40
    if isinstance(oos90, (int, float)):
        if not (oos90 < 40):
            reasons.append(f"OOS90={oos90}%>=40")

    # 2) FBA>=2（欠損時は total>=3 or buybox 有りで代替OK）
    if isinstance(cnt_fba, int):
        if cnt_fba < 2 and not ((isinstance(cnt_new_total, int) and cnt_new_total >= 3) or (isinstance(bb_price, int) and bb_price >= 0)):
            reasons.append("FBA<2 and no(total>=3 or buybox)")
    else:
        if not ((isinstance(cnt_new_total, int) and cnt_new_total >= 3) or (isinstance(bb_price, int) and bb_price >= 0)):
            reasons.append("noFBA and no(total>=3 or buybox)")

    # 3) 予約除外
    if not _is_ok_release(rel):
        reasons.append("PREORDER")

    # 4) バックオーダーは除外
    if is_backorder is True or (isinstance(is_backorder, int) and is_backorder == 1):
        reasons.append("BACKORDER")

    if reasons:
        return {"status": "在庫なし", "reason": ";".join(reasons)[:200]}
    return {"status": "在庫あり", "reason": None}

# ====== ダウンロード版／バッテリー（日本語）NG ======
_PAT_DIGITAL_JA = re.compile(r"(ダウンロード|デジタル|コード版|オンラインコード|プロダクトキー)")
_PAT_BATTERY_JA = re.compile(r"(電池|バッテリー|充電式|リチウム|Li-?ion)", re.IGNORECASE)

def is_download_or_battery_jp(text: str) -> Tuple[bool, Optional[str]]:
    s = (text or "").strip()
    if not s:
        return False, None
    if _PAT_DIGITAL_JA.search(s):
        return True, "DIGITAL"
    if _PAT_BATTERY_JA.search(s):
        return True, "BATTERY"
    return False, None

# ====== 表示価格・画像選択 ======
def _choose_display_price_jpy(snap: Dict[str, Any]) -> Optional[int]:
    bb = snap.get("buybox_price_jpy")
    if isinstance(bb, int) and bb >= 0:
        return bb
    newp = snap.get("new_current_price")
    if isinstance(newp, int) and newp >= 0:
        return newp
    return None

# ====== 英題整形 ======
def smart_truncate80(s: str) -> str:
    s = (s or "").strip()
    if len(s) <= 80:
        return s
    cut = s[:77]
    if " " in cut and not cut.endswith(" "):
        cut = cut[: cut.rfind(" ")]
    return cut.rstrip() + "..."

# ====== vendor_item_amazon UPSERT ======
def upsert_vendor_item_amazon(conn, rows: List[Dict[str, Any]]):
    schema_table = f"{SCHEMA}.{TABLE_AMZ}"

    sql_upd = f"""
    UPDATE {schema_table}
       SET product_title                = ?,
           brand_name                   = ?,
           upc_code                     = ?,
           category_path                = ?,
           parent_vendor_item_id        = ?,
           release_date                 = ?,
           variation_count              = ?,
           variation_asin_list          = ?,
           buybox_current_price         = ?,
           buybox_current_seller_id     = ?,
           buybox_current_is_fba        = ?,
           buybox_is_amazon_seller      = ?,
           buybox_is_backorder_status   = ?,
           buybox_availability_message  = ?,
           new_current_price            = ?,
           offer_count_new_total        = ?,
           offer_count_new_fba          = ?,
           offer_count_new_fbm          = ?,
           offer_count_used_total       = ?,
           offer_count_used_fba         = ?,
           offer_count_used_fbm         = ?,
           monthly_sold_count           = ?,
           is_eligible_super_saver_shipping = ?,
           referral_fee_rate_percent    = ?,
           fba_fulfillment_fee          = ?,
           current_sales_rank           = ?,
           current_rating_score         = ?,
           current_review_count         = ?,
           out_of_stock_90day_percent   = ?,
           current_availability_type    = ?,
           package_weight_grams         = ?,
           package_length_millimeters   = ?,
           package_width_millimeters    = ?,
           package_height_millimeters   = ?,
           delivery_delay_days_estimate = ?,
           last_updated_at              = ?,
           last_price_changed_at        = ?,
           isAdultProduct               = ?
     WHERE vendor_item_id = ?
    """

    upd_params = []
    for r in rows:
        variations = r.get("variations") or []
        var_count = len(variations) if isinstance(variations, list) else 0
        var_asins = ",".join([v.get("asin", "") for v in variations if isinstance(v, dict)]) if variations else None
        release_date = _parse_release_date(r.get("release_date"))
        last_update = _to_datetime(r.get("last_update"))
        last_price_change = _to_datetime(r.get("last_price_change"))

        upd_params.append((
            r.get("title"),
            r.get("brand"),
            r.get("upc"),
            r.get("category_path"),
            r.get("parent_asin"),
            release_date,
            var_count,
            var_asins,
            r.get("buybox_price_jpy"),
            r.get("buybox_seller_id"),
            1 if r.get("buybox_is_fba") is True else 0 if r.get("buybox_is_fba") is False else None,
            1 if r.get("buybox_is_amazon") is True else 0 if r.get("buybox_is_amazon") is False else None,
            1 if r.get("buybox_is_backorder") is True else 0 if r.get("buybox_is_backorder") is False else None,
            r.get("buybox_availability_message"),
            r.get("new_current_price"),
            r.get("count_new_total"),
            r.get("count_new_fba"),
            r.get("count_new_fbm"),
            r.get("count_used_total"),
            r.get("count_used_fba"),
            r.get("count_used_fbm"),
            r.get("monthly_sold"),
            1 if r.get("super_saver_shipping") is True else 0 if r.get("super_saver_shipping") is False else None,
            r.get("referral_fee_percentage"),
            r.get("fba_fee"),
            r.get("current_sales_rank"),
            r.get("current_rating"),
            r.get("current_review_count"),
            r.get("oos90_new_pct"),
            r.get("availability_type"),
            r.get("weight_g"),
            r.get("length_mm"),
            r.get("width_mm"),
            r.get("height_mm"),
            r.get("delivery_delay_days"),
            last_update,
            last_price_change,
            1 if r.get("is_adult_product") is True else 0 if r.get("is_adult_product") is False else None,
            r.get("asin"),
        ))

    with conn.cursor() as cur:
        cur.fast_executemany = True
        cur.executemany(sql_upd, upd_params)

    sql_ins = f"""
    INSERT INTO {schema_table} (
        vendor_item_id, product_title, brand_name, upc_code, category_path,
        parent_vendor_item_id, release_date,
        variation_count, variation_asin_list,
        buybox_current_price, buybox_current_seller_id, buybox_current_is_fba,
        buybox_is_amazon_seller, buybox_is_backorder_status, buybox_availability_message,
        new_current_price,
        offer_count_new_total, offer_count_new_fba, offer_count_new_fbm,
        offer_count_used_total, offer_count_used_fba, offer_count_used_fbm,
        monthly_sold_count, is_eligible_super_saver_shipping,
        referral_fee_rate_percent, fba_fulfillment_fee,
        current_sales_rank, current_rating_score, current_review_count,
        out_of_stock_90day_percent, current_availability_type,
        package_weight_grams, package_length_millimeters, package_width_millimeters, package_height_millimeters,
        delivery_delay_days_estimate,
        last_updated_at, last_price_changed_at,
        created_at,
        isAdultProduct
    )
    SELECT
        ?, ?, ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, 
        ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?, 
        ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?,
        ?, 
        ?, ?,
        SYSDATETIME(),
        ?
    WHERE NOT EXISTS (
        SELECT 1 FROM {schema_table} WITH (UPDLOCK, HOLDLOCK)
        WHERE vendor_item_id = ?
    )
    """

    ins_params = []
    for r in rows:
        variations = r.get("variations") or []
        var_count = len(variations) if isinstance(variations, list) else 0
        var_asins = ",".join([v.get("asin", "") for v in variations if isinstance(v, dict)]) if variations else None
        release_date = _parse_release_date(r.get("release_date"))
        last_update = _to_datetime(r.get("last_update"))
        last_price_change = _to_datetime(r.get("last_price_change"))

        ins_params.append((
            r.get("asin"),
            r.get("title"),
            r.get("brand"),
            r.get("upc"),
            r.get("category_path"),
            r.get("parent_asin"),
            release_date,
            var_count,
            var_asins,
            r.get("buybox_price_jpy"),
            r.get("buybox_seller_id"),
            1 if r.get("buybox_is_fba") is True else 0 if r.get("buybox_is_fba") is False else None,
            1 if r.get("buybox_is_amazon") is True else 0 if r.get("buybox_is_amazon") is False else None,
            1 if r.get("buybox_is_backorder") is True else 0 if r.get("buybox_is_backorder") is False else None,
            r.get("buybox_availability_message"),
            r.get("new_current_price"),
            r.get("count_new_total"),
            r.get("count_new_fba"),
            r.get("count_new_fbm"),
            r.get("count_used_total"),
            r.get("count_used_fba"),
            r.get("count_used_fbm"),
            r.get("monthly_sold"),
            1 if r.get("super_saver_shipping") is True else 0 if r.get("super_saver_shipping") is False else None,
            r.get("referral_fee_percentage"),
            r.get("fba_fee"),
            r.get("current_sales_rank"),
            r.get("current_rating"),
            r.get("current_review_count"),
            r.get("oos90_new_pct"),
            r.get("availability_type"),
            r.get("weight_g"),
            r.get("length_mm"),
            r.get("width_mm"),
            r.get("height_mm"),
            r.get("delivery_delay_days"),
            last_update,
            last_price_change,
            1 if r.get("is_adult_product") is True else 0 if r.get("is_adult_product") is False else None,
            r.get("asin"),
        ))

    if ins_params:
        with conn.cursor() as cur:
            cur.fast_executemany = True
            cur.executemany(sql_ins, ins_params)
    conn.commit()

# ====== vendor_item（画像1..10、在庫状況、preset）UPSERT ======
def upsert_vendor_item_status(conn, rows: List[Dict[str, Any]]):
    schema_table = "[ebay].[trx].[vendor_item]"
    sql = f"""
    MERGE {schema_table} AS t
    USING (
        SELECT
            ?  AS vendor_name,
            ?  AS vendor_item_id,
            ?  AS title_jp,
            ?  AS title_en,
            ?  AS price,
            ?  AS image_url1,
            ?  AS image_url2,
            ?  AS image_url3,
            ?  AS image_url4,
            ?  AS image_url5,
            ?  AS image_url6,
            ?  AS image_url7,
            ?  AS image_url8,
            ?  AS image_url9,
            ?  AS image_url10,
            ?  AS [出品状況],
            ?  AS [出品状況詳細],
            ?  AS preset
    ) AS s
    ON (t.vendor_name = s.vendor_name AND t.vendor_item_id = s.vendor_item_id)
    WHEN MATCHED THEN UPDATE SET
        t.title_jp         = s.title_jp,
        t.title_en         = s.title_en,
        t.price            = s.price,
        t.image_url1       = s.image_url1,
        t.image_url2       = s.image_url2,
        t.image_url3       = s.image_url3,
        t.image_url4       = s.image_url4,
        t.image_url5       = s.image_url5,
        t.image_url6       = s.image_url6,
        t.image_url7       = s.image_url7,
        t.image_url8       = s.image_url8,
        t.image_url9       = s.image_url9,
        t.image_url10      = s.image_url10,
        t.[出品状況]       = s.[出品状況],
        t.[出品状況詳細]   = s.[出品状況詳細],
        t.preset           = s.preset,
        t.last_updated_str = CONVERT(VARCHAR(19), SYSDATETIME(), 120)
    WHEN NOT MATCHED THEN INSERT (
        vendor_name, vendor_item_id, title_jp, title_en, price,
        image_url1, image_url2, image_url3, image_url4, image_url5,
        image_url6, image_url7, image_url8, image_url9, image_url10,
        [出品状況], [出品状況詳細], preset, created_at, last_updated_str
    ) VALUES (
        s.vendor_name, s.vendor_item_id, s.title_jp, s.title_en, s.price,
        s.image_url1, s.image_url2, s.image_url3, s.image_url4, s.image_url5,
        s.image_url6, s.image_url7, s.image_url8, s.image_url9, s.image_url10,
        s.[出品状況], s.[出品状況詳細], s.preset, SYSDATETIME(), CONVERT(VARCHAR(19), SYSDATETIME(), 120)
    );
    """
    params = []
    for r in rows:
        imgs = (r.get("images") or [])
        imgs = [u for u in imgs if u][:10] + [None]*10
        imgs = imgs[:10]
        params.append((
            r.get("vendor_name", "amazon"),
            r["item_id"],
            r.get("title_jp"),
            r.get("title_en") or "",
            r.get("price"),
            imgs[0], imgs[1], imgs[2], imgs[3], imgs[4],
            imgs[5], imgs[6], imgs[7], imgs[8], imgs[9],
            r.get("listing_status"),
            r.get("listing_status_detail"),
            r.get("preset", "ゲームA"),
        ))
    if not params: return
    with conn.cursor() as cur:
        cur.fast_executemany = True
        cur.executemany(sql, params)
    conn.commit()

# ====== 英題の既存取得 ======
def fetch_existing_title_en(conn, vendor_name: str, vendor_item_id: str) -> Optional[str]:
    sql = """
        SELECT title_en
          FROM trx.vendor_item WITH (NOLOCK)
         WHERE vendor_name = ? AND vendor_item_id = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql, (vendor_name, vendor_item_id))
        row = cur.fetchone()
        if not row:
            return None
        val = (row[0] or "").strip()
        return val or None

# ====== DDP想定の開始価格USD（ゲーム用） ======
LOW_USD_TARGET  = 20.0     # 必要に応じて調整
HIGH_USD_TARGET = 200.0

def compute_start_price_usd(cost_jpy: int) -> str | None:
    """
    ゲーム/DDP用の売価逆算:
      ・国際送料(INTERNATIONAL_SHIPPING_JPY)のみ加算（国内送料は不使用）
      ・関税率(DUTY_RATE)を“送料込み原価”に掛ける
      ・利益率/手数料率を除いた分母で割る
      ・レンジ外は None
    """
    cost = Decimal(cost_jpy)
    intl_ship = Decimal(INTERNATIONAL_SHIPPING_JPY)
    rate = Decimal(str(USD_JPY_RATE))
    p = Decimal(str(PROFIT_RATE))
    f = Decimal(str(EBAY_FEE_RATE))
    duty = Decimal(str(DUTY_RATE))

    denom = Decimal(1) - p - f
    if denom <= 0:
        raise ValueError("利益率とeBay手数料率の合計が1.0以上です。")

    # （仕入れ＋国際送料）に関税を乗せる → 利益・手数料分で割る → USD化
    jpy_total = (cost + intl_ship) * (Decimal(1) + duty) / denom
    usd = (jpy_total / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    if usd < Decimal(str(LOW_USD_TARGET)) or usd > Decimal(str(HIGH_USD_TARGET)):
        return None
    return f"{usd:.2f}"

# ====== listings に DRAFT を書き出し ======
def upsert_draft_listing(conn, vendor_item_id: str, vendor_name: str, account: str):
    """
    listing_id='DRAFT-{ASIN}' で MERGE。
    本出品時（実listing_id付与）には vendor_item_id で一致して更新される想定。
    """
    draft_id = f"DRAFT-{vendor_item_id}"
    sql = f"""
MERGE INTO [trx].[{TABLE_LST}] AS tgt
USING (SELECT ? AS listing_id, ? AS account, ? AS vendor_item_id, ? AS vendor_name) AS src
ON (tgt.listing_id = src.listing_id OR (tgt.vendor_item_id = src.vendor_item_id AND src.vendor_item_id <> ''))
WHEN MATCHED THEN
    UPDATE SET
        tgt.account        = src.account,
        tgt.vendor_item_id = src.vendor_item_id,
        tgt.vendor_name    = src.vendor_name,
        tgt.start_time     = SYSDATETIME()
WHEN NOT MATCHED THEN
    INSERT ([listing_id], [start_time], [account], [vendor_item_id], [vendor_name])
    VALUES (src.listing_id, SYSDATETIME(), src.account, src.vendor_item_id, src.vendor_name);
"""
    with conn.cursor() as cur:
        cur.execute(sql, (draft_id, account, vendor_item_id, vendor_name))
    conn.commit()

# ====== vendor_item 行を作る ======
def _build_vendor_item_rows(snaps: List[Dict[str, Any]],
                            judged: List[Dict[str, Any]],
                            translated: Dict[str, str]) -> List[Dict[str, Any]]:
    by_asin = {j["asin"]: j for j in judged}
    rows: List[Dict[str, Any]] = []
    for s in snaps:
        asin = s.get("asin")
        j = by_asin.get(asin, {})
        raw_imgs = [u for u in (s.get("images") or []) if u]
        imgs10 = (raw_imgs[:10] + [None]*10)[:10]
        price = _choose_display_price_jpy(s)
        rows.append({
            "vendor_name": "amazon",
            "item_id": asin,
            "title_jp": s.get("title"),
            "title_en": translated.get(asin, ""),   # 既存 or 翻訳済み or 空
            "price": price,
            "images": imgs10,
            "listing_status": j.get("listing_status"),
            "listing_status_detail": j.get("listing_status_detail"),
            "preset": "ゲームA",
        })
    return rows

# ====== 価格CSV書き出し<debug用> ======
import csv, os
from datetime import datetime

def export_price_csv(snaps: List[Dict[str, Any]],
                     judged_rows: List[Dict[str, Any]],
                     csv_dir: str = r"D:\apps_nostock\logs") -> str:
    """
    在庫OKかつダウンロード版/電池NGを除外した商品について、
    ASIN / buy-box価格 / 新品価格 / 売価(USD) をCSVに書き出す。
    戻り値: 保存先パス
    """
    os.makedirs(csv_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(csv_dir, f"price_check_{ts}.csv")

    # 在庫判定をASINで引けるように
    judged_by_asin = {j["asin"]: j for j in judged_rows}

    rows_out: List[List[str]] = []
    header = ["ASIN", "buybox_price_jpy", "new_current_price_jpy", "sell_price_usd"]
    rows_out.append(header)

    for s in snaps:
        asin = s.get("asin") or ""
        j = judged_by_asin.get(asin)
        if not j or j.get("listing_status") != "在庫あり":
            continue

        # DL/電池の日本語フィルタ（既存の関数を利用）
        title_jp = s.get("title") or ""
        hit, _reason = is_download_or_battery_jp(title_jp)
        if hit:
            continue

        bb = s.get("buybox_price_jpy")
        newp = s.get("new_current_price")
        cost_jpy = _choose_display_price_jpy(s)  # buybox優先→new
        sell_usd = compute_start_price_usd(cost_jpy, "GA") if cost_jpy is not None else None

        rows_out.append([
            asin,
            "" if bb is None else str(bb),
            "" if newp is None else str(newp),
            "" if not sell_usd else str(sell_usd),
        ])

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows_out)

    print(f"[CSV] 書き出し完了: {csv_path}  行数={len(rows_out)-1}")
    return csv_path

# ====== メイン ======
def main():
    if not API_KEY:
        raise SystemExit("KEEPA_API_KEY を設定してください。")

    EBAY_ACCOUNT = "谷川②"  # trx.listings に書くアカウント名

    print("=== Keepa Finder → vendor_item_amazon / vendor_item / listings (DRAFT) ===")
    api = keepa.Keepa(API_KEY, timeout=60)

    t_before = api.tokens_left
    print(f"[{jst_now_str()}] tokens_left(start): {t_before}")

    # 1) Finder で ASIN 抽出
    finder = _sanitize_finder_for_keepa(FINDER_BASE, per_page=PER_PAGE, start_page=0)
    all_asins: List[str] = []
    page = 0
    try:
        while True:
            finder["page"] = page
            asins = api.product_finder(finder, domain=DOMAIN, wait=True, n_products=PER_PAGE) or []
            if not asins:
                break
            all_asins.extend(asins)
            print(f"[PAGE {page}] {len(asins)} 件取得（累計 {len(all_asins)} 件）")
            if FETCH_LIMIT and FETCH_LIMIT > 0 and len(all_asins) >= FETCH_LIMIT:
                all_asins = all_asins[:FETCH_LIMIT]
                print(f"⚠️ FETCH_LIMIT={FETCH_LIMIT} に達したため打ち切り")
                break
            page += 1
    except Exception as e:
        print(f"[ERROR] finder失敗: {e}")
        return

    if not all_asins:
        print("結果0件。終了。")
        return
    print(f"✅ ASIN取得完了: {len(all_asins)} 件")

    # 2) スナップショット取得
    snaps: List[Dict[str, Any]] = []
    for i, asin in enumerate(all_asins, 1):
        try:
            snap = fetch_keepa_product_snapshot(api, asin, domain=DOMAIN)
            snaps.append(snap)
            print(f"[{i}/{len(all_asins)}] {asin}")
        except Exception as e:
            print(f"[WARN] snapshot失敗 asin={asin}: {e}")

    if not snaps:
        print("スナップショット0件。終了。")
        return
    print(f"✅ snapshot 取得完了: {len(snaps)} 件")

    # 3) DB 更新一式
    conn = None
    batch_size = 50
    try:
        conn = get_sql_server_connection()

        # 3-1) vendor_item_amazon UPSERT（バッチ）
        for batch_start in range(0, len(snaps), batch_size):
            batch = snaps[batch_start: batch_start + batch_size]
            upsert_vendor_item_amazon(conn, batch)
            print(f"[DB] vendor_item_amazon upsert: {batch_start + len(batch)}/{len(snaps)} 件")
        print(f"[DB] vendor_item_amazon upsert完了: {len(snaps)} 件")

        # 3-2) 在庫判定（4条件）
        judged_rows: List[Dict[str, Any]] = []
        for s in snaps:
            j = evaluate_stock_simple(s)
            judged_rows.append({
                "asin": s.get("asin"),
                "listing_status": j["status"],            # 在庫あり / 在庫なし
                "listing_status_detail": j.get("reason"), # 理由
            })

        # 3-3) ダウンロード／バッテリー（日本語）NG → listing_status を「在庫なし(理由)」へ強制
        forced_detail: Dict[str, str] = {}
        for s in snaps:
            asin = s.get("asin")
            title_jp = s.get("title") or ""
            hit, reason = is_download_or_battery_jp(title_jp)
            if hit:
                forced_detail[asin] = reason or "FILTER"
        if forced_detail:
            for j in judged_rows:
                if j["asin"] in forced_detail:
                    j["listing_status"] = "在庫なし"
                    j["listing_status_detail"] = (j.get("listing_status_detail") or "")
                    tag = forced_detail[j["asin"]]
                    j["listing_status_detail"] = (j["listing_status_detail"] + f";{tag}").strip(";")[:200]

        # 3-4) 英題補完（title_en が未保存なら翻訳して80字丸め）
        translated_map: Dict[str, str] = {}
        for s in snaps:
            asin = s.get("asin")
            exist = fetch_existing_title_en(conn, "amazon", asin)
            if exist:
                translated_map[asin] = exist
                continue
            jp = (s.get("title") or "").strip()
            if not jp:
                translated_map[asin] = ""
                continue
            try:
                en = translate_to_english(jp) or ""
            except Exception as e:
                print(f"[translate] fail asin={asin}: {e}")
                en = ""
            translated_map[asin] = smart_truncate80(en) if en else ""

        # 3-5) vendor_item へ反映（画像1..10、在庫状況、preset）
        vendor_item_rows = _build_vendor_item_rows(snaps, judged_rows, translated_map)
        upsert_vendor_item_status(conn, vendor_item_rows)
        print(f"[DB] vendor_item 反映: {len(vendor_item_rows)} 件")

        export_price_csv(snaps, judged_rows)  # <debug用>        

        # 3-6) 出品OKのみ listings に DRAFT を記録（価格DDPの計算はここで判定に使用）
        ok_count = 0
        for s in snaps:
            asin = s.get("asin")
            # 在庫OK？
            j = next((x for x in judged_rows if x["asin"] == asin), None)
            if not j or j["listing_status"] != "在庫あり":
                continue
            # DL/電池フィルタに引っかかっていないか（すでに在庫なし化済みだが二重防御）
            title_jp = s.get("title") or ""
            hit, _ = is_download_or_battery_jp(title_jp)
            if hit:
                continue

            # 代表価格（buybox→new）
            cost_jpy = _choose_display_price_jpy(s)
            if cost_jpy is None:
                continue

            # DDP計算でレンジ外ならスキップ
            try:
                start_price_usd = compute_start_price_usd(cost_jpy, "GA")
            except Exception as e:
                print(f"[price] error asin={asin}: {e}")
                continue
            if not start_price_usd:
                continue

            # DRAFT行を listings へ
            upsert_draft_listing(conn, asin, "amazon", EBAY_ACCOUNT)
            ok_count += 1

        print(f"[DB] listings (DRAFT) 記録: {ok_count} 件")

    except Exception as e:
        print(f"[DB][ERROR] 処理失敗: {e}")
        raise
    finally:
        if conn:
            conn.close()

    t_after = api.tokens_left
    print(f"[{jst_now_str()}] 実行トークン概算: {max(0, (t_before or 0) - (t_after or 0))} / 残トークン: {t_after}")
    print("✅ 完了")

if __name__ == "__main__":
    main()
