# -*- coding: utf-8 -*-
"""
既存出品の「価格範囲外」判定に使う価格帯（2026-11-24）。

新規出品の範囲と既存出品を維持する範囲は同じにする。判定は、その商品を出品している
アカウント自身の mst.presets_price_ranges の low_usd_target / high_usd_target
（アカウントの preset_group × 商品の presets_lookup.category_group × 仕入先の vendor_code）で行う。
delete_flag は見ない（新規出品を止めた行でも、既存出品の判定は続ける）。

他のアカウントの価格帯で自分の既存出品が維持されることはない。収集範囲（presets_lookup の
low/high_usd_target。組の全アカウントの最小〜最大）は、出品しているアカウントの価格行が無いとき
（出品後に仕入先で商品のカテゴリーが変わり、別のカテゴリーグループの preset になった商品など）だけ使う。
"""
from typing import Optional, Tuple

# 出品中の listing を探す条件は mercari_item_status.get_active_listing_for_action と同じ
# （終了・価格変更はその listing に対して行うため）。
SQL_LISTING_RANGE = """
SELECT TOP (1) l.account, r.low_usd_target, r.high_usd_target
FROM trx.listings AS l
INNER JOIN mst.ebay_accounts AS a
    ON a.account = l.account
INNER JOIN mst.vendors AS vd
    ON vd.vendor_name = l.vendor_name
LEFT JOIN trx.vendor_item AS v
    ON v.vendor_name = l.vendor_name AND v.vendor_item_id = l.vendor_item_id
LEFT JOIN mst.presets_lookup AS p
    ON p.preset = v.preset
LEFT JOIN mst.presets_price_ranges AS r
    ON r.preset_group = a.preset_group
   AND r.category_group = p.category_group
   AND r.vendor_code = vd.vendor_code
WHERE l.vendor_item_id = ?
  AND l.vendor_name = ?
  AND ISNULL(l.is_deleted, 0) = 0
  AND ISNULL(a.is_excluded, 0) = 0
"""


def listing_price_range(
    conn,
    vendor_name: str,
    vendor_item_id: str,
    fallback_low: Optional[float],
    fallback_high: Optional[float],
) -> Tuple[Optional[float], Optional[float], str]:
    """
    (low_usd, high_usd, 判定の根拠) を返す。
    出品中の listing が無い、またはアカウントの価格行が無いときは fallback（収集範囲）を返す。
    """
    cur = conn.cursor()
    try:
        cur.execute(SQL_LISTING_RANGE, vendor_item_id, vendor_name)
        row = cur.fetchone()
    finally:
        cur.close()

    if row is None:
        return fallback_low, fallback_high, "出品なし"
    account, low, high = row
    if low is None or high is None:
        return fallback_low, fallback_high, f"{account}: 価格行なし（収集範囲で判定）"
    return float(low), float(high), f"{account}"
