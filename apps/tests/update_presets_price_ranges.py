# ============================================================
# update_presets_price_ranges.py
#
# ■ 目的
# nostock.mst.presets_price_ranges に登録されている
# USD価格レンジ（low_usd_target / high_usd_target）を元に、
#
# calc_cost_range_from_usd_range() と同じ前提条件
#   - USD_JPY_RATE
#   - PROFIT_RATE
#   - EBAY_FEE_RATE
#   - DOMESTIC_SHIPPING_JPY / INTL_SHIPPING_JPY
#   - DUTY_RATE
#
# を使用して、
# 「そのUSD価格で売るために許容される仕入れ円レンジ」
# （low_jpy_target / high_jpy_target）を逆算し、
# テーブルへ UPDATE する。
#
# ■ 実行方法
# プロジェクトルートで以下を実行：
#   python -m apps.tests.update_presets_price_ranges
#
# ■ 注意
# 為替レートや利益率を変更した場合は再実行すること。
# ============================================================



# -*- coding: utf-8 -*-

from typing import Optional, Tuple
import pyodbc

from decimal import Decimal, ROUND_HALF_UP

# 既存関数を import する前提
from apps.adapters.mercari_search import calc_cost_range_from_usd_range
from apps.common.utils import get_sql_server_connection


def main():

    conn = get_sql_server_connection()
    cur = conn.cursor()

    # ① 現在のテーブルを読む
    cur.execute("""
        SELECT
            preset_group,
            mode,
            low_usd_target,
            high_usd_target
        FROM nostock.mst.presets_price_ranges
    """)

    rows = cur.fetchall()

    print(f"対象件数: {len(rows)}")

    # ② 1件ずつ計算して UPDATE
    for row in rows:

        preset_group = row.preset_group
        mode         = row.mode
        low_usd      = row.low_usd_target
        high_usd     = row.high_usd_target

        min_cost, max_cost = calc_cost_range_from_usd_range(
            mode=mode,
            low_usd_target=low_usd,
            high_usd_target=high_usd,
        )

        cur.execute("""
            UPDATE nostock.mst.presets_price_ranges
            SET
                low_jpy_target  = ?,
                high_jpy_target = ?
            WHERE preset_group = ?
        """, min_cost, max_cost, preset_group)

        print(f"更新: {preset_group} → {min_cost} - {max_cost}")

    conn.commit()
    conn.close()

    print("完了")


if __name__ == "__main__":
    main()
