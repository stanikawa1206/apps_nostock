# ============================================================
# update_presets_price_ranges.py  (統一版・修正版)
#
# ■ 目的
# nostock.mst.presets_price_ranges の
# USD価格レンジ（low_usd_target / high_usd_target）を元に、
# publish_ebay と同じ compute_cost_range_jpy_from_usd_range() で
# 仕入れ円レンジ（low_jpy_target / high_jpy_target）を逆算して UPDATE する。
#
# ■ 実行方法（推奨）
#   cd D:\apps_nostock
#   python -m apps.tests.update_presets_price_ranges
#
# ■ 重要（今回のバグ）
# preset_group だけで UPDATE すると、同一 group 内の複数行（GA/DDP等）が
# まとめて同じ値に上書きされるため、
# WHERE は行を一意に特定できるキーで絞る（group+mode+usd_range）。
# ============================================================

# -*- coding: utf-8 -*-

from apps.common.utils import get_sql_server_connection, compute_cost_range_jpy_from_usd_range


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
        ORDER BY preset_group, mode, low_usd_target, high_usd_target
    """)

    rows = cur.fetchall()
    print(f"対象件数: {len(rows)}")

    # ② 1件ずつ計算して UPDATE（行を一意に絞る）
    for row in rows:

        preset_group = (row.preset_group or "").strip()
        mode         = (row.mode or "").strip()
        low_usd      = row.low_usd_target
        high_usd     = row.high_usd_target

        if not preset_group or not mode:
            raise RuntimeError(f"不正データ: preset_group/mode が空です: {row}")

        if low_usd is None or high_usd is None:
            raise RuntimeError(
                f"不正データ: low/high_usd_target が NULL です: "
                f"group={preset_group} mode={mode} low={low_usd} high={high_usd}"
            )

        # publish と同じロジックで逆算
        min_cost, max_cost = compute_cost_range_jpy_from_usd_range(
            mode=mode,
            low_usd_target=float(low_usd),
            high_usd_target=float(high_usd),
        )

        cur.execute("""
            UPDATE nostock.mst.presets_price_ranges
            SET
                low_jpy_target  = ?,
                high_jpy_target = ?
            WHERE preset_group   = ?
              AND mode           = ?
              AND low_usd_target = ?
              AND high_usd_target= ?
        """, min_cost, max_cost, preset_group, mode, low_usd, high_usd)

        # 「1行だけ更新」以外はバグ（キーが一意じゃない or データ重複）
        if cur.rowcount != 1:
            raise RuntimeError(
                f"UPDATE対象が一意でない/存在しない: "
                f"group={preset_group} mode={mode} low={low_usd} high={high_usd} "
                f"rowcount={cur.rowcount}"
            )

        print(f"更新: {preset_group} {mode} {low_usd}-{high_usd} → {min_cost} - {max_cost}")

    conn.commit()
    conn.close()

    print("完了")


if __name__ == "__main__":
    main()