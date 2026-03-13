# ============================================================
# update_category_groups_and_presets_price_ranges.py
#
# ■ 目的
# 1) nostock.mst.category_groups の USD価格レンジを元に
#    compute_cost_range_jpy_from_usd_range() で円レンジを逆算し、
#    low_jpy_target / high_jpy_target を UPDATE する。
#
# 2) 続いて同じ category_group に属する
#    nostock.mst.presets_price_ranges の全行について、
#    （presets側の USDレンジを使い、mode は category_groups 側を使って）
#    low_jpy_target / high_jpy_target を UPDATE する。
# ============================================================

# -*- coding: utf-8 -*-

from apps.common.utils import (
    get_sql_server_connection,
    compute_cost_range_jpy_from_usd_range,
)


SQL_SELECT_CATEGORY_GROUPS = """
SELECT
    category_group,
    mode,
    low_usd_target,
    high_usd_target
FROM nostock.mst.category_groups
ORDER BY category_group
"""

SQL_UPDATE_CATEGORY_GROUPS_JPY = """
UPDATE nostock.mst.category_groups
SET
    low_jpy_target  = ?,
    high_jpy_target = ?
WHERE category_group = ?
"""

SQL_SELECT_PRESETS_BY_CATEGORY_GROUP = """
SELECT
    preset_group,
    category_group,
    low_usd_target,
    high_usd_target
FROM nostock.mst.presets_price_ranges
WHERE category_group = ?
ORDER BY preset_group
"""

SQL_UPDATE_PRESETS_JPY = """
UPDATE nostock.mst.presets_price_ranges
SET
    low_jpy_target  = ?,
    high_jpy_target = ?
WHERE category_group = ?
  AND preset_group  = ?
"""


def main():
    conn = get_sql_server_connection()
    cur = conn.cursor()

    # ===== 外側: category_groups =====
    cur.execute(SQL_SELECT_CATEGORY_GROUPS)
    category_rows = cur.fetchall()
    print(f"[category_groups] 対象件数: {len(category_rows)}")

    for row in category_rows:
        category_group = (row.category_group or "").strip()
        mode          = (row.mode or "").strip()
        low_usd       = row.low_usd_target
        high_usd      = row.high_usd_target

        if not category_group:
            raise RuntimeError(f"category_groupが空: {row}")
        if not mode:
            raise RuntimeError(f"modeが空: category_group={category_group}")
        if low_usd is None or high_usd is None:
            raise RuntimeError(
                f"category_groups USDレンジNULL: category_group={category_group} mode={mode}"
            )

        # 1) category_groups を USD→JPY 逆算して更新
        min_cost, max_cost = compute_cost_range_jpy_from_usd_range(
            mode=mode,
            low_usd_target=float(low_usd),
            high_usd_target=float(high_usd),
        )

        cur.execute(
            SQL_UPDATE_CATEGORY_GROUPS_JPY,
            min_cost,
            max_cost,
            category_group,
        )

        if cur.rowcount != 1:
            raise RuntimeError(
                f"[category_groups] UPDATE一意でない: category_group={category_group} rowcount={cur.rowcount}"
            )

        print(
            f"[category_groups] 更新: {category_group} mode={mode} "
            f"usd={low_usd}-{high_usd} -> jpy={min_cost}-{max_cost}"
        )

        # ===== 内側: presets_price_ranges（同じcategory_group配下を全部） =====
        cur.execute(SQL_SELECT_PRESETS_BY_CATEGORY_GROUP, category_group)
        preset_rows = cur.fetchall()

        # category_groupに紐づくpresetが0件でも問題ない（0件ならスキップ）
        print(f"  [presets] {category_group} 対象件数: {len(preset_rows)}")

        for prow in preset_rows:
            preset_group = (prow.preset_group or "").strip()
            cg2          = (prow.category_group or "").strip()
            p_low_usd    = prow.low_usd_target
            p_high_usd   = prow.high_usd_target

            if cg2 != category_group:
                raise RuntimeError(
                    f"[presets] category_group不整合: outer={category_group} inner={cg2}"
                )
            if not preset_group:
                raise RuntimeError(f"[presets] preset_groupが空: category_group={category_group}")

            if p_low_usd is None or p_high_usd is None:
                raise RuntimeError(
                    f"[presets] USDレンジNULL: category_group={category_group} preset_group={preset_group}"
                )

            # ★ mode は category_groups の mode を使う（仕様通り）
            p_min_cost, p_max_cost = compute_cost_range_jpy_from_usd_range(
                mode=mode,
                low_usd_target=float(p_low_usd),
                high_usd_target=float(p_high_usd),
            )

            cur.execute(
                SQL_UPDATE_PRESETS_JPY,
                p_min_cost,
                p_max_cost,
                category_group,
                preset_group,
            )

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"[presets] UPDATE一意でない: category_group={category_group} "
                    f"preset_group={preset_group} rowcount={cur.rowcount}"
                )

            print(
                f"    [presets] 更新: preset={preset_group} "
                f"usd={p_low_usd}-{p_high_usd} -> jpy={p_min_cost}-{p_max_cost} (mode={mode})"
            )

    conn.commit()
    conn.close()
    print("完了")


if __name__ == "__main__":
    main()