# -*- coding: utf-8 -*-
"""
旧価格設定の廃止（2026-11-14）を1トランザクションで適用する。

  1. drop_old_price_settings_20261114.sql
     （presets_price_ranges.keep_* 追加と値の移し替え・for_account 関数の除外をやめる・
       mst.ebay_account_excluded_vendors 削除・mst.category_groups の価格4列削除・
       壊れていた mst.v_presets と dbo.vw_vendor_item_unlisted_active の削除）
  2. 検査
     - presets_price_ranges の keep_* が全行入り、category_groups の値と一致していた
     - 新しい daily_check（このリポジトリ）で作る lookup が、今の mst.presets_lookup と全列一致
       （収集範囲・JPY・keep_* が変わらない。漸減する組は出品の増減で preset が変わりうる）
     - アカウント別の出品候補（fn_take_one_candidates_for_account）の件数が変更前と一致
     - 旧テーブル・旧列・削除したビューを参照する SQL モジュールが無い
  3. --commit のときだけ commit。指定しなければ rollback（予行）。

在庫管理ループを止め、出品処理が動いていない時間に実行する。
実行: python apps/publish/sql/apply_drop_old_price_settings_20261114.py [--commit]
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from apps.common.utils import get_sql_server_connection  # noqa: E402
from apps.inventory import daily_check as dc  # noqa: E402

SQL_DIR = Path(__file__).resolve().parent
OLD_COLS = ("low_usd_target", "high_usd_target", "low_jpy_target", "high_jpy_target")


def batches(path):
    text = path.read_text(encoding="utf-8")
    return [b for b in re.split(r"^\s*GO\s*$", text, flags=re.M | re.I) if b.strip()]


def for_account_counts(cur, accounts):
    return {a: cur.execute("SELECT COUNT(*) FROM dbo.fn_take_one_candidates_for_account(?, ?)", pg, a).fetchone()[0]
            for a, pg in accounts}


# 旧方式（base − アカウント別除外 → 最小優先度）と新しい for_account を、1文（同じ時点のデータ）で数える。
OLD_VS_NEW_SQL = """
WITH b AS (SELECT * FROM dbo.fn_take_one_candidates_base(?)),
old_allowed AS (
    SELECT b.* FROM b
    WHERE NOT EXISTS (SELECT 1 FROM #excl x INNER JOIN mst.vendors vd ON vd.vendor_code = x.vendor_code
                      WHERE x.account = ? AND vd.vendor_name = b.vendor_name)
)
SELECT
    (SELECT COUNT(*) FROM old_allowed o
      WHERE o.publish_priority * 1000 + o.category_priority =
            (SELECT MIN(publish_priority * 1000 + category_priority) FROM old_allowed)),
    (SELECT COUNT(*) FROM dbo.fn_take_one_candidates_for_account(?, ?))
"""


def main():
    commit = "--commit" in sys.argv
    conn = get_sql_server_connection()
    conn.autocommit = False
    cur = conn.cursor()
    ok = True

    accounts = [tuple(r) for r in cur.execute(
        "SELECT account, preset_group FROM mst.ebay_accounts WHERE preset_group IS NOT NULL ORDER BY account")]
    cg_before = {r.category_group: (r.low_usd_target, r.high_usd_target) for r in cur.execute(
        "SELECT category_group, low_usd_target, high_usd_target FROM mst.category_groups")}
    cols = ", ".join(dc.LOOKUP_INSERT_COLUMNS)
    lookup_before = {r[0]: tuple(r) for r in cur.execute(f"SELECT {cols} FROM mst.presets_lookup")}
    print(f"変更前 lookup {len(lookup_before)} 行")
    cand_before = for_account_counts(cur, accounts)
    print("変更前 候補（for_account）:", cand_before, "計", sum(cand_before.values()))
    cur.execute("SELECT account, vendor_code INTO #excl FROM mst.ebay_account_excluded_vendors")

    try:
        for b in batches(SQL_DIR / "drop_old_price_settings_20261114.sql"):
            cur.execute(b)
            while cur.nextset():
                pass

        # 1) keep_* の移し替え
        rows = cur.execute("""SELECT preset_group, category_group, vendor_code, delete_flag, low_usd_target, high_usd_target,
                                     keep_low_usd_target, keep_high_usd_target
                              FROM mst.presets_price_ranges ORDER BY 1, 2, 3""").fetchall()
        bad = [tuple(r) for r in rows if (r.keep_low_usd_target, r.keep_high_usd_target) != cg_before.get(r.category_group)]
        print(f"presets_price_ranges {len(rows)} 行 / keep_* が category_groups と不一致 {len(bad)}")
        for r in rows:
            print("  ", tuple(r))
        ok &= not bad

        # 2) lookup を新しいコードで作り、今の lookup と比べる（DB には書かない）
        new_rows, prune = dc.build_presets_lookup(dc.load_lookup_sources(cur))
        lookup_after = {r[0]: tuple(r) for r in new_rows}
        prune_pairs = {(p["category_group"], p["vendor_code"]) for p in prune}
        added = sorted(set(lookup_after) - set(lookup_before))
        lost = sorted(set(lookup_before) - set(lookup_after))
        changed = [p for p in set(lookup_after) & set(lookup_before) if lookup_after[p] != lookup_before[p]]
        print(f"lookup 新コード {len(lookup_after)} 行 / 追加 {len(added)} / 消失 {len(lost)} / 値が変わった {len(changed)}")
        print("  ", dc.format_lookup_prune(prune))
        ci = dc.LOOKUP_INSERT_COLUMNS.index("category_group")
        vi = dc.LOOKUP_INSERT_COLUMNS.index("vendor_name")
        for p in (added + lost)[:20]:
            r = lookup_after.get(p) or lookup_before.get(p)
            print("   追加/消失:", p, r[ci], r[vi])
        for p in changed[:20]:
            diff = [(c, a, b) for c, a, b in zip(dc.LOOKUP_INSERT_COLUMNS, lookup_before[p], lookup_after[p]) if a != b]
            print("   変化:", p, diff)
        vendor_code_of = {r.vendor_name: r.vendor_code for r in cur.execute("SELECT vendor_name, vendor_code FROM mst.vendors")}
        outside = [p for p in added + lost
                   if ((lookup_after.get(p) or lookup_before[p])[ci],
                       vendor_code_of.get((lookup_after.get(p) or lookup_before[p])[vi])) not in prune_pairs]
        print(f"  漸減の組以外での追加/消失 {len(outside)}")
        ok &= not changed and not outside

        # 3) 候補: 旧方式と新方式を同じ時点で比べる（変更前の件数との差は参考。商品データは常に動く）
        cand_after, diff = {}, {}
        for a, pg in accounts:
            old_n, new_n = cur.execute(OLD_VS_NEW_SQL, pg, a, pg, a).fetchone()
            cand_after[a] = new_n
            if old_n != new_n:
                diff[a] = (old_n, new_n)
        print("変更後 候補（for_account）:", cand_after, "計", sum(cand_after.values()),
              "/ 旧方式との差（同時点）", diff or "なし",
              "/ 参考: 変更前からの増減", {a: cand_after[a] - cand_before[a] for a in cand_before if cand_after[a] != cand_before[a]} or "なし")
        ok &= not diff

        # 4) 旧テーブル・旧列の参照
        t = cur.execute("SELECT OBJECT_ID('mst.ebay_account_excluded_vendors')").fetchone()[0]
        c = cur.execute(f"""SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('mst.category_groups')
                            AND name IN ({", ".join("'%s'" % x for x in OLD_COLS)})""").fetchone()[0]
        refs = [tuple(r) for r in cur.execute("""
            SELECT OBJECT_SCHEMA_NAME(m.object_id) + '.' + OBJECT_NAME(m.object_id)
            FROM sys.sql_modules m
            WHERE m.definition LIKE '%ebay_account_excluded_vendors%'
               OR m.definition LIKE '%v[_]presets%'
               OR m.definition LIKE '%vw[_]vendor[_]item[_]unlisted[_]active%'
               OR (m.definition LIKE '%category_groups%' AND m.definition LIKE '%cg.low_%')""")]
        views = cur.execute("SELECT COUNT(*) FROM sys.views WHERE object_id IN (OBJECT_ID('mst.v_presets'), OBJECT_ID('dbo.vw_vendor_item_unlisted_active'))").fetchone()[0]
        print(f"旧テーブル {'残っている' if t else 'なし'} / category_groups の旧列 {c} / 削除するビュー残り {views} / 参照するSQLモジュール {refs}")
        ok &= t is None and c == 0 and views == 0 and refs == []

        if not ok:
            raise RuntimeError("検査で不一致があったので中止します")
        if commit:
            conn.commit()
            print("commit しました")
        else:
            conn.rollback()
            print("予行のため rollback しました（--commit で確定）")
    except Exception:
        conn.rollback()
        print("rollback しました")
        raise


if __name__ == "__main__":
    main()
