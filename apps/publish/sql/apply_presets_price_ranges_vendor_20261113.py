# -*- coding: utf-8 -*-
"""
mst.presets_price_ranges の仕入先別への移行（2026-11-13）を1トランザクションで適用する。

  1. presets_price_ranges_vendor_20261113.sql（展開・主キー・外部キー・presets_lookup.keep_*）
  2. fn_take_one_candidates_base.sql（価格帯の結合に vendor_code・delete_flag=0）
  3. 検査（行数・一意・外部キー・delete_flag と新規出品除外の一致・候補の重複なし）
  4. --commit のときだけ commit。指定しなければ rollback（予行）。

在庫管理ループを止め、出品処理が動いていない時間に実行する。
実行: python apps/publish/sql/apply_presets_price_ranges_vendor_20261113.py [--commit]
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from apps.common.utils import get_sql_server_connection  # noqa: E402

SQL_DIR = Path(__file__).resolve().parent


def batches(path):
    text = path.read_text(encoding="utf-8")
    return [b for b in re.split(r"^\s*GO\s*$", text, flags=re.M | re.I) if b.strip()]


def main():
    commit = "--commit" in sys.argv
    conn = get_sql_server_connection()
    conn.autocommit = False
    cur = conn.cursor()
    before = cur.execute("SELECT COUNT(*) FROM mst.presets_price_ranges").fetchone()[0]
    print(f"変更前 mst.presets_price_ranges: {before} 行")
    try:
        for b in batches(SQL_DIR / "presets_price_ranges_vendor_20261113.sql"):
            cur.execute(b)
            while cur.nextset():
                pass
        for b in batches(SQL_DIR / "fn_take_one_candidates_base.sql"):
            cur.execute(b)

        rows = cur.execute("""
            SELECT preset_group, category_group, vendor_code, low_usd_target, high_usd_target,
                   low_jpy_target, high_jpy_target, category_priority, delete_flag
            FROM mst.presets_price_ranges ORDER BY 1, 2, 3""").fetchall()
        for r in rows:
            print("  ", tuple(r))
        n0 = sum(1 for r in rows if not r.delete_flag)
        print(f"変更後: {len(rows)} 行（delete_flag=0: {n0} / 1: {len(rows) - n0}）")

        problems = []
        pk = cur.execute("""
            SELECT STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
            FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID('mst.presets_price_ranges') AND i.is_primary_key = 1""").fetchone()[0]
        if pk != "preset_group,category_group,vendor_code":
            problems.append(f"主キー: {pk}")
        fk = cur.execute("""
            SELECT COUNT(*) FROM sys.foreign_keys
            WHERE parent_object_id = OBJECT_ID('mst.presets_price_ranges')
              AND referenced_object_id = OBJECT_ID('mst.vendors')""").fetchone()[0]
        if fk != 1:
            problems.append(f"外部キー: {fk}")
        old_pairs = cur.execute("SELECT COUNT(*) FROM #ppr_old o WHERE NOT EXISTS (SELECT 1 FROM mst.presets_price_ranges r WHERE r.preset_group = o.preset_group AND r.category_group = o.category_group)").fetchone()[0]
        if old_pairs:
            problems.append(f"展開されなかった変更前の行: {old_pairs}")
        mismatch = cur.execute("""
            SELECT COUNT(*) FROM mst.presets_price_ranges r
            LEFT JOIN mst.ebay_accounts a ON a.preset_group = r.preset_group
            LEFT JOIN mst.ebay_account_excluded_vendors x ON x.account = a.account AND x.vendor_code = r.vendor_code
            WHERE r.delete_flag <> CASE WHEN x.vendor_code IS NULL THEN 0 ELSE 1 END""").fetchone()[0]
        if mismatch:
            problems.append(f"delete_flag と新規出品除外の不一致: {mismatch}")
        changed = cur.execute("""
            SELECT COUNT(*) FROM mst.presets_price_ranges r JOIN #ppr_old o
              ON o.preset_group = r.preset_group AND o.category_group = r.category_group
            WHERE NOT (r.low_usd_target = o.low_usd_target AND r.high_usd_target = o.high_usd_target
                       AND ISNULL(r.low_jpy_target, -1) = ISNULL(o.low_jpy_target, -1)
                       AND ISNULL(r.high_jpy_target, -1) = ISNULL(o.high_jpy_target, -1)
                       AND r.category_priority = o.category_priority)""").fetchone()[0]
        if changed:
            problems.append(f"元の行と価格・優先度が違う行: {changed}")
        keep_null = cur.execute("SELECT COUNT(*) FROM mst.presets_lookup WHERE keep_low_usd_target IS NULL OR keep_high_usd_target IS NULL").fetchone()[0]
        if keep_null:
            problems.append(f"keep_* が NULL の lookup: {keep_null}")
        for (pg,) in cur.execute("SELECT DISTINCT preset_group FROM mst.presets_price_ranges").fetchall():
            n, d = cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT CONCAT(vendor_name, '|', vendor_item_id))
                FROM dbo.fn_take_one_candidates_base(?)""", pg).fetchone()
            print(f"  候補 {pg}: {n:,} 行 / 商品 {d:,}")
            if n != d:
                problems.append(f"候補の重複 {pg}: {n} 行 / {d} 商品")

        if problems:
            raise RuntimeError("検査に不合格: " + " / ".join(problems))
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
