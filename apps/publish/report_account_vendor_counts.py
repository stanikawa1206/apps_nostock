# -*- coding: utf-8 -*-
"""
アカウントの仕入先別の状況（読み取りだけ）。谷川② に Yahoo!フリマを足した後、R＋Y だけで出品数を確保できるかを見るため（2026-11-16）。

    python -m apps.publish.report_account_vendor_counts 谷川② [日数=7]

表示するもの（カテゴリーグループ × 仕入先）:
  - lookup の preset 数（その仕入先・グループで価格行が delete_flag=0 のもの）
  - 出品候補数（dbo.fn_take_one_candidates_base。1商品1件に数える）
  - 出品中の件数（trx.listings is_deleted=0）
  - 直近 N 日の日別の新規出品数（trx.listings.start_time）
"""
import sys
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main(account, days):
    from apps.common.utils import get_sql_server_connection
    cur = get_sql_server_connection().cursor()
    pg = cur.execute("SELECT preset_group FROM mst.ebay_accounts WHERE account = ?", account).fetchone()
    if not pg or not pg[0]:
        print(f"アカウント {account} の preset_group がない")
        return 1
    pg = pg[0]
    code = dict(cur.execute("SELECT vendor_name, vendor_code FROM mst.vendors").fetchall())

    presets = defaultdict(int)
    for g, vc, n in cur.execute("""
        SELECT pl.category_group, vd.vendor_code, COUNT(*)
        FROM mst.presets_lookup pl
        JOIN mst.vendors vd ON vd.vendor_name = pl.vendor_name
        JOIN mst.presets_price_ranges r ON r.preset_group = ? AND r.category_group = pl.category_group
             AND r.vendor_code = vd.vendor_code AND r.delete_flag = 0
        GROUP BY pl.category_group, vd.vendor_code""", pg).fetchall():
        presets[(g, vc)] = n

    cand = defaultdict(int)
    for g, vn, n in cur.execute("""
        SELECT category_group, vendor_name, COUNT(DISTINCT vendor_item_id)
        FROM dbo.fn_take_one_candidates_base(?) GROUP BY category_group, vendor_name""", pg).fetchall():
        cand[(g, code.get(vn, vn))] = n

    active = defaultdict(int)
    for g, vn, n in cur.execute("""
        SELECT pl.category_group, l.vendor_name, COUNT(*)
        FROM trx.listings l
        LEFT JOIN trx.vendor_item v ON v.vendor_name = l.vendor_name AND v.vendor_item_id = l.vendor_item_id
        LEFT JOIN mst.presets_lookup pl ON pl.preset = v.preset
        WHERE l.account = ? AND l.is_deleted = 0
        GROUP BY pl.category_group, l.vendor_name""", account).fetchall():
        active[(g or "(preset不明)", code.get(vn, vn))] = n

    daily = defaultdict(lambda: defaultdict(int))
    dates = set()
    for d, g, vn, n in cur.execute("""
        SELECT CONVERT(char(10), l.start_time, 120), pl.category_group, l.vendor_name, COUNT(*)
        FROM trx.listings l
        LEFT JOIN trx.vendor_item v ON v.vendor_name = l.vendor_name AND v.vendor_item_id = l.vendor_item_id
        LEFT JOIN mst.presets_lookup pl ON pl.preset = v.preset
        WHERE l.account = ? AND l.start_time >= DATEADD(DAY, -?, CAST(GETDATE() AS date))
        GROUP BY CONVERT(char(10), l.start_time, 120), pl.category_group, l.vendor_name""", account, days).fetchall():
        daily[(g or "(preset不明)", code.get(vn, vn))][d] = n
        dates.add(d)
    dates = sorted(dates)

    keys = sorted(set(presets) | set(cand) | set(active) | set(daily))
    print(f"{account}（preset_group={pg}）  仕入先別: preset / 候補 / 出品中 / 新規出品（{', '.join(d[5:] for d in dates)}）")
    last_g = None
    for g, vc in keys:
        if g != last_g:
            print(f"■ {g}")
            last_g = g
        per_day = " ".join(f"{daily[(g, vc)].get(d, 0):>4}" for d in dates)
        print(f"   {vc:<3} preset {presets.get((g, vc), 0):>5}  候補 {cand.get((g, vc), 0):>6}  出品中 {active.get((g, vc), 0):>6}  新規 {per_day}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)
    sys.exit(main(args[0], int(args[1]) if len(args) > 1 else 7))
