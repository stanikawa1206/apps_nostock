# -*- coding: utf-8 -*-
"""
daily_ops.py — 毎朝8:00の在庫管理 日次実績メール（inventory_daily_report.py）に載せる、
在庫チェック・検索ジョブ・価格更新・出品終了の実績と異常（2026-11-19）。

集計元:
  - trx.inventory_ops_stats（在庫チェック・価格更新・出品終了の10分ごとの件数。apps/inventory/ops_stats.py が記録）
  - trx.inventory_ops_issue（出品終了失敗・仕入不可出品中・判定不可の明細）
  - trx.scrape_job（検索ジョブ。3日で消えるので、日次メールの送信時に対象日の件数を inventory_ops_stats に
    kind='検索ジョブ'・host='日次' で1日分残す。7日平均はその行から作る）
  - trx.listings（価格更新の再試行待ち・重複出品や規約違反などの終了理由）
  - logs/ops_alerts_state.json（続いている広範囲の障害）、logs/inventory_monitor_heartbeat.json（監視の最終実行）
記録が始まる前の日・記録の無い日は 0 件ではなく「集計不可」にする。
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

VENDORS = ("メルカリ", "メルカリshops", "ラクマ", "ＰａｙＰａｙフリマ")
SHORT = {"メルカリ": "M", "メルカリshops": "MS", "ラクマ": "R", "ＰａｙＰａｙフリマ": "Y"}
CHECK_RESULTS = ("販売中", "売り切れ", "削除", "判定不可", "例外")
NA = "集計不可"
LONG_UNDETERMINED_HOURS = 24
# 価格更新・出品終了以外の理由で「正常に」終了した出品（日次で件数だけ見る）
NORMAL_END_REASONS_LIKE = ("価格更新失敗", "IP違反", "eBay削除%", "販売上限")

SQL_STATS_DAYS = """
SELECT CAST(bucket_at AS date) AS d, kind, vendor_name, result, cause,
       SUM(items) AS items, SUM(attempts) AS attempts, SUM(retried) AS retried
  FROM trx.inventory_ops_stats
 WHERE bucket_at >= ? AND bucket_at < ?
 GROUP BY CAST(bucket_at AS date), kind, vendor_name, result, cause
"""
SQL_STATS_FIRST = "SELECT MIN(bucket_at) FROM trx.inventory_ops_stats WHERE kind <> N'検索ジョブ'"

SQL_JOB_DAY = """
SELECT JSON_VALUE(job_payload, '$.vendor_name'),
       SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END),
       SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END),
       COUNT(*)
  FROM trx.scrape_job
 WHERE created_at >= ? AND created_at < ?
 GROUP BY JSON_VALUE(job_payload, '$.vendor_name')
"""
SQL_JOB_SAVED = "SELECT COUNT(*) FROM trx.inventory_ops_stats WHERE kind = N'検索ジョブ' AND bucket_at = ?"
SQL_JOB_SAVE = """
INSERT INTO trx.inventory_ops_stats (bucket_at, host, kind, vendor_name, account, result, cause, items, attempts, retried)
VALUES (?, N'日次', N'検索ジョブ', ?, NULL, ?, NULL, ?, ?, 0)
"""
SQL_JOB_FIRST = "SELECT MIN(created_at) FROM trx.scrape_job"

SQL_OPEN_ISSUES = """
SELECT issue_kind, vendor_name, vendor_item_id, account, listing_id, cause, first_at, last_at, occurrences
  FROM trx.inventory_ops_issue
 WHERE resolved_at IS NULL
 ORDER BY first_at
"""
SQL_PRICE_PENDING = """
SELECT COUNT(*) FROM trx.listings WHERE is_deleted = 0 AND error_message LIKE N'価格更新失敗(再試行待ち)%'
"""
SQL_NORMAL_ENDS = """
SELECT CAST(deleted_at AS date), delete_reason, COUNT(*)
  FROM trx.listings
 WHERE deleted_at >= ? AND deleted_at < ?
   AND (""" + " OR ".join(f"delete_reason LIKE N'{r}'" for r in NORMAL_END_REASONS_LIKE) + """)
 GROUP BY CAST(deleted_at AS date), delete_reason
"""


def _dt(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time())


def save_job_day(conn, day: date) -> None:
    """対象日の検索ジョブの件数を inventory_ops_stats に1日分残す（scrape_job は3日で消えるため）。二重には入れない。"""
    cur = conn.cursor()
    first = cur.execute(SQL_JOB_FIRST).fetchone()[0]
    if first is None or first > _dt(day) or cur.execute(SQL_JOB_SAVED, _dt(day)).fetchone()[0]:
        return
    rows = cur.execute(SQL_JOB_DAY, _dt(day), _dt(day + timedelta(days=1))).fetchall()
    for vn, done, error, total in rows:
        for result, n in (("成功", done or 0), ("失敗", error or 0), ("未完了", (total or 0) - (done or 0) - (error or 0))):
            if n:
                cur.execute(SQL_JOB_SAVE, _dt(day), vn, result, n, n)
    conn.commit()


def fetch_ops(conn, target: date, compare_days: int, root: Path, now: datetime) -> dict:
    start = target - timedelta(days=compare_days)
    cur = conn.cursor()
    first = cur.execute(SQL_STATS_FIRST).fetchone()[0]
    rows = cur.execute(SQL_STATS_DAYS, _dt(start), _dt(target + timedelta(days=1))).fetchall()
    issues = [dict(zip([c[0] for c in cur.description], r)) for r in cur.execute(SQL_OPEN_ISSUES).fetchall()]
    price_pending = cur.execute(SQL_PRICE_PENDING).fetchone()[0]
    ends = cur.execute(SQL_NORMAL_ENDS, _dt(start), _dt(target + timedelta(days=1))).fetchall()
    conn.commit()

    # stats[day][kind] = list of rows
    stats = defaultdict(lambda: defaultdict(list))
    for d, kind, vn, result, cause, items, attempts, retried in rows:
        d = d if isinstance(d, date) else date.fromisoformat(str(d))
        stats[d][kind].append({"vendor": vn, "result": result, "cause": cause, "items": items or 0,
                               "attempts": attempts or 0, "retried": retried or 0})

    def covered(d: date) -> bool:
        """その日の0時から記録がある（記録開始の日の途中からは集計不可）"""
        return first is not None and first <= _dt(d)

    normal_ends = defaultdict(lambda: defaultdict(int))
    for d, reason, n in ends:
        d = d if isinstance(d, date) else date.fromisoformat(str(d))
        normal_ends[d][reason] += n

    state_path = root / "logs" / "ops_alerts_state.json"
    outages = json.loads(state_path.read_text(encoding="utf-8")).get("outages", {}) if state_path.exists() else {}
    hb_path = root / "logs" / "inventory_monitor_heartbeat.json"
    heartbeat = (datetime.fromisoformat(json.loads(hb_path.read_text(encoding="utf-8"))["at"])
                 if hb_path.exists() else None)
    return {"target": target, "days": [target - timedelta(days=i) for i in range(compare_days, -1, -1)],
            "stats": stats, "covered": covered, "issues": issues, "price_pending": price_pending,
            "normal_ends": normal_ends, "outages": outages, "heartbeat": heartbeat, "now": now}


# ---------- 集計 ----------
def check_by_vendor(day_rows):
    """在庫チェック: {仕入先: {"items", 結果ごと, "causes", "retried_ok", "retried_ng"}}"""
    out = {v: {"items": 0, "results": defaultdict(int), "causes": defaultdict(int), "retried_ok": 0, "retried_ng": 0}
           for v in VENDORS}
    for r in day_rows:
        o = out.get(r["vendor"])
        if o is None:
            continue
        o["items"] += r["items"]
        o["results"][r["result"]] += r["items"]
        if r["result"] in ("判定不可", "例外"):
            o["causes"][r["cause"] or "不明"] += r["items"]
            o["retried_ng"] += r["retried"]
        else:
            o["retried_ok"] += r["retried"]
    return out


def ok_fail(day_rows, success="成功", fail="失敗"):
    """価格更新・出品終了・検索ジョブ: {仕入先: (成功, 失敗, 合計)} と 失敗の原因別"""
    out = {v: [0, 0, 0] for v in VENDORS}
    causes = defaultdict(int)
    for r in day_rows:
        o = out.setdefault(r["vendor"] or "（不明）", [0, 0, 0])
        o[2] += r["items"]
        if r["result"] == success:
            o[0] += r["items"]
        elif r["result"] == fail:
            o[1] += r["items"]
            causes[r["cause"] or "不明"] += r["items"]
    return out, causes


def _rate(a, b):
    return f"{a / b:.1%}" if b else "-"


def _avg(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def week_values(ops, fn):
    """前7日のうち集計できた日の値の一覧（集計できない日は None）。"""
    t = ops["target"]
    return [fn(d) if ops["covered"](d) else None for d in ops["days"] if d != t]


# ---------- 表示用 ----------
def summary_lines(ops) -> list:
    """メールの最初に出す全体の状態（重大な未解決問題を先に）。"""
    t, now = ops["target"], ops["now"]
    lines = []
    end_items = {(i["vendor_name"], i["vendor_item_id"]) for i in ops["issues"]
                 if i["issue_kind"] in ("出品終了失敗", "仕入不可出品中")}
    long_end = {(i["vendor_name"], i["vendor_item_id"]) for i in ops["issues"]
                if i["issue_kind"] in ("出品終了失敗", "仕入不可出品中") and now - i["first_at"] >= timedelta(hours=2)}
    if long_end:
        lines.append(f"【重大】仕入先で購入できないのに eBay で出品中: {len(long_end)}件（2時間以上未解決）")
    elif end_items:
        lines.append(f"仕入先で購入できないのに eBay で出品中（2時間未満・処理中）: {len(end_items)}件")
    for k, o in ops["outages"].items():
        lines.append(f"【重大】障害が継続中: {o['detail']}（{o['first']} から）")
    hb = ops["heartbeat"]
    if hb is None or now - hb >= timedelta(minutes=35):
        lines.append(f"【要確認】監視（inventory_monitor）の最終実行が古い: {hb:%m/%d %H:%M}" if hb
                     else "【要確認】監視（inventory_monitor）の実行記録がありません")
    if ops["covered"](t):
        cur = check_by_vendor(ops["stats"][t]["在庫チェック"])
        for v in VENDORS:
            c = cur[v]
            und = c["results"]["判定不可"] + c["results"]["例外"]
            if c["items"] and und / c["items"] >= 0.02:
                lines.append(f"【要確認】{SHORT[v]} の在庫チェックの判定不可・例外が {und}/{c['items']}件（{und / c['items']:.1%}）")
            # 通常は出る判定が突然0件（判定の不具合の兆候）
            for res in ("売り切れ", "削除"):
                avg = _avg(week_values(ops, lambda d, v=v, res=res: check_by_vendor(ops["stats"][d]["在庫チェック"])[v]["results"][res]))
                if avg and avg >= 5 and c["results"][res] == 0:
                    lines.append(f"【要確認】{SHORT[v]} の「{res}」判定が0件（直近7日平均 {avg:.0f}件/日）")
            avg_items = _avg(week_values(ops, lambda d, v=v: check_by_vendor(ops["stats"][d]["在庫チェック"])[v]["items"]))
            if avg_items and avg_items >= 20 and c["items"] == 0:
                lines.append(f"【要確認】{SHORT[v]} の在庫チェックが0件（直近7日平均 {avg_items:.0f}件/日）")
    if not lines:
        lines.append("重大な未解決の問題はありません。")
    return lines


def sections(ops) -> list:
    """[(見出し, 注記, 見出し行, 行)]"""
    t = ops["target"]
    prev = t - timedelta(days=1)
    out = []
    covered_t, covered_p = ops["covered"](t), ops["covered"](prev)

    # 在庫チェック（仕入先別）
    if covered_t:
        cur = check_by_vendor(ops["stats"][t]["在庫チェック"])
        prv = check_by_vendor(ops["stats"][prev]["在庫チェック"]) if covered_p else None
        rows = []
        for v in VENDORS:
            c = cur[v]
            und = c["results"]["判定不可"] + c["results"]["例外"]
            avg = _avg(week_values(ops, lambda d, v=v: check_by_vendor(ops["stats"][d]["在庫チェック"])[v]["items"]))
            rows.append([SHORT[v], f"{c['items']:,}", f"{c['results']['販売中']:,}", f"{c['results']['売り切れ']:,}",
                         f"{c['results']['削除']:,}", f"{und:,}", _rate(c["items"] - und, c["items"]),
                         f"{c['retried_ok']:,}", f"{c['retried_ng']:,}",
                         f"{prv[v]['items']:,}" if prv else NA, f"{avg:,.0f}" if avg is not None else NA])
        out.append(("在庫チェック（仕入先別）",
                    "件数は判定した延べ件数（同じ商品を1日に複数回確認すれば回数分）。成功率 = 判定不可・例外以外の割合。"
                    "retry回復 = retry のあと判定できた件数、retry未解決 = retry しても判定不可・例外だった件数。",
                    ["仕入先", "件数", "販売中", "売り切れ", "削除", "判定不可・例外", "成功率", "retry回復", "retry未解決",
                     "前日の件数", "7日平均"], rows))
        causes = [[SHORT[v], cause, f"{n:,}"] for v in VENDORS for cause, n in sorted(cur[v]["causes"].items(), key=lambda x: -x[1])]
        if causes:
            out.append(("判定不可・例外の原因", None, ["仕入先", "原因", "件数"], causes))
    else:
        out.append(("在庫チェック（仕入先別）", None, ["項目", "内容"], [["在庫チェックの実績", f"{NA}（記録開始前）"]]))

    # 長期間 判定不可
    long_und = [i for i in ops["issues"] if i["issue_kind"] == "判定不可"
                and ops["now"] - i["first_at"] >= timedelta(hours=LONG_UNDETERMINED_HOURS)]
    out.append((f"判定不可が{LONG_UNDETERMINED_HOURS}時間以上続いている商品", None, ["仕入先", "商品", "発生", "回数", "原因"],
                [[SHORT.get(i["vendor_name"], i["vendor_name"]), i["vendor_item_id"], f"{i['first_at']:%m/%d %H:%M}",
                  str(i["occurrences"]), (i["cause"] or "")[:60]] for i in long_und[:20]]
                or [["-", "なし", "", "", ""]]))

    # 検索ジョブ
    def job_rows(d):
        return ops["stats"][d]["検索ジョブ"]
    if job_rows(t):
        cur, _ = ok_fail(job_rows(t))
        prv, _ = ok_fail(job_rows(prev)) if job_rows(prev) else (None, None)
        rows = []
        for v in VENDORS:
            s, f, n = cur.get(v, [0, 0, 0])
            avg = _avg([ (lambda x: (x[0] / x[2]) if x[2] else None)(ok_fail(job_rows(d))[0].get(v, [0, 0, 0]))
                         for d in ops["days"] if d != t and job_rows(d)])
            rows.append([SHORT[v], f"{n:,}", f"{f:,}", _rate(s, n),
                         _rate(prv[v][0], prv[v][2]) if prv else NA, f"{avg:.2%}" if avg is not None else NA])
        out.append(("検索ジョブ（仕入先別）", "失敗 = retry しても取得できなかったジョブ。",
                    ["仕入先", "ジョブ数", "失敗", "成功率", "前日の成功率", "7日平均"], rows))
    else:
        out.append(("検索ジョブ（仕入先別）", None, ["項目", "内容"], [["検索ジョブの実績", NA]]))

    # 価格更新・出品終了
    for kind in ("価格更新", "出品終了"):
        if covered_t:
            cur, causes = ok_fail(ops["stats"][t][kind])
            tot = [sum(x[i] for x in cur.values()) for i in range(3)]
            prv = ok_fail(ops["stats"][prev][kind])[0] if covered_p else None
            ptot = [sum(x[i] for x in prv.values()) for i in range(3)] if prv else None
            rows = [["合計", f"{tot[2]:,}", f"{tot[1]:,}", _rate(tot[0], tot[2]), f"{ptot[1]:,}" if ptot else NA]]
            rows += [[f"失敗の原因: {c}", "", f"{n:,}", "", ""] for c, n in sorted(causes.items(), key=lambda x: -x[1])[:8]]
            note = None
            if kind == "価格更新":
                note = f"再試行待ちのまま出品中（未解決）: {ops['price_pending']}件。原因の「一時的」は5xx・通信エラー等、「個別」は商品ごとのエラー。"
            out.append((kind, note, ["項目", "件数", "失敗", "成功率", "前日の失敗"], rows))
        else:
            out.append((kind, None, ["項目", "内容"], [[f"{kind}の実績", f"{NA}（記録開始前）"]]))

    # 正常に終わった重複出品・規約違反など
    ne = ops["normal_ends"]
    reasons = sorted({r for d in ne for r in ne[d]})
    rows = []
    for r in reasons:
        avg = sum(ne[d].get(r, 0) for d in ops["days"] if d != t) / max(1, len(ops["days"]) - 1)
        rows.append([r, str(ne[t].get(r, 0)), str(ne[prev].get(r, 0)), f"{avg:.1f}"])
    out.append(("価格更新失敗・規約違反などによる出品終了（正常に終了したもの）",
                "価格更新失敗は重複出品・販売上限など。eBay削除はポリシー違反で eBay が削除したもの。",
                ["終了理由", "対象日", "前日", "7日平均"], rows or [["なし", "0", "0", "0"]]))

    # 重大障害・未解決
    rows = [[o["detail"], o["first"], o.get("notified") or "-"] for o in ops["outages"].values()]
    end_open = [i for i in ops["issues"] if i["issue_kind"] in ("出品終了失敗", "仕入不可出品中")]
    rows += [[f"{i['issue_kind']}: {i['account']} eBay {i['listing_id']} {SHORT.get(i['vendor_name'], i['vendor_name'])} "
              f"{i['vendor_item_id']} {(i['cause'] or '')[:50]}", f"{i['first_at']:%m/%d %H:%M}", ""] for i in end_open[:20]]
    hb = ops["heartbeat"]
    rows.append([f"監視（inventory_monitor）の最終実行", f"{hb:%m/%d %H:%M}" if hb else "記録なし", ""])
    out.append(("重大障害・未解決の問題（送信時点）", None, ["内容", "発生", "最終通知"], rows))
    return out
