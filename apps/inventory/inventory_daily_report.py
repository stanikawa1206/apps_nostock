# -*- coding: utf-8 -*-
"""
inventory_daily_report.py

在庫管理（inventory_loop.py）の前日の実績を毎朝1通メールする。
常駐はしない。タスクスケジューラ（nostock_inventory_daily_report）から毎朝 8:00 に起動する。

データの取り方（inventory_loop には手を入れない。読み取りのみ）:
  - 工程・サイクルの開始/終了時刻: logs/inventory_loop.log（inventory_loop が工程ごとに書く行）
      「=== inventory_loop 開始 PID=… ===」で区切り、終了行が無いまま再起動・停止した工程とサイクルは数えない。
      日付をまたいだ工程・サイクルは「終了した日」に1回だけ数える。
  - 現在の工程: logs/inventory_loop_status.json と trx.scrape_job（その工程の開始後に投入されたジョブ）
  - 出品処理の時間帯: trx.listings.start_time（その日の eBay 出品の最初〜最後。JOOM は除く）
実績が無い項目は「データなし」と表示する（推定値では埋めない）。

重複送信の防止:
  送信に成功した対象日を logs/inventory_daily_report_state.json に記録し、同じ対象日は再送しない。
  送信に失敗した場合は記録しないので、タスクの次の繰り返し（30分ごと、12:00まで）で再送する。
  --sample は件名に【サンプル】を付けて送り、記録しない（定時のメールを妨げない）。

実行: python -m apps.inventory.inventory_daily_report [--date YYYY-MM-DD] [--sample]
"""

import argparse
import html
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import get_sql_server_connection, send_mail
from apps.inventory.inventory_loop import LOG_PATH as LOOP_LOG_PATH
from apps.inventory.inventory_loop import read_status

STATE_PATH = PROJECT_ROOT / "logs" / "inventory_daily_report_state.json"
LOG_PATH = PROJECT_ROOT / "logs" / "inventory_daily_report.log"

# 工程の順序（inventory_loop.run_cycle と同じ）と、各工程のジョブ種別（trx.scrape_job.job_kind）
PHASES = ("簡易 fetch_sold", "完全 fetch_active", "完全 fetch_sold", "完全 check_remaining")
PHASE_JOB_KIND = {
    "簡易 fetch_sold": "fetch_sold_ebay",
    "完全 fetch_active": "fetch_active_ebay",
    "完全 fetch_sold": "fetch_sold_ebay",
}
COMPARE_DAYS = 7

SQL_PUBLISH_WINDOWS = """
SELECT CAST(start_time AS date), MIN(start_time), MAX(start_time), COUNT(*)
FROM trx.listings
WHERE start_time >= ? AND start_time < ?
  AND account <> 'JOOM'
GROUP BY CAST(start_time AS date)
"""

SQL_PHASE_JOB_PROGRESS = """
SELECT COUNT(*), SUM(CASE WHEN status IN ('done', 'error') THEN 1 ELSE 0 END)
FROM trx.scrape_job
WHERE job_kind = ? AND created_at >= ?
"""

_logger = logging.getLogger("inventory_daily_report")
if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    _handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _logger.addHandler(_handler)
    _logger.propagate = False


def log(msg: str):
    print(msg, flush=True)
    _logger.info(msg)


# ======================
# inventory_loop.log の読み取り
# ======================
@dataclass
class Run:
    """完了した工程（phase）またはサイクル（phase=None）の1回分。"""
    phase: str | None
    started: datetime
    ended: datetime
    count: int | None = None   # ジョブ件数（check_remaining は対象件数）

    @property
    def seconds(self) -> float:
        return (self.ended - self.started).total_seconds()


@dataclass
class OpenPhase:
    """まだ終了していない工程（ログの最後の起動分のみ）。"""
    phase: str
    started: datetime
    initial: int | None = None   # 「完了待ち開始: 残り N 件」


LINE_RE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d{3} (.*)$")
RUN_START_RE = re.compile(r"^=== inventory_loop 開始 ")
RUN_END_RE = re.compile(r"^=== inventory_loop (正常終了|異常終了|手動停止)")
CYCLE_START_RE = re.compile(r"^===== サイクル \d+ 開始 =====")
CYCLE_END_RE = re.compile(r"^===== サイクル \d+ 終了")
PHASE_START_RE = re.compile(r"^\[(.+?)\] 開始: ")
PHASE_WAIT_RE = re.compile(r"^\[(.+?)\] 完了待ち開始: 残り (\d+) 件")
PHASE_END_RE = re.compile(r"^\[(.+?)\] 終了: (?:ジョブ|対象) (\d+) 件")


def parse_loop_log(lines):
    """
    inventory_loop.log の行から、完了した工程・サイクルと、実行中の工程を返す。
    戻り値: (phases: list[Run], cycles: list[Run], open_phase: OpenPhase | None)
    再起動・停止で終了行が無いまま途切れた工程とサイクルは捨てる（二重計上・中途半端な所要時間を出さない）。
    """
    phases, cycles = [], []
    open_phase = None
    cycle_started = None
    for line in lines:
        m = LINE_RE.match(line.rstrip("\r\n"))
        if not m:
            continue
        at = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        msg = m.group(2)

        if RUN_START_RE.match(msg) or RUN_END_RE.match(msg):
            open_phase = None
            cycle_started = None
        elif CYCLE_START_RE.match(msg):
            cycle_started = at
        elif CYCLE_END_RE.match(msg):
            if cycle_started is not None:
                cycles.append(Run(None, cycle_started, at))
            cycle_started = None
        elif mm := PHASE_START_RE.match(msg):
            open_phase = OpenPhase(mm.group(1), at)
        elif mm := PHASE_WAIT_RE.match(msg):
            if open_phase and open_phase.phase == mm.group(1):
                open_phase.initial = int(mm.group(2))
        elif mm := PHASE_END_RE.match(msg):
            if open_phase and open_phase.phase == mm.group(1):
                phases.append(Run(open_phase.phase, open_phase.started, at, int(mm.group(2))))
            open_phase = None
    return phases, cycles, open_phase


def runs_ended_on(runs, days):
    """終了日が days（date の集合）に含まれるものだけ返す。"""
    return [r for r in runs if r.ended.date() in days]


# ======================
# 集計
# ======================
def overlaps(run: Run, windows) -> bool:
    """工程の実行時間が、いずれかの出品時間帯（(開始, 終了)）と重なるか。"""
    return any(run.started < end and start < run.ended for start, end in windows)


def phase_stats(runs):
    """回数・平均所要秒・1時間あたり件数（合計件数 ÷ 合計時間）。実績が無ければ None。"""
    if not runs:
        return None
    total_sec = sum(r.seconds for r in runs)
    counts = [r.count for r in runs if r.count is not None]
    per_hour = sum(counts) / (total_sec / 3600) if counts and total_sec > 0 else None
    return {"n": len(runs), "avg_sec": total_sec / len(runs), "per_hour": per_hour}


def build_report(target: date, phases, cycles, open_phase, log_days, publish_windows, status, progress, now):
    """
    メールに載せる内容を組み立てる（DB・ファイルには触れない）。
    log_days: inventory_loop.log に1行でも記録がある日（在庫管理が動いていた日）の集合
    publish_windows: {date: (最初の出品時刻, 最後の出品時刻, 件数)}
    progress: 実行中の工程の (処理済み件数, 総件数)。わからなければ None
    """
    prev = target - timedelta(days=1)
    week = {target - timedelta(days=i) for i in range(1, COMPARE_DAYS + 1)}
    week_with_data = sorted(d for d in week if d in log_days)

    def period(days):
        ds = set(days)
        return {
            "has_data": bool(ds & log_days),
            "days": len(ds & log_days),
            "cycles": runs_ended_on(cycles, ds),
            "phases": {p: phase_stats([r for r in runs_ended_on(phases, ds) if r.phase == p]) for p in PHASES},
        }

    periods = {"target": period({target}), "prev": period({prev}), "week": period(week)}

    # 出品処理と並行した工程・しなかった工程（対象日と前7日間の完了分）
    span = week | {target}
    windows = [(s, e) for d, (s, e, _) in publish_windows.items() if d in span]
    concurrent = {}
    for p in PHASES:
        runs = [r for r in runs_ended_on(phases, span) if r.phase == p]
        concurrent[p] = (
            phase_stats([r for r in runs if overlaps(r, windows)]),
            phase_stats([r for r in runs if not overlaps(r, windows)]),
        )

    return {
        "target": target,
        "prev": prev,
        "week_from": min(week),
        "week_to": max(week),
        "week_with_data": week_with_data,
        "periods": periods,
        "concurrent": concurrent,
        "publish_window": publish_windows.get(target),
        "status": status,
        "open_phase": open_phase,
        "progress": progress,
        "now": now,
    }


# ======================
# 表示
# ======================
NO_DATA = "データなし"


def fmt_dur(sec) -> str:
    if sec is None:
        return NO_DATA
    minutes = int(round(sec / 60))
    h, m = divmod(minutes, 60)
    return f"{h}時間{m:02d}分" if h else f"{m}分"


def fmt_rate(per_hour) -> str:
    return NO_DATA if per_hour is None else f"{per_hour:,.0f} 件/時"


def fmt_diff(value, base, lower_is_better=False) -> str:
    """value の base に対する増減（%）。速くなった（良くなった）ときは「速い」と付ける。"""
    if value is None or base is None or base == 0:
        return "—"
    pct = (value - base) / base * 100
    better = (pct < 0) if lower_is_better else (pct > 0)
    word = "速い" if better else "遅い"
    if abs(pct) < 0.5:
        return "±0%"
    return f"{pct:+.0f}%（{word}）"


def avg_cycle_sec(cycles):
    return sum(c.seconds for c in cycles) / len(cycles) if cycles else None


def cycles_per_day(period):
    if not period["has_data"]:
        return None
    return len(period["cycles"]) / period["days"]


def table(headers, rows) -> tuple[str, str]:
    """(HTML, テキスト) の表を返す。"""
    th = "".join(f'<th style="border:1px solid #bbb;padding:4px 8px;background:#eef2f7;text-align:left">{html.escape(h)}</th>' for h in headers)
    trs = []
    for row in rows:
        tds = "".join(
            f'<td style="border:1px solid #bbb;padding:4px 8px;{"text-align:right;" if i else ""}white-space:nowrap">{html.escape(str(c))}</td>'
            for i, c in enumerate(row)
        )
        trs.append(f"<tr>{tds}</tr>")
    html_table = f'<table style="border-collapse:collapse;font-size:13px;margin:4px 0 14px">{"<tr>" + th + "</tr>"}{"".join(trs)}</table>'
    text = "\n".join(" | ".join(str(c) for c in r) for r in [headers, *rows])
    return html_table, text


def render(report, sample: bool):
    t = report["target"]
    pr = report["periods"]
    parts_html, parts_text = [], []

    def section(title, note=None):
        parts_html.append(f'<h3 style="margin:16px 0 4px;font-size:15px">{html.escape(title)}</h3>')
        parts_text.append(f"\n■ {title}")
        if note:
            parts_html.append(f'<div style="color:#555;font-size:12px">{html.escape(note)}</div>')
            parts_text.append(note)

    def add_table(headers, rows):
        h, tx = table(headers, rows)
        parts_html.append(h)
        parts_text.append(tx)

    tgt = pr["target"]

    # 1. 完全在庫チェックの完了回数・3. 平均所要時間
    section(
        f"1. {t:%m/%d} の完全在庫チェック",
        "1サイクル = 簡易 fetch_sold → 完全 fetch_active → 完全 fetch_sold → check_remaining。"
        "日付をまたいだものは終了した日に数えます。途中で停止・再起動したサイクルは数えません。",
    )
    if tgt["has_data"]:
        add_table(
            ["項目", "実績"],
            [
                ["完了回数（回転数）", f"{len(tgt['cycles'])} 回"],
                ["1サイクルの平均所要時間", fmt_dur(avg_cycle_sec(tgt["cycles"]))],
            ],
        )
    else:
        add_table(["項目", "実績"], [["完了回数（回転数）", NO_DATA], ["1サイクルの平均所要時間", NO_DATA]])

    # 2. 工程ごとの実行回数・平均所要時間
    section(f"2. {t:%m/%d} の工程ごとの実績", "件数: fetch_sold / fetch_active はジョブ数、check_remaining は確認対象の出品数。")
    rows = []
    for p in PHASES:
        s = tgt["phases"][p]
        rows.append([p, f"{s['n']} 回" if s else NO_DATA, fmt_dur(s["avg_sec"]) if s else NO_DATA,
                     fmt_rate(s["per_hour"]) if s else NO_DATA])
    add_table(["工程", "実行回数", "平均所要時間", "処理速度"], rows)

    # 4. 現在の工程
    section(f"3. 現在の工程（{report['now']:%m/%d %H:%M} 時点）")
    status = report["status"] or {}
    op = report["open_phase"]
    if status.get("state") != "running":
        add_table(["項目", "内容"], [["状態", f"在庫管理は稼働していません（{status.get('state') or '状態ファイルなし'}）"]])
    else:
        phase = status.get("phase")
        started = status.get("phase_started_at")
        rows = [
            ["サイクル", f"{status.get('cycle_no')}（{status.get('cycle_started_at', '').replace('T', ' ')} 開始）"],
            ["実行中の工程", phase],
            ["工程の開始時刻", (started or NO_DATA).replace("T", " ")],
        ]
        prog = report["progress"]
        if prog:
            done, total = prog
            rate = f"{done / total * 100:.1f}%" if total else NO_DATA
            rows.append(["処理済み／総件数", f"{done:,} / {total:,} 件"])
            rows.append(["進捗率", rate])
        else:
            rows.append(["処理済み／総件数", NO_DATA])
            rows.append(["進捗率", NO_DATA])
        if op is None or op.phase != phase:
            rows[1][1] = f"{phase}（工程の切り替え中）"
        add_table(["項目", "内容"], rows)

    # 5. 速度比較
    week_note = (
        f"直近7日 = {report['week_from']:%m/%d}〜{report['week_to']:%m/%d}のうち在庫管理の記録がある "
        f"{len(report['week_with_data'])} 日の平均。"
        "増減は対象日の値との比較。"
    )
    section(f"4. 処理速度の比較（{t:%m/%d} と 前日 {report['prev']:%m/%d}・直近7日）", week_note)
    prev, week = pr["prev"], pr["week"]
    rows = [[
        "完全在庫チェック 完了回数（1日あたり）",
        f"{len(tgt['cycles'])} 回" if tgt["has_data"] else NO_DATA,
        f"{len(prev['cycles'])} 回" if prev["has_data"] else NO_DATA,
        f"{cycles_per_day(week):.1f} 回" if week["has_data"] else NO_DATA,
        fmt_diff(cycles_per_day(tgt), cycles_per_day(prev)),
        fmt_diff(cycles_per_day(tgt), cycles_per_day(week)),
    ]]
    a_t, a_p, a_w = avg_cycle_sec(tgt["cycles"]), avg_cycle_sec(prev["cycles"]), avg_cycle_sec(week["cycles"])
    rows.append(["1サイクルの平均所要時間", fmt_dur(a_t), fmt_dur(a_p), fmt_dur(a_w),
                 fmt_diff(a_t, a_p, lower_is_better=True), fmt_diff(a_t, a_w, lower_is_better=True)])
    for p in PHASES:
        st, sp, sw = (pr[k]["phases"][p] for k in ("target", "prev", "week"))
        rt, rp, rw = (s["per_hour"] if s else None for s in (st, sp, sw))
        rows.append([f"{p} 処理速度", fmt_rate(rt), fmt_rate(rp), fmt_rate(rw), fmt_diff(rt, rp), fmt_diff(rt, rw)])
    add_table(["項目", f"対象日 {t:%m/%d}", f"前日 {report['prev']:%m/%d}", "直近7日平均", "前日比", "7日平均比"], rows)

    # 6. 出品処理との並行稼働
    pw = report["publish_window"]
    pw_text = f"{pw[0]:%H:%M}〜{pw[1]:%H:%M}（eBay出品 {pw[2]:,} 件）" if pw else NO_DATA
    section(
        "5. 出品処理との並行稼働による速度の違い",
        f"対象日の出品処理の時間帯: {pw_text}。"
        f"出品の時間帯（その日の最初〜最後の eBay 出品）と少しでも重なった工程を「並行あり」とし、"
        f"{report['week_from']:%m/%d}〜{t:%m/%d} に完了した工程で比べます。",
    )
    rows = []
    for p in PHASES:
        with_pub, without = report["concurrent"][p]
        r1 = with_pub["per_hour"] if with_pub else None
        r2 = without["per_hour"] if without else None
        rows.append([
            p,
            f"{fmt_rate(r1)}（{with_pub['n']} 回）" if with_pub else NO_DATA,
            f"{fmt_rate(r2)}（{without['n']} 回）" if without else NO_DATA,
            fmt_diff(r1, r2),
        ])
    add_table(["工程", "出品と並行あり", "並行なし", "並行ありの速度（並行なし比）"], rows)

    title = f"在庫管理 日次実績 {t:%Y/%m/%d}（{'月火水木金土日'[t.weekday()]}）"
    if sample:
        title = "【サンプル】" + title
    footer = "集計元: logs/inventory_loop.log・logs/inventory_loop_status.json・trx.scrape_job・trx.listings"
    if sample:
        footer += "\nこのメールは確認用のサンプルです（定時送信の記録には含めていません）。"
    body_html = (
        '<div style="font-family:Meiryo,\'Yu Gothic\',sans-serif;font-size:13px;color:#222">'
        f'<h2 style="font-size:17px;margin:0 0 8px">{html.escape(title)}</h2>'
        + "".join(parts_html)
        + f'<div style="color:#777;font-size:11px;margin-top:12px;white-space:pre-line">{html.escape(footer)}</div></div>'
    )
    body_text = title + "\n" + "\n".join(parts_text) + "\n\n" + footer + "\n"
    return title, body_text, body_html


# ======================
# DB・ファイル
# ======================
def fetch_publish_windows(conn, start: date, end: date):
    """{date: (最初の出品時刻, 最後の出品時刻, 件数)}（start 以上 end 未満の日）"""
    with conn.cursor() as cur:
        cur.execute(SQL_PUBLISH_WINDOWS, datetime.combine(start, datetime.min.time()),
                    datetime.combine(end, datetime.min.time()))
        return {(d if isinstance(d, date) else date.fromisoformat(str(d))): (s, e, n) for d, s, e, n in cur.fetchall()}


def fetch_progress(conn, status, open_phase):
    """実行中の工程の (処理済み件数, 総件数)。わからなければ None。"""
    if not status or status.get("state") != "running" or not status.get("phase_started_at"):
        return None
    phase = status.get("phase")
    kind = PHASE_JOB_KIND.get(phase)
    if kind:
        with conn.cursor() as cur:
            cur.execute(SQL_PHASE_JOB_PROGRESS, kind, datetime.fromisoformat(status["phase_started_at"]))
            total, done = cur.fetchone()
        return (done or 0, total) if total else None
    if phase == "完全 check_remaining" and open_phase and open_phase.phase == phase and open_phase.initial \
            and status.get("outstanding") is not None:
        return open_phase.initial - status["outstanding"], open_phase.initial
    return None


def log_days_of(lines):
    days = set()
    for line in lines:
        m = LINE_RE.match(line)
        if m:
            days.add(date.fromisoformat(m.group(1)[:10]))
    return days


def load_sent():
    if not STATE_PATH.exists():
        return {}
    return json.loads(STATE_PATH.read_text(encoding="utf-8")).get("sent", {})


def save_sent(target: date, at: datetime):
    sent = load_sent()
    sent[target.isoformat()] = at.isoformat(timespec="seconds")
    STATE_PATH.write_text(json.dumps({"sent": sent}, ensure_ascii=False, indent=2), encoding="utf-8")


# ======================
# メイン
# ======================
def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="対象日 YYYY-MM-DD（省略時は前日）")
    ap.add_argument("--sample", action="store_true", help="【サンプル】として送り、送信済みに記録しない")
    args = ap.parse_args(argv)

    now = datetime.now()
    target = date.fromisoformat(args.date) if args.date else now.date() - timedelta(days=1)

    if not args.sample and target.isoformat() in load_sent():
        log(f"{target} の日次実績メールは送信済みのため送りません")
        return 0

    lines = LOOP_LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines() if LOOP_LOG_PATH.exists() else []
    phases, cycles, open_phase = parse_loop_log(lines)
    status = read_status()

    conn = get_sql_server_connection()
    try:
        windows = fetch_publish_windows(conn, target - timedelta(days=COMPARE_DAYS), target + timedelta(days=1))
        progress = fetch_progress(conn, status, open_phase)
    finally:
        conn.close()

    report = build_report(target, phases, cycles, open_phase, log_days_of(lines), windows, status, progress, now)
    subject, body_text, body_html = render(report, args.sample)

    if not send_mail(subject, body_text, html=body_html):
        # 記録しないので、タスクの次の繰り返しで再送する
        log(f"{subject} の送信に失敗しました")
        return 1
    if not args.sample:
        save_sent(target, now)
    log(f"{subject} を送信しました")
    return 0


if __name__ == "__main__":
    sys.exit(main())
