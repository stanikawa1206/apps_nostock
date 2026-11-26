# -*- coding: utf-8 -*-
"""
ops_alerts.py — 在庫管理の重大障害の即時通知（2026-11-19）。inventory_monitor（10分ごと）から呼ぶ。

A. 仕入先で終了系（売り切れ・削除・公開停止・オークション）なのに eBay で出品中のまま
   - 出品終了の各経路（check_remaining・scrape_worker の fetch_sold/fetch_active・価格更新失敗の終了）が残す
     「出品終了失敗」の明細と、ここで DB を見て作る「仕入不可出品中」の明細（経路に関係なく拾う）の両方を見る。
   - 最初の発生から ISSUE_ALERT_AFTER_MIN 以上 未解決なら1件でも通知。SKU 不一致など構造的な失敗はすぐ通知。
   - 同じ明細は ISSUE_RENOTIFY_HOURS ごとにだけ再通知。何件あっても1通。通知後も各経路の終了処理は続く。
   - eBay で終了（trx.listings.is_deleted=1）すれば解決にする。「仕入不可出品中」は仕入先で販売中と確認できた時も解決にする
     （status が空欄に戻っただけでは解決にしない）。
   - 通知の対象になった商品だけ、送る前に GetItem で eBay の実際の状態を確かめる（2026-11-26）。
     eBay で終了済みなら mark_listing_deleted_if_ended で DB を終了済みにして解決し、通知しない（ログと実績だけ残す）。
     出品中・確認できない・DB の是正に失敗した商品は、その状態を明記して通知する。
B. 仕入先の広範囲な障害（M・MS・R・Y ごと）
   - 直近30分の在庫チェック（inventory_ops_stats）と検索ジョブ（trx.scrape_job、1ジョブ1件で数え retry は重ねない）で、
     （ラクマの自動停止による再試行待ちは数えない）
     失敗率 VENDOR_FAIL_RATE 以上かつ失敗 VENDOR_FAIL_MIN 件以上。
   - 件数が少ない仕入先も、直近60分に試行が VENDOR_DEAD_MIN_ATTEMPTS 件以上あって成功0件なら通知。
   - ラクマ障害の自動停止（mst.control ラクマ障害_開始）が RAKUMA_PAUSE_ALERT_MIN 分以上続いたら通知。
C. eBay API の広範囲な障害
   - 直近60分の価格更新・出品終了のうち「一時的」（5xx・25001・通信エラー等）な失敗が、率 EBAY_FAIL_RATE 以上かつ
     EBAY_FAIL_MIN 件以上、または2アカウント以上で合計 EBAY_MULTI_ACCOUNT_MIN 件以上。個別商品のエラー（規約・重複等）は数えない。
B・C は同じ障害につき発生時に1通、続く間は OUTAGE_RENOTIFY_HOURS ごとに再通知、解消したら回復メールを1通送って状態を消す
（通知を送っていない障害は回復メールも送らない）。
状態: logs/ops_alerts_state.json
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from apps.common.utils import send_mail
from apps.inventory import ops_stats

# inventory_monitor が logs/inventory_monitor.log へのハンドラーを付ける
_logger = logging.getLogger("inventory_monitor")

STATE_PATH = None  # inventory_monitor が PROJECT_ROOT から設定する

ISSUE_ALERT_AFTER_MIN = 120
ISSUE_RENOTIFY_HOURS = 24
ISSUE_STALE_HOURS = 24          # これだけ再発しない「出品終了失敗」「判定不可」は解決扱い
JUDGE_STALE_HOURS = 8           # 判定不可は次のサイクル（約4.5時間）で再発しなければ解決扱い
STRUCTURAL_CAUSES = ("sku_mismatch",)

VENDOR_WINDOW_MIN = 30
VENDOR_FAIL_RATE = 0.10
VENDOR_FAIL_MIN = 50
VENDOR_DEAD_WINDOW_MIN = 60
VENDOR_DEAD_MIN_ATTEMPTS = 5
RAKUMA_PAUSE_ALERT_MIN = 30
# ラクマの自動停止中に止めたジョブの再試行待ち（scrape_job.error_message に "RAKUMA_PAUSED kind="）。
# 10分の自動停止→自動解除→再試行は想定どおりの処理なので失敗に数えない（11/25 に500が1回で614件が数えられた）。
# 停止が長引いた場合は RAKUMA_PAUSE_ALERT_MIN で通知する。実際の HTTP エラーで再試行待ちになったジョブは従来どおり数える。
RAKUMA_PAUSED_MARK = "RAKUMA_PAUSED kind="

EBAY_WINDOW_MIN = 60
EBAY_FAIL_RATE = 0.05
EBAY_FAIL_MIN = 20
EBAY_MULTI_ACCOUNT_MIN = 30

OUTAGE_RENOTIFY_HOURS = 6

VENDORS = ("メルカリ", "メルカリshops", "ラクマ", "ＰａｙＰａｙフリマ")
ENDED_VENDOR_STATUSES = ("売り切れ", "削除", "公開停止", "オークション")
AVAILABLE_VENDOR_STATUS = "販売中"


def _load_state() -> dict:
    if STATE_PATH is None or not STATE_PATH.exists():
        return {}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def _save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")


# ===================== A. 仕入不可なのに eBay で出品中 =====================
SQL_SCAN_UNAVAILABLE_ACTIVE = f"""
SELECT v.vendor_name, v.vendor_item_id, l.account, l.listing_id, v.status
  FROM trx.listings l
  JOIN trx.vendor_item v ON v.vendor_name = l.vendor_name AND v.vendor_item_id = l.vendor_item_id
 WHERE l.is_deleted = 0
   AND v.status IN ({", ".join("N'" + s + "'" for s in ENDED_VENDOR_STATUSES)})
"""

SQL_RESOLVE_ISSUES = f"""
UPDATE i SET resolved_at = SYSDATETIME()
  FROM trx.inventory_ops_issue i
 WHERE i.resolved_at IS NULL
   AND (
        -- eBay で終了した（出品中の行が無い）
        NOT EXISTS (SELECT 1 FROM trx.listings l
                     WHERE l.vendor_name = i.vendor_name AND l.vendor_item_id = i.vendor_item_id AND l.is_deleted = 0)
        OR (i.issue_kind = N'出品終了失敗' AND i.last_at < DATEADD(HOUR, -{ISSUE_STALE_HOURS}, SYSDATETIME()))
        OR (i.issue_kind = N'判定不可' AND i.last_at < DATEADD(HOUR, -{JUDGE_STALE_HOURS}, SYSDATETIME()))
        -- 仕入先で購入できる（販売中）と確認できた。次のサイクルの開始で status が空欄に戻っただけでは解決にしない
        -- （2026-11-24 BUZZ② 800909223838 は空欄に戻った 18:20 に解決扱いになり、eBay では 21:08 まで出品中だった）
        OR (i.issue_kind = N'仕入不可出品中' AND EXISTS (
               SELECT 1 FROM trx.vendor_item v
                WHERE v.vendor_name = i.vendor_name AND v.vendor_item_id = i.vendor_item_id
                  AND v.status = N'{AVAILABLE_VENDOR_STATUS}'))
   )
"""

SQL_OPEN_END_ISSUES = """
SELECT issue_id, issue_kind, vendor_name, vendor_item_id, account, listing_id, cause, first_at, last_at,
       occurrences, notified_at, DATEDIFF(MINUTE, first_at, SYSDATETIME()) AS age_min
  FROM trx.inventory_ops_issue
 WHERE resolved_at IS NULL AND issue_kind IN (N'出品終了失敗', N'仕入不可出品中')
 ORDER BY first_at
"""


def scan_unavailable_active(conn) -> int:
    """仕入先で終了系なのに出品中の行を「仕入不可出品中」の明細にする（経路に関係なく拾う）。"""
    from apps.inventory import ops_stats
    with conn.cursor() as cur:
        rows = cur.execute(SQL_SCAN_UNAVAILABLE_ACTIVE).fetchall()
    conn.commit()
    for vn, sku, account, listing_id, status in rows:
        ops_stats.issue("仕入不可出品中", vn, sku, f"仕入先={status}", account=account, listing_id=listing_id)
    return len(rows)


def end_issue_alert(rows, now: datetime) -> list:
    """通知する明細（最初の発生から規定時間以上・構造的な失敗。再通知の間隔内のものは除く）。"""
    out = []
    for r in rows:
        structural = any(c in (r["cause"] or "") for c in STRUCTURAL_CAUSES)
        if r["age_min"] < ISSUE_ALERT_AFTER_MIN and not structural:
            continue
        if r["notified_at"] and now - r["notified_at"] < timedelta(hours=ISSUE_RENOTIFY_HOURS):
            continue
        out.append(r)
    return out


# 通知前の確認で eBay の終了を確かめて DB を終了済みにした出品の削除理由
VERIFIED_END_REASON = "仕入不可(通知前にeBay終了確認)"
# 通知前の確認の実績（trx.inventory_ops_stats の kind）と結果
VERIFY_KIND = "通知前確認"
VERIFY_FIXED = "eBay終了済み・DB是正"
VERIFY_ACTIVE = "eBay出品中"
VERIFY_UNKNOWN = "eBay確認不可"
VERIFY_DB_FAILED = "eBay終了済み・DB是正失敗"

SQL_ACTIVE_LISTING = "SELECT COUNT(*) FROM trx.listings WHERE account = ? AND listing_id = ? AND is_deleted = 0"


def verify_on_ebay(conn, account, listing_id, sku) -> tuple[str, str]:
    """
    通知の対象になった出品の eBay の実際の状態を GetItem で確かめる。戻り値は (結果, メールに書く説明)。
      VERIFY_FIXED     eBay で終了済み → DB を終了済みにした（他の経路が先に終了済みにした場合も含む）
      VERIFY_ACTIVE    eBay で出品中
      VERIFY_UNKNOWN   GetItem に失敗して確認できない
      VERIFY_DB_FAILED eBay で終了済みだが DB の是正に失敗
    """
    from apps.adapters.ebay_api import get_listing_status, mark_listing_deleted_if_ended
    if not account or not listing_id:
        return VERIFY_UNKNOWN, "eBay の状態を確認できませんでした（アカウント・出品番号が不明）"
    try:
        st = get_listing_status(account, str(listing_id))
    except Exception as e:
        st = {"ok": False, "ended": False, "error": f"{type(e).__name__}: {e}"}
    if not st.get("ended"):
        if st.get("ok"):
            return VERIFY_ACTIVE, f"eBay で出品中を確認（{st.get('listing_status')}）"
        return VERIFY_UNKNOWN, f"eBay の状態を確認できませんでした（GetItem 失敗: {str(st.get('error'))[:120]}）"
    ended = f"eBay では終了済み（{st.get('listing_status')} {st.get('end_time') or ''}）"
    try:
        done = mark_listing_deleted_if_ended(account, str(listing_id), sku, VERIFIED_END_REASON)
        if not done:
            # 0行: 同じ時に他の経路が終了済みにした（解決）か、mark 内の GetItem で終了を確かめ直せなかった（DB は出品中のまま）
            with conn.cursor() as cur:
                done = cur.execute(SQL_ACTIVE_LISTING, account, str(listing_id)).fetchone()[0] == 0
            conn.commit()
        if done:
            return VERIFY_FIXED, ended
        err = "DB の行を更新できませんでした"
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
    return VERIFY_DB_FAILED, f"{ended}、DB への反映に失敗（DB は出品中のまま）: {err[:160]}"


def check_listing_end(conn, now: datetime) -> None:
    scan_unavailable_active(conn)
    with conn.cursor() as cur:
        cur.execute(SQL_RESOLVE_ISSUES)
        cur.execute(SQL_OPEN_END_ISSUES)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.commit()
    due = end_issue_alert(rows, now)
    if not due:
        return
    # 同じ商品が「出品終了失敗」「仕入不可出品中」の両方に載ることがあるので、商品ごとに1行にする
    by_item = {}
    for r in due:
        by_item.setdefault((r["vendor_name"], r["vendor_item_id"]), []).append(r)
    lines = []
    mailed = []
    fixed = 0
    for (vn, sku), rs in by_item.items():
        first = min(x["first_at"] for x in rs)
        rr = rs[0]
        causes = " / ".join(f"{x['issue_kind']}: {(x['cause'] or '')[:160]}" for x in rs)
        # 通知の前に eBay の実際の状態を確かめる（DB 同士の比較は一次の絞り込み）
        verdict, note = verify_on_ebay(conn, rr["account"], rr["listing_id"], sku)
        ops_stats.count(VERIFY_KIND, vn, verdict, account=rr["account"])
        _logger.info(f"通知前確認 {verdict}: {rr['account']} eBay {rr['listing_id']} {vn} {sku} 発生 {first:%m/%d %H:%M}"
                     f" / {causes} / {note}" + ("（通知しない）" if verdict == VERIFY_FIXED else "（通知する）"))
        if verdict == VERIFY_FIXED:
            fixed += 1
            continue
        mailed.extend(rs)
        lines.append(f"- {rr['account']} eBay {rr['listing_id']} {vn} {sku} 発生 {first:%m/%d %H:%M}"
                     f"（{(now - first).total_seconds() / 3600:.1f}時間）回数 {sum(x['occurrences'] for x in rs)} / {causes}"
                     f"\n    eBay の状態: [{verdict}] {note}")
    if fixed:
        # DB を終了済みにした商品の明細を解決にする（出品中の行が無くなった）
        with conn.cursor() as cur:
            cur.execute(SQL_RESOLVE_ISSUES)
        conn.commit()
    ops_stats.flush()
    if not lines:
        return
    due = mailed
    if any(f"[{VERIFY_ACTIVE}]" in ln for ln in lines):
        subject = f"【重要】仕入先で購入できないのに eBay で出品中の商品があります（{len(lines)}件）"
    else:
        # eBay で出品中と確認できた商品は無い（確認できない・DB への反映に失敗した商品だけ）
        subject = f"【要確認】仕入先で購入できない商品の eBay の状態を確認・DB に反映できていません（{len(lines)}件）"
    body = (
        "仕入先では売り切れ・削除などなのに、DB で eBay の出品中のまま残っている商品があります。\n"
        f"最初の発生から{ISSUE_ALERT_AFTER_MIN // 60}時間以上続いているもの、または構造的な失敗を含みます。"
        "終了処理は各工程で自動で再試行を続けています。\n"
        "送る前に GetItem で eBay の実際の状態を確かめ、各行の「eBay の状態」に書いています"
        f"（[{VERIFY_ACTIVE}] は在庫切れ販売の恐れ、[{VERIFY_UNKNOWN}] は eBay の状態が不明、"
        f"[{VERIFY_DB_FAILED}] は eBay では終了済みで DB への反映だけ失敗）。"
        "eBay で終了済みと確かめて DB を直せた商品は、このメールに含めていません。\n"
        f"同じ商品は{ISSUE_RENOTIFY_HOURS}時間ごとにだけ再通知します。eBay で終了すれば解決になります。\n\n"
        + "\n".join(lines) + "\n\n明細: trx.inventory_ops_issue（resolved_at IS NULL）\n"
    )
    if send_mail(subject, body):
        ids = [r["issue_id"] for r in due]
        with conn.cursor() as cur:
            cur.execute(f"UPDATE trx.inventory_ops_issue SET notified_at = SYSDATETIME() "
                        f"WHERE issue_id IN ({', '.join(str(int(i)) for i in ids)})")
        conn.commit()


# ===================== B・C. 広範囲な障害 =====================
SQL_CHECK_STATS = f"""
SELECT vendor_name,
       SUM(items) AS items,
       SUM(CASE WHEN result IN (N'判定不可', N'例外') THEN items ELSE 0 END) AS fails
  FROM trx.inventory_ops_stats
 WHERE kind = N'在庫チェック' AND bucket_at >= DATEADD(MINUTE, -?, SYSDATETIME())
 GROUP BY vendor_name
"""

SQL_JOB_STATS = f"""
SELECT JSON_VALUE(job_payload, '$.vendor_name') AS vendor_name,
       SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
       SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error,
       SUM(CASE WHEN status = 'retry_wait' THEN 1 ELSE 0 END) AS retry_wait
  FROM trx.scrape_job
 WHERE (status IN ('done', 'error') AND finished_at >= DATEADD(MINUTE, -?, SYSDATETIME()))
    OR (status = 'retry_wait' AND ISNULL(error_message, N'') NOT LIKE N'%{RAKUMA_PAUSED_MARK}%')
 GROUP BY JSON_VALUE(job_payload, '$.vendor_name')
"""

SQL_EBAY_STATS = """
SELECT account,
       SUM(items) AS items,
       SUM(CASE WHEN result = N'失敗' AND (cause LIKE N'一時的%' OR cause IN (N'ConnectionError', N'ReadTimeout',
                N'Timeout', N'ConnectTimeout', N'ChunkedEncodingError')) THEN items ELSE 0 END) AS transient
  FROM trx.inventory_ops_stats
 WHERE kind IN (N'価格更新', N'出品終了') AND bucket_at >= DATEADD(MINUTE, -?, SYSDATETIME())
 GROUP BY account
"""


def _vendor_counts(conn, minutes: int) -> dict:
    """仕入先ごとの (試行, 失敗)。在庫チェックは判定の件数、検索ジョブは1ジョブ1件（retry_wait 中は失敗1件。
    ラクマの自動停止による retry_wait は試行にも失敗にも数えない）。"""
    out = {v: [0, 0] for v in VENDORS}
    with conn.cursor() as cur:
        for vn, items, fails in cur.execute(SQL_CHECK_STATS, minutes).fetchall():
            if vn in out:
                out[vn][0] += items or 0
                out[vn][1] += fails or 0
        for vn, done, error, retry_wait in cur.execute(SQL_JOB_STATS, minutes).fetchall():
            if vn in out:
                out[vn][0] += (done or 0) + (error or 0) + (retry_wait or 0)
                out[vn][1] += (error or 0) + (retry_wait or 0)
    conn.commit()
    return out


def vendor_outages(window: dict, dead_window: dict, rakuma_pause_min) -> dict:
    """{障害キー: 説明}。window/dead_window は {仕入先: (試行, 失敗)}。"""
    out = {}
    for vn in VENDORS:
        att, fail = window.get(vn, (0, 0))
        if fail >= VENDOR_FAIL_MIN and att and fail / att >= VENDOR_FAIL_RATE:
            out[f"vendor:{vn}"] = f"{vn}: 直近{VENDOR_WINDOW_MIN}分の失敗 {fail}/{att}件（{fail / att:.0%}）"
        datt, dfail = dead_window.get(vn, (0, 0))
        if f"vendor:{vn}" not in out and datt >= VENDOR_DEAD_MIN_ATTEMPTS and dfail == datt:
            out[f"vendor:{vn}"] = f"{vn}: 直近{VENDOR_DEAD_WINDOW_MIN}分に {datt}件試して成功0件"
    if rakuma_pause_min is not None and rakuma_pause_min >= RAKUMA_PAUSE_ALERT_MIN:
        out["rakuma_pause"] = f"ラクマ障害による自動停止が {rakuma_pause_min:.0f}分 続いています（mst.control ラクマ障害_開始）"
    return out


def ebay_outages(rows) -> dict:
    """rows: [(account, items, transient)]（直近 EBAY_WINDOW_MIN 分）。"""
    items = sum(r[1] or 0 for r in rows)
    transient = sum(r[2] or 0 for r in rows)
    accounts = [r[0] for r in rows if (r[2] or 0) > 0]
    out = {}
    if transient >= EBAY_FAIL_MIN and items and transient / items >= EBAY_FAIL_RATE:
        out["ebay"] = f"eBay API: 直近{EBAY_WINDOW_MIN}分の一時的な失敗 {transient}/{items}件（{transient / items:.0%}）"
    elif len(accounts) >= 2 and transient >= EBAY_MULTI_ACCOUNT_MIN:
        out["ebay"] = f"eBay API: 直近{EBAY_WINDOW_MIN}分に {len(accounts)}アカウントで一時的な失敗 計{transient}件"
    return out


def _rakuma_pause_minutes(conn):
    # 値は 2000-01-01 からの秒数（DB の時刻。rakuma_outage.py 参照）。NULL = 障害なし
    with conn.cursor() as cur:
        row = cur.execute("""SELECT DATEDIFF(SECOND, DATEADD(SECOND, TRY_CAST(param_value AS bigint), '2000-01-01'),
                                    SYSDATETIME()) / 60.0
                               FROM mst.control WHERE param_key = N'ラクマ障害_開始' AND param_value IS NOT NULL""").fetchone()
    conn.commit()
    return float(row[0]) if row and row[0] is not None else None


def check_outages(conn, now: datetime) -> None:
    current = {}
    current.update(vendor_outages(_vendor_counts(conn, VENDOR_WINDOW_MIN), _vendor_counts(conn, VENDOR_DEAD_WINDOW_MIN),
                                  _rakuma_pause_minutes(conn)))
    with conn.cursor() as cur:
        rows = cur.execute(SQL_EBAY_STATS, EBAY_WINDOW_MIN).fetchall()
    conn.commit()
    current.update(ebay_outages(rows))

    state = _load_state()
    outages = state.setdefault("outages", {})
    changed = False
    # 解消したもの → 回復メール1通・状態を消す
    # 通知していない障害（自動で短時間に解消したもの等）は回復メールを送らず状態だけ消す
    recovered = [k for k in outages if k not in current]
    notified_recovered = [k for k in recovered if outages[k].get("notified")]
    if notified_recovered:
        lines = [f"- {outages[k]['detail']}（{outages[k]['first']} から）" for k in notified_recovered]
        send_mail(f"【回復】在庫管理の障害が解消しました（{len(notified_recovered)}件）",
                  "次の障害の条件に当たらなくなりました。\n\n" + "\n".join(lines) + "\n")
    if recovered:
        for k in recovered:
            del outages[k]
        changed = True
    # 新しい障害・再通知の間隔を過ぎた障害 → 1通にまとめる
    due = []
    for k, detail in current.items():
        o = outages.get(k)
        if o is None:
            outages[k] = {"first": f"{now:%m/%d %H:%M}", "detail": detail, "notified": None}
            o = outages[k]
            changed = True
        o["detail"] = detail
        if not o["notified"] or now - datetime.fromisoformat(o["notified"]) >= timedelta(hours=OUTAGE_RENOTIFY_HOURS):
            due.append(k)
    if due:
        lines = [f"- {outages[k]['detail']}（{outages[k]['first']} から）" for k in due]
        subject = f"【重要】在庫管理の広範囲な障害（{len(due)}件）"
        body = ("仕入先または eBay API で、広い範囲の失敗が続いています。在庫チェック・価格更新・出品終了の"
                "一部ができていない可能性があります（各処理は自動で再試行を続けています）。\n\n"
                + "\n".join(lines) + "\n\n"
                f"続く間は{OUTAGE_RENOTIFY_HOURS}時間ごとに再通知し、解消したら回復メールを送ります。\n"
                "集計: trx.inventory_ops_stats（在庫チェック・価格更新・出品終了）、trx.scrape_job（検索ジョブ）\n")
        if send_mail(subject, body):
            for k in due:
                outages[k]["notified"] = now.isoformat(timespec="seconds")
            changed = True
    if changed:
        _save_state(state)


def run(conn) -> None:
    """inventory_monitor から呼ぶ。A と B・C は互いの失敗で止めない。"""
    now = datetime.now()
    errors = []
    for fn in (check_listing_end, check_outages):
        try:
            fn(conn, now)
        except Exception as e:
            conn.rollback()
            errors.append(f"{fn.__name__}: {type(e).__name__}: {e}")
    if errors:
        raise RuntimeError(" / ".join(errors))
