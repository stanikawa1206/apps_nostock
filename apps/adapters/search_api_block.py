# -*- coding: utf-8 -*-
"""
search_api_block.py — メルカリ検索API（api 方式）の 401・403・429 による停止を全台（mouse・VPS）で共有し、自動で解除する。

2026-11-22 14:13 に全8台が同時に 403 を受け、各台の search_api_blocked.txt（人が消すまでジョブを取らない）で
在庫管理が約2.5時間止まった。16:40 の確認では正常に戻っていた。そこで、止まった後に1台の1ワーカーだけが
間隔を空けて1回ずつ検索APIを試し、200 なら全台の停止を解除する。止め方（ファイルがある間は全仕入先の
ジョブを取らない）は変えない。

【mst.control のキー】（値は INT。時刻は「2000-01-01 00:00:00 からの秒数」で SQL Server の時刻。rakuma_outage と同じ）
  - 検索API停止_開始    : いまの停止の始まり（最初に 401・403・429 を受けた台が記録した時刻）。NULL = 停止していない
  - 検索API停止_HTTP    : 最初に受けた HTTP ステータス（401・403・429。分からなければ NULL）
  - 検索API停止_試行回数: 自動確認を行った回数（PROBE_DELAYS_SEC の数が上限。上限まで失敗したら手動対応を待つ）
  - 検索API停止_次回確認: 次に自動確認してよい時刻。NULL = もう自動確認しない（上限到達）
  - 検索API停止_解除    : 最後に停止を解除した時刻（各台はこれを見て自分の search_api_blocked.txt を消す）

【動き】
  - 401・403・429 を受けた台は search_api_blocked.txt を置き（従来どおり）、register_block() で停止を記録する。
    停止中でなければ（または上限到達で手動対応待ちの停止なら）新しい停止として始め、15分後を次回確認にする。
    停止中なら何もしない（同じ停止に加わる）。
  - ファイルがある台のワーカーは、待機中に1分ごとに on_blocked_wait() を呼ぶ:
      * 停止がなく、ファイルの最後の書き込みより後に解除されていれば、ファイルを消す（＝その台の再開）。
      * 停止がなく、解除もファイルより前なら（停止の記録に失敗していた）、停止として記録し直す。
      * 停止中で次回確認の時刻を過ぎていれば、確認する権利を取り合う。取れるのは全台で1ワーカーだけ
        （行をアプリケーションロックの中で読み書きし、次回確認を先へ進めてから確認する）。
        取れたワーカーが検索APIを1回だけ呼び、200 なら停止を解除して自分のファイルを消す。
        他の台は次の待機（1分以内）で解除に気づいてファイルを消す。失敗なら次の確認は 30分後・60分後…。
  - 時計のずれ: VPS の時計は SQL Server より数分ずれていることがあるため、「解除がファイルの書き込みより後か」は
    「解除からの経過秒（SQL Server の時計）」と「ファイルの書き込みからの経過秒（その台の時計）」で比べる。
  - 人が手で解除するときは、clear_block()（python -m apps.adapters.search_api_block --clear）で全台が再開する。
    従来どおり各台のファイルを消してもよい。
"""

from __future__ import annotations

import os
import re
import sys
import time
from typing import Optional

from apps.common.utils import get_sql_server_connection

KEY_START = "検索API停止_開始"
KEY_HTTP = "検索API停止_HTTP"
KEY_ATTEMPTS = "検索API停止_試行回数"
KEY_NEXT = "検索API停止_次回確認"
KEY_CLEARED = "検索API停止_解除"
KEYS = (KEY_START, KEY_HTTP, KEY_ATTEMPTS, KEY_NEXT, KEY_CLEARED)

# 停止の始まりから1回目の確認まで、その後の各確認の間隔（秒）。要素の数が自動確認の上限回数。
# 15分後・その30分後・その60分後・… の計5回（停止から約3時間45分まで）。上限まで失敗したら手動対応を待つ。
PROBE_DELAYS_SEC = (900, 1800, 3600, 3600, 3600)

# 確認に使う検索（1ページ目だけ。2026-11-22 の復旧確認で使い、120件返ったもの）
PROBE_URL = ("https://jp.mercari.com/search?brand_id=1326&category_id=241"
             "&status=sold_out%7Ctrading&item_types=mercari&order=desc&sort=created_time")

API_NAME = "メルカリ検索API（api.mercari.jp/v2/entities:search）"

APPLOCK = "search_api_block"
NOW_SEC_SQL = "DATEDIFF(SECOND, '20000101', SYSDATETIME())"


def status_from_message(message: str) -> Optional[int]:
    """SearchApiBlocked のメッセージ（"status=403 ..."）から HTTP ステータスを取り出す。"""
    m = re.search(r"status=(\d{3})", message or "")
    return int(m.group(1)) if m else None


# ----------------------------------------------------------------------------
# DB（1回の操作 = アプリケーションロックを取ったトランザクション1つ）
# ----------------------------------------------------------------------------
def _locked(fn):
    """fn(cur, state, now) を排他（全台で1つずつ）で実行して結果を返す。state は各キーの値の dict。"""
    conn = get_sql_server_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("EXEC sp_getapplock @Resource = ?, @LockMode = 'Exclusive', @LockOwner = 'Transaction', "
                    "@LockTimeout = 10000", APPLOCK)
        cur.execute(f"SELECT {NOW_SEC_SQL}")
        now = cur.fetchone()[0]
        placeholders = ",".join("?" * len(KEYS))
        cur.execute(f"SELECT param_key, param_value FROM mst.control WHERE param_key IN ({placeholders})", *KEYS)
        state = {k: None for k in KEYS}
        state.update({k: v for k, v in cur.fetchall()})
        result = fn(cur, state, now)
        conn.commit()
        return result
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _set(cur, key: str, value: Optional[int]):
    cur.execute("UPDATE mst.control SET param_value = ? WHERE param_key = ?", value, key)
    if cur.rowcount == 0:
        cur.execute("INSERT INTO mst.control (param_key, param_value) VALUES (?, ?)", key, value)


def _start_new(cur, now: int, http_status: Optional[int]):
    _set(cur, KEY_START, now)
    _set(cur, KEY_HTTP, http_status)
    _set(cur, KEY_ATTEMPTS, 0)
    _set(cur, KEY_NEXT, now + PROBE_DELAYS_SEC[0])


def _is_waiting_manual(state) -> bool:
    return state[KEY_START] is not None and state[KEY_NEXT] is None


def register_block(http_status: Optional[int]) -> str:
    """401・403・429 を受けたときに呼ぶ。戻り値: "new"（新しい停止）/ "joined"（停止中の停止に加わった）"""
    def fn(cur, state, now):
        if state[KEY_START] is None or _is_waiting_manual(state):
            _start_new(cur, now, http_status)
            return "new"
        return "joined"
    return _locked(fn)


def claim_probe() -> Optional[int]:
    """
    確認する権利を取る。取れたら何回目の確認か（1〜）を返し、次回確認を次の間隔の後（上限なら NULL）へ進める。
    取れなければ（停止していない・時刻前・上限到達・他のワーカーが先に取った）None。
    """
    def fn(cur, state, now):
        nxt = state[KEY_NEXT]
        if state[KEY_START] is None or nxt is None or nxt > now:
            return None
        attempt = (state[KEY_ATTEMPTS] or 0) + 1
        _set(cur, KEY_ATTEMPTS, attempt)
        _set(cur, KEY_NEXT, now + PROBE_DELAYS_SEC[attempt] if attempt < len(PROBE_DELAYS_SEC) else None)
        return attempt
    return _locked(fn)


def clear_block() -> bool:
    """停止を解除する（自動確認が 200 だったとき・手動）。停止中だったら True。"""
    def fn(cur, state, now):
        was = state[KEY_START] is not None
        for k in (KEY_START, KEY_HTTP, KEY_ATTEMPTS, KEY_NEXT):
            _set(cur, k, None)
        _set(cur, KEY_CLEARED, now)
        return was
    return _locked(fn)


def read_state() -> dict:
    """現在の停止状態（ロックは取らない）。時刻は秒のまま。now は SQL Server の今。"""
    conn = get_sql_server_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {NOW_SEC_SQL}")
        now = cur.fetchone()[0]
        placeholders = ",".join("?" * len(KEYS))
        cur.execute(f"SELECT param_key, param_value FROM mst.control WHERE param_key IN ({placeholders})", *KEYS)
        state = {k: None for k in KEYS}
        state.update({k: v for k, v in cur.fetchall()})
        state["now"] = now
        return state
    finally:
        conn.close()


def _fmt(sec: Optional[int]) -> str:
    from datetime import datetime, timedelta
    return "-" if sec is None else f"{datetime(2000, 1, 1) + timedelta(seconds=sec):%m/%d %H:%M}"


def describe_state(state: dict) -> str:
    """進捗停止メールに載せる説明。停止中でなければ空文字。"""
    if state.get(KEY_START) is None:
        return ""
    attempts = state.get(KEY_ATTEMPTS) or 0
    lines = [
        "【検索APIの停止】",
        f"- 対象API: {API_NAME}",
        f"- HTTPステータス: {state.get(KEY_HTTP) or '不明'}",
        f"- 停止開始: {_fmt(state[KEY_START])}（SQL Server の時刻）",
    ]
    if state.get(KEY_NEXT) is None:
        lines.append(f"- 自動復旧: 確認 {attempts}/{len(PROBE_DELAYS_SEC)} 回すべて失敗。手動対応を待っています。")
        lines.append("  → 解除: mouse で python -m apps.adapters.search_api_block --clear（全台が1分以内に再開）")
    else:
        lines.append(f"- 自動復旧: 確認 {attempts}/{len(PROBE_DELAYS_SEC)} 回済み、次回 {_fmt(state[KEY_NEXT])} に1台が確認します"
                     "（200 なら全台を自動で再開）。")
    lines.append("- 401・403・429 を受けたことは記録で確認できますが、進捗停止の原因がこれだけかは断定できません。")
    return "\n".join(lines) + "\n"


# ----------------------------------------------------------------------------
# ワーカー側（scrape_worker の待機ループから呼ぶ）
# ----------------------------------------------------------------------------
def _remove(path: str) -> bool:
    try:
        os.remove(path)
        return True
    except FileNotFoundError:
        return True
    except OSError as e:
        # mouse では他プロセスが開いているファイルの削除が WinError 32 になることがある。次の待機で消し直す
        print(f"[SEARCH_API][UNBLOCK] {path} を消せない（次回再試行）: {e}", flush=True)
        return False


def on_blocked_wait(flag_path: str, probe) -> str:
    """
    search_api_blocked.txt がある間、待機ループから1分ごとに呼ぶ。probe() は検索APIを1回呼び、
    200 なら True、401・403・429 などで失敗なら False を返す（例外は False 扱い）。
    戻り値（ログ・テスト用）: "removed"（ファイルを消した）/ "reregistered" / "probe_ok" / "probe_ng" / "wait"
    """
    try:
        file_age = time.time() - os.path.getmtime(flag_path)
    except FileNotFoundError:
        return "removed"

    st = read_state()
    if st[KEY_START] is None:
        cleared = st[KEY_CLEARED]
        if cleared is not None and st["now"] - cleared < file_age:
            # ファイルの最後の書き込みより後に解除された
            if _remove(flag_path):
                print(f"[SEARCH_API][UNBLOCK] 停止が解除されたため {flag_path} を消し、ジョブの取得を再開する", flush=True)
                return "removed"
            return "wait"
        # 停止の記録が無い（ファイルを置いたときに DB に書けなかった等）→ 記録し直して自動確認の対象にする
        register_block(None)
        print("[SEARCH_API][BLOCKED] 停止の記録が無かったため記録し直した（15分後から自動確認）", flush=True)
        return "reregistered"

    attempt = claim_probe()
    if attempt is None:
        return "wait"
    print(f"[SEARCH_API][PROBE] 自動確認 {attempt}/{len(PROBE_DELAYS_SEC)} 回目: 検索APIを1回呼ぶ", flush=True)
    try:
        ok = bool(probe())
    except Exception as e:
        print(f"[SEARCH_API][PROBE] 例外: {type(e).__name__}: {e}", flush=True)
        ok = False
    if not ok:
        tail = "上限に達したため手動対応を待つ" if attempt >= len(PROBE_DELAYS_SEC) else \
            f"{PROBE_DELAYS_SEC[attempt] // 60}分後に再確認"
        print(f"[SEARCH_API][PROBE] 失敗 → {tail}", flush=True)
        return "probe_ng"
    clear_block()
    print("[SEARCH_API][PROBE] 200 → 全台の停止を解除した", flush=True)
    _remove(flag_path)
    return "probe_ok"


def main(argv):
    if argv[1:] == ["--clear"]:
        was = clear_block()
        print("停止を解除しました（各台は1分以内に再開します）" if was else "停止中ではありません（解除時刻だけ更新しました）")
        return 0
    if argv[1:] in ([], ["--status"]):
        st = read_state()
        print(describe_state(st) or "停止していません")
        return 0
    print("使い方: python -m apps.adapters.search_api_block [--status | --clear]")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
