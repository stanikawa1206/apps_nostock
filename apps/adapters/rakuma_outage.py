# -*- coding: utf-8 -*-
"""
rakuma_outage.py — ラクマ(fril.jp)の障害情報を、在庫管理・出品の全ワーカー（mouse・VPS）で共有する。

2026-11-15 01:00ごろ〜04:10 に fril.jp が全台へ 503 を返した（2026-10-28 01:06〜02:00 にも同じ障害）。
出品側は 503 の候補を次々に「解析失敗」にして 1,490 件・約1時間を空振りした。
そこで、ラクマへの HTTP アクセスの結果を mst.control に記録し、障害中はアクセスしないようにする。

【mst.control のキー】（値は INT。時刻は「2000-01-01 00:00:00 からの秒数」で、SQL Server の時刻で数える）
  - ラクマ障害_停止期限          : 5xx（500/502/503/504）。全台のラクマへのアクセスをこの時刻まで止める。NULL = 正常
  - ラクマ障害_開始              : いまの障害（5xx）の始まった時刻。NULL = 障害なし（所要時間をログに出すため）
  - ラクマ制限_停止期限_<台の名前>: 429。その台（WORKER_NAME かホスト名）だけをこの時刻まで止める。NULL = 正常
  時刻に戻すには SQL Server で DATEADD(SECOND, param_value, '2000-01-01')。ワーカーの時計のずれの影響を受けない。

【動き】
  - エラーのたびに停止期限を「今の値」と「今から OUTAGE_PAUSE_SEC 秒後（429 は Retry-After の秒数）」の遅い方にする。
    行を UPDLOCK・HOLDLOCK で読んでから書くので、複数の台が同時に書いても期限が短くなることはない。
  - 期限が過ぎたら止めない（次のアクセスがそのまま様子見になる）。
  - アクセスが成功（2xx・404）したら、期限が過ぎている停止だけを NULL に戻す（障害の終了）。
    期限内の停止は成功では解除しない（期限切れの後に別の台が新しい 503 で期限を延ばした直後に、
    それより前に送った要求の成功が届いても、新しい停止は残る）。延長と解除は同じ行のロックで順番に処理される。
  - 障害状態の読み書きで DB に失敗した場合は RakumaStateError（一時的なエラー）を送出する。
    商品のせいではないので、呼び出し側は「解析失敗」にしない（出品は候補を手放す、在庫確認は判定不可、
    検索ジョブは再試行待ち。どれも RakumaHttpError の一時的なエラーと同じ扱い）。

使う側:
  - 検索（rakuma_search.fetch_rakuma_search_html）・商品ページ（rakuma_item_status.fetch_rakuma_item_html）:
    取得前に raise_if_paused()、取得後に check_response()。
  - 出品の候補取得（publish_ebay.TAKE_ONE_SQL）: PAUSED_EXISTS_SQL と同じ条件でラクマの候補を外す。
"""

from __future__ import annotations

import os
import re
import socket
import time
from datetime import datetime, timedelta
from typing import Optional

from apps.common.utils import get_sql_server_connection

# ラクマ側の障害とみなすステータス（全台を止める）
OUTAGE_STATUSES = {500, 502, 503, 504}
# アクセス制限とみなすステータス（その台だけ止める）
RATE_LIMIT_STATUS = 429

OUTAGE_PAUSE_SEC = 600
RATE_LIMIT_PAUSE_SEC = 600
# Retry-After を信じる上限（極端な値で長時間止まらないように）
RATE_LIMIT_PAUSE_MAX_SEC = 3600

RAKUMA_STATE_CACHE_SEC = 30

KEY_OUTAGE_UNTIL = "ラクマ障害_停止期限"
KEY_OUTAGE_START = "ラクマ障害_開始"
KEY_RATE_LIMIT_PREFIX = "ラクマ制限_停止期限_"

# SQL Server の「今」を、mst.control に入れる秒数にする式
NOW_SEC_SQL = "DATEDIFF(SECOND, '20000101', SYSDATETIME())"


def worker_host() -> str:
    """scrape_worker / publish_ebay / check_remaining と同じ名前（WORKER_NAME か ホスト名）。"""
    return os.environ.get("WORKER_NAME") or socket.gethostname()


def rate_limit_key(host: str) -> str:
    return KEY_RATE_LIMIT_PREFIX + host


# 自分の台に効いている（期限内の）停止があるか。? = 自分の台の名前。publish_ebay.TAKE_ONE_SQL に埋め込む。
PAUSED_EXISTS_SQL = f"""
EXISTS (
    SELECT 1 FROM mst.control rk
    WHERE rk.param_key IN (N'{KEY_OUTAGE_UNTIL}', N'{KEY_RATE_LIMIT_PREFIX}' + ?)
      AND rk.param_value > {NOW_SEC_SQL}
)
"""

# 自分に効く停止の状態: (期限内の停止期限（datetime）, その種類, 期限切れも含めて値が残っているキーの数)
SQL_SELECT_STATE = f"""
SET NOCOUNT ON;
DECLARE @now INT = {NOW_SEC_SQL};
SELECT
    DATEADD(SECOND, MAX(CASE WHEN param_value > @now THEN param_value END), '20000101'),
    MAX(CASE WHEN param_value > @now
             THEN CASE WHEN param_key = N'{KEY_OUTAGE_UNTIL}' THEN N'OUTAGE' ELSE N'RATE_LIMIT' END END),
    SUM(CASE WHEN param_value IS NOT NULL THEN 1 ELSE 0 END)
FROM mst.control
WHERE param_key IN (N'{KEY_OUTAGE_UNTIL}', ?);
"""

# 停止期限を延ばす（遅い方だけ残す）。キーが無ければ作る。開始キー（5xx のときだけ）は、
# それまで停止していなかった（NULL か期限切れ）ときに今の時刻にする。
# 返り値: (変更前の停止期限（秒）, 変更後の停止期限（秒）, 今（秒）)
SQL_EXTEND = f"""
SET NOCOUNT ON;
DECLARE @now INT = {NOW_SEC_SQL};
DECLARE @until INT = @now + ?;
DECLARE @key NVARCHAR(200) = ?;
DECLARE @start_key NVARCHAR(200) = ?;
DECLARE @old INT, @found INT;

BEGIN TRAN;
SELECT @old = param_value FROM mst.control WITH (UPDLOCK, HOLDLOCK) WHERE param_key = @key;
SET @found = @@ROWCOUNT;
IF @found = 0
    INSERT INTO mst.control (param_key, param_value) VALUES (@key, @until);
ELSE
    UPDATE mst.control
       SET param_value = CASE WHEN param_value > @until THEN param_value ELSE @until END
     WHERE param_key = @key;

IF @start_key IS NOT NULL AND (@old IS NULL OR @old <= @now)
BEGIN
    UPDATE mst.control WITH (UPDLOCK, HOLDLOCK) SET param_value = @now WHERE param_key = @start_key;
    IF @@ROWCOUNT = 0
        INSERT INTO mst.control (param_key, param_value) VALUES (@start_key, @now);
END
COMMIT;

SELECT @old, (SELECT param_value FROM mst.control WHERE param_key = @key), @now;
"""

# 成功したとき: 期限が過ぎている停止だけを NULL に戻す（5xx は開始キーも）。期限内の停止には触れない。
# 返り値: (戻した5xxの開始（秒）か NULL, 戻した5xxの件数, 戻した429の件数, 今（秒）)
SQL_CLEAR_EXPIRED = f"""
SET NOCOUNT ON;
DECLARE @now INT = {NOW_SEC_SQL};
DECLARE @rl_key NVARCHAR(200) = ?;
DECLARE @outage INT = 0, @rl INT = 0, @start INT = NULL;

BEGIN TRAN;
UPDATE mst.control WITH (UPDLOCK, HOLDLOCK)
   SET param_value = NULL
 WHERE param_key = N'{KEY_OUTAGE_UNTIL}' AND param_value IS NOT NULL AND param_value <= @now;
SET @outage = @@ROWCOUNT;
IF @outage > 0
BEGIN
    SELECT @start = param_value FROM mst.control WITH (UPDLOCK, HOLDLOCK) WHERE param_key = N'{KEY_OUTAGE_START}';
    UPDATE mst.control SET param_value = NULL WHERE param_key = N'{KEY_OUTAGE_START}';
END
UPDATE mst.control WITH (UPDLOCK, HOLDLOCK)
   SET param_value = NULL
 WHERE param_key = @rl_key AND param_value IS NOT NULL AND param_value <= @now;
SET @rl = @@ROWCOUNT;
COMMIT;

SELECT @start, @outage, @rl, @now;
"""


class RakumaHttpError(Exception):
    """ラクマのページの取得が 2xx / 404 以外で失敗した。原因調査用にレスポンスの情報を持つ。"""

    def __init__(self, url: str, status_code: int, retry_after: Optional[str],
                 server: Optional[str], content_type: Optional[str], body_head: str,
                 title: Optional[str] = None):
        self.url = url
        self.status_code = status_code
        self.retry_after = retry_after
        self.server = server
        self.content_type = content_type
        self.body_head = body_head
        self.title = title
        super().__init__(
            f"RAKUMA_HTTP status={status_code} retry_after={retry_after} server={server} "
            f"content_type={content_type} title={title!r} url={url} body_head={body_head!r}"
        )

    @property
    def transient(self) -> bool:
        return self.status_code in TRANSIENT_HTTP_STATUSES


class _RakumaNotFetchedError(RakumaHttpError):
    """HTTP の応答とは別の理由でラクマへアクセスしなかった・結果を記録できなかった（いつも一時的なエラー）。"""

    def __init__(self, message: str, url: str):
        Exception.__init__(self, message)
        self.url = url
        self.status_code = None
        self.retry_after = None
        self.server = None
        self.content_type = None
        self.body_head = ""
        self.title = None

    @property
    def transient(self) -> bool:
        return True


class RakumaPausedError(_RakumaNotFetchedError):
    """障害中のため、ラクマへアクセスしなかった（HTTP リクエストは送っていない）。"""

    def __init__(self, url: str, kind: Optional[str], paused_until):
        super().__init__(f"RAKUMA_PAUSED kind={kind} paused_until={paused_until} url={url}", url)
        self.kind = kind
        self.paused_until = paused_until


class RakumaStateError(_RakumaNotFetchedError):
    """障害状態（mst.control）の読み書きに失敗した（DB の接続エラー等）。商品の解析失敗ではない。"""

    def __init__(self, url: str, cause: BaseException):
        super().__init__(f"RAKUMA_STATE_DB_ERROR {type(cause).__name__}: {str(cause)[:300]} url={url}", url)
        self.cause = cause


# 時間をおけば回復する見込みのあるHTTPステータス。scrape_worker はこれらのときジョブを再試行待ちにする。
# 2026-10-28 01:06〜02:00 に fril.jp が全ワーカーへ 503 を返し、fetch_active の318ジョブが error のまま残った。
TRANSIENT_HTTP_STATUSES = {RATE_LIMIT_STATUS, *OUTAGE_STATUSES}

# エラーログに残すレスポンス本文の先頭の文字数
ERROR_BODY_HEAD_CHARS = 300

# プロセス内の覚え: {"checked": monotonic, "paused_until": datetime|None, "kind": str|None, "open": int}
_state: Optional[dict] = None


def _run(sql: str, params: tuple, url: str = ""):
    """SQL を1回実行して最後の結果の1行を返す。DB の失敗は RakumaStateError にする。"""
    try:
        conn = get_sql_server_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            return row
        finally:
            conn.close()
    except Exception as e:
        raise RakumaStateError(url, e) from e


def _current_state(url: str = "") -> dict:
    global _state
    if _state is None or time.monotonic() - _state["checked"] >= RAKUMA_STATE_CACHE_SEC:
        row = _run(SQL_SELECT_STATE, (rate_limit_key(worker_host()),), url)
        _state = {
            "checked": time.monotonic(),
            "paused_until": row[0] if row else None,
            "kind": row[1] if row else None,
            "open": int(row[2] or 0) if row else 0,
        }
    return _state


def _forget_state() -> None:
    global _state
    _state = None


def raise_if_paused(url: str) -> None:
    state = _current_state(url)
    if state["paused_until"] is not None:
        raise RakumaPausedError(url, state["kind"], state["paused_until"])


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def page_title(text: str) -> Optional[str]:
    """エラーページの <title>（メンテナンス表示か、アクセス制限の表示かを後から見分けるため）。"""
    m = _TITLE_RE.search(text or "")
    if not m:
        return None
    return " ".join(m.group(1).split())[:200] or None


def _retry_after_sec(retry_after: Optional[str]) -> int:
    if retry_after and retry_after.strip().isdigit():
        return min(int(retry_after.strip()), RATE_LIMIT_PAUSE_MAX_SEC)
    return RATE_LIMIT_PAUSE_SEC


def _sec_text(sec) -> str:
    if sec is None:
        return "-"
    return (datetime(2000, 1, 1) + timedelta(seconds=int(sec))).strftime("%Y-%m-%d %H:%M:%S")


def record_response(status_code: int, url: str, retry_after: Optional[str] = None) -> None:
    """
    ラクマへの HTTP アクセスの結果を記録する（2xx・404 は成功、5xx は全台の障害、429 はこの台の制限）。
    それ以外のエラー（403 など）は障害として扱わない（従来どおり呼び出し側の処理に任せる）。
    """
    host = worker_host()
    if status_code in OUTAGE_STATUSES:
        old, new, now = _run(SQL_EXTEND, (OUTAGE_PAUSE_SEC, KEY_OUTAGE_UNTIL, KEY_OUTAGE_START), url)
        _forget_state()
        began = old is None or old <= now
        print(f"[RAKUMA OUTAGE] status={status_code} host={host} → 全台のラクマのアクセスを {_sec_text(new)} まで止めます"
              f"{'（障害の開始）' if began else '（継続・延長）'} url={url}", flush=True)
        return

    if status_code == RATE_LIMIT_STATUS:
        pause = _retry_after_sec(retry_after)
        old, new, now = _run(SQL_EXTEND, (pause, rate_limit_key(host), None), url)
        _forget_state()
        print(f"[RAKUMA RATE_LIMIT] status=429 retry_after={retry_after} → この台({host})のラクマのアクセスを "
              f"{_sec_text(new)} まで止めます url={url}", flush=True)
        return

    if status_code < 400 or status_code == 404:
        if _current_state(url)["open"] > 0:
            start, outage, rl, now = _run(SQL_CLEAR_EXPIRED, (rate_limit_key(host),), url)
            _forget_state()
            if outage:
                minutes = (now - start) // 60 if start is not None else None
                print(f"[RAKUMA RECOVERED] ラクマへのアクセスが成功したため、障害を終了しました host={host} "
                      f"開始={_sec_text(start)} 所要={minutes}分", flush=True)
            if rl:
                print(f"[RAKUMA RECOVERED] この台({host})の 429 による停止を終了しました", flush=True)


def check_response(resp, url: str) -> None:
    """
    requests のレスポンスを記録し、2xx / 404 以外なら RakumaHttpError を送出する
    （ログには応答の情報だけを出す。リクエストヘッダー・Cookie は出さない）。
    応答の内容（ページの title 等）はログと、呼び出し側の記録（scrape_job.error_message 等）に残る。
    """
    if resp.status_code < 400 or resp.status_code == 404:
        record_response(resp.status_code, url)
        return
    err = RakumaHttpError(
        url=url,
        status_code=resp.status_code,
        retry_after=resp.headers.get("Retry-After"),
        server=resp.headers.get("Server"),
        content_type=resp.headers.get("Content-Type"),
        body_head=" ".join(resp.text[:ERROR_BODY_HEAD_CHARS].split()),
        title=page_title(resp.text),
    )
    print(f"[RAKUMA HTTP ERROR] {err}", flush=True)
    record_response(resp.status_code, url, retry_after=err.retry_after)
    raise err
