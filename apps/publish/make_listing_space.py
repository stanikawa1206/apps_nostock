# -*- coding: utf-8 -*-
"""
出品枠確保処理

指定したアカウントについて、出品枠を確保する。
現時点では「watchers=0 の出品を削除する」ことで枠を確保しているが、
将来的には --count 指定・必要枠数だけ確保する仕様・LIMIT判定などを
追加していく前提の構成にしている。
"""

import logging
import time
from contextlib import contextmanager
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from apps.common.utils import get_sql_server_connection, log_listings_change
from apps.adapters.ebay_api import delete_items_from_ebay_batch, get_listing_status, mark_listing_deleted_if_ended


# ===== アカウント単位の排他 =====
# 出品処理の LIMIT 対応（make_listing_space）と、在庫管理の値上げ時の販売上限対応
# （free_listing_space_for_price_up。複数のPC・ワーカーで動く）が、同じアカウントの古い出品を
# 同時に終了しないよう、SQL Server のアプリケーションロックでアカウントごとに1つずつ処理する。
# ロックは専用の接続（セッション）が持ち、接続を閉じると解放される（プロセスが落ちても残らない）。
LIMIT_LOCK_TIMEOUT_SEC = 600      # LIMIT 対応の待ち上限（値上げ側は1回 数分以内で終わる）


@contextmanager
def account_listing_space_lock(account, timeout_sec):
    """
    アカウント単位の排他ロックを取る。with の値は sp_getapplock の戻り値
    （0: すぐ取得 / 1: 待って取得 / 負: 取得できず）。
    """
    conn = get_sql_server_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SET NOCOUNT ON;
                DECLARE @r int;
                EXEC @r = sp_getapplock @Resource = ?, @LockMode = 'Exclusive',
                                        @LockOwner = 'Session', @LockTimeout = ?;
                SELECT @r;
            """, (f"listing_space:{account}", int(timeout_sec * 1000)))
            rc = cur.fetchone()[0]
        yield rc
    finally:
        conn.close()


# ===== [一時デバッグ] DELETE DEBUG専用ログファイル =====
# actual_count=0になる原因調査用の一時ログ。logs/delete_debug.log にタイムスタンプ付きで出力する。
# 調査が終わったらこのブロックごと削除してよい。
_DELETE_DEBUG_LOG_PATH = Path(__file__).resolve().parents[2] / "logs" / "delete_debug.log"
_DELETE_DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

_delete_debug_logger = logging.getLogger("delete_debug")
if not _delete_debug_logger.handlers:
    _delete_debug_logger.setLevel(logging.DEBUG)
    _delete_debug_handler = logging.FileHandler(_DELETE_DEBUG_LOG_PATH, encoding="utf-8")
    _delete_debug_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _delete_debug_logger.addHandler(_delete_debug_handler)
    _delete_debug_logger.propagate = False


def _log_delete_debug(msg: str) -> None:
    """[一時デバッグ] コンソール(print)と logs/delete_debug.log の両方に同じ内容を出力する。"""
    print(msg)
    _delete_debug_logger.debug(msg)


# ===== 設定 =====
# account -> 今回確保したい出品枠数
LISTING_SPACE_CONFIG = {
    "BUZZ②": 12,
}


MAX_WORKERS = 2
BATCH_SIZE  = 10


# ===== 出品枠確保候補取得 =====
SQL_SELECT_CANDIDATES = """
SELECT TOP (?) listing_id
FROM ext.ebay_active_download
WHERE
    account = ?
    AND LEN(vendor_item_id) >= 6
    AND watchers = 0
ORDER BY start_time
"""


def get_listing_space_candidates(account, limit):

    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_CANDIDATES, limit, account)
            rows = cur.fetchall()

        item_ids = [str(row[0]) for row in rows]

        print(f"=== {account} 出品枠確保候補件数: {len(item_ids)} ===")
        for iid in item_ids:
            print(f"  候補 item_id={iid}")

        return item_ids

    finally:
        conn.close()


# ===== 出品枠確保処理 =====
def secure_listing_space_for_account(account, item_ids):

    if not item_ids:
        return account, 0

    conn = get_sql_server_connection()
    secured = 0

    try:
        idx = 0
        batch_no = 0
        while idx < len(item_ids):

            batch_no += 1
            batch = item_ids[idx: idx + BATCH_SIZE]

            result = delete_items_from_ebay_batch(account, batch, batch_no=batch_no)

            # ===== [一時デバッグ] delete_items_from_ebay_batch の戻り値をそのまま出力 =====
            _log_delete_debug("[DELETE DEBUG]")
            _log_delete_debug(f"account={account} batch_no={batch_no}")
            _log_delete_debug(f"requested_item_ids={batch}")
            _log_delete_debug(f"result={result!r}")

            if not isinstance(result, dict):
                _log_delete_debug(f"⚠️ {account}: API異常")
                _log_delete_debug(f"[DELETE DEBUG] account={account} batch_no={batch_no} result型異常のためbreak: type={type(result)!r}")
                break

            res_list = result.get("results") or []

            ok_ids = []
            for r in res_list:
                if r.get("success"):
                    ok_ids.append(str(r.get("item_id")))

            # ===== [一時デバッグ] このバッチで何件確保できたか／できなかった理由 =====
            _log_delete_debug(
                "[DELETE DEBUG] "
                f"account={account} batch_no={batch_no} "
                f"requested={len(batch)} "
                f"deleted={len(ok_ids)} "
                f"success={result.get('success')} "
                f"error_code={result.get('error_code')} "
                f"error_message={result.get('error_message') or result.get('message')} "
                f"raw_response={result.get('raw_response')!r} "
                f"results={res_list!r}"
            )

            if not ok_ids:
                _log_delete_debug(
                    f"[DELETE DEBUG] account={account} batch_no={batch_no} このバッチはdeleted=0件。"
                    f"success={result.get('success')} のため成功扱いのitemが無かったか、"
                    f"resultsキー自体が無い(no_token/parse_error等)ことが原因です。"
                )

            # ===== DB更新 =====
            if ok_ids:
                with conn.cursor() as cur:
                    for iid in ok_ids:
                        delete_reason = "定期削除(watch0)"
                        cur.execute("""
                            UPDATE trx.listings
                               SET is_deleted = 1,
                                   deleted_at = SYSDATETIME(),
                                   delete_reason = ?
                             OUTPUT deleted.vendor_item_id
                             WHERE account = ?
                               AND listing_id = ?
                               AND ISNULL(is_deleted, 0) = 0
                        """, (delete_reason, account, iid))
                        row = cur.fetchone()
                        log_listings_change(
                            "DELETE", account, iid, row[0] if row else None, delete_reason
                        )

                        cur.execute("""
                            DELETE FROM ext.ebay_active_download
                            WHERE account = ?
                            AND listing_id = ?
                        """, (account, iid))


                conn.commit()

                secured += len(ok_ids)

            idx += BATCH_SIZE

        # ===== [一時デバッグ] 最終結果サマリー(actual_count==0の場合に理由が追えるように) =====
        if secured == 0:
            _log_delete_debug(
                f"[DELETE DEBUG] account={account} requested_total={len(item_ids)} batch_count={batch_no} "
                f"actual_count=0 (delete_items_from_ebay_batch が1件も success を返しませんでした。"
                f"logs/delete_debug.log の同一account/batch_noの result= / error_code= を確認してください)"
            )
        else:
            _log_delete_debug(f"[DELETE DEBUG] account={account} actual_count={secured} (requested_total={len(item_ids)}, batch_count={batch_no})")

        return account, secured

    finally:
        conn.close()


def _secure_listing_space_locked(account, space_count):
    """候補の取得から終了・DB更新までを、アカウント単位の排他ロックを持ったまま行う。"""
    with account_listing_space_lock(account, LIMIT_LOCK_TIMEOUT_SEC) as rc:
        if rc < 0:
            _log_delete_debug(f"⚠️ {account}: 出品枠確保の排他ロックを{LIMIT_LOCK_TIMEOUT_SEC}秒で取得できず rc={rc}")
            return account, 0
        item_ids = get_listing_space_candidates(account, space_count)
        return secure_listing_space_for_account(account, item_ids)


# ===== メイン =====
def make_listing_space(config=None):
    """
    指定アカウント分の出品枠を確保する。

    config: {account: 確保したい件数} の辞書。
            省略時は LISTING_SPACE_CONFIG を使う。
            将来 daily_check_publish.py から必要件数を渡せるようにするための拡張ポイント。
    """

    target_config = config if config is not None else LISTING_SPACE_CONFIG

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for account, space_count in target_config.items():

            print(f"\n▶ {account}: {space_count}件分の出品枠確保対象取得")

            futures.append(
                executor.submit(_secure_listing_space_locked, account, space_count)
            )

        for f in as_completed(futures):
            results.append(f.result())

    print("\n=== 出品枠確保結果 ===")
    total = 0
    for account, cnt in results:
        print(f"{account}: {cnt}件分の出品枠を確保")
        total += cnt

    print(f"合計確保: {total}")

    return dict(results)


def run_make_listing_space(account, count):
    """
    他モジュール（publish_manager.py 等）から呼び出すための、
    「1アカウント分だけ出品枠を確保する」シンプルな公開関数。

    内部的には既存の make_listing_space() にそのまま委譲するだけで、
    削除ロジック自体（候補の選び方・削除の実行方法）は一切変更しない。

    account: 出品枠を確保したいアカウント名
    count:   確保したい件数（呼び出し側で計算したremaining等）
    戻り値:  実際に確保できた件数（0件の場合や、countに届かない場合もある）
    """

    results = make_listing_space(config={account: count})
    return results.get(account, 0)


# ===== 値上げ時の販売上限（25002 "amount you can list"）対応 =====
# 値上げが月間の販売上限で失敗した時、LIMIT 対応と同じ基準（watchers=0 の古い順）で出品を1件終了し、
# 値上げを再試行する。上限のままなら次の1件を終了して再試行する。
# 上限まで試しても通らなければ、呼び出し元は従来どおり値上げ対象の出品を終了する。
# （2026-11-08 谷川④は出品総額が上限に張り付き、値上げのたびに値上げ対象が終了していた。
#   同日の LIMIT 対応では古い出品の終了で枠がすぐ戻ったが、反映の時間差に備えて待って再試行する）
PRICE_UP_MAX_ENDS = 3               # 1回の値上げで終了する古い出品の上限
PRICE_UP_MAX_CANDIDATES = 10        # 確認する候補の上限（GetItem で出品中でない等の理由で飛ばした分も含む）
PRICE_UP_RETRY_WAITS = [5, 20]      # 1件終了した後、値上げを再試行するまでの待ち（秒）
PRICE_UP_END_CHECK_WAITS = [2, 5, 10]   # 終了要求の後、GetItem で終了を確認するまでの待ち（秒）
PRICE_UP_LOCK_TIMEOUT_SEC = 180
PRICE_UP_DELETE_REASON = "値上げ枠確保(watch0)"
PRICE_PENDING_PREFIX_LIKE = "価格更新失敗(再試行待ち)%"   # mercari_item_status.PRICE_PENDING_PREFIX

SQL_SELECT_PRICE_UP_CANDIDATE = """
SELECT TOP (1) d.listing_id, d.vendor_item_id
FROM ext.ebay_active_download d
JOIN trx.listings l
  ON l.account = d.account AND l.listing_id = d.listing_id AND l.is_deleted = 0
WHERE
    d.account = ?
    AND LEN(d.vendor_item_id) >= 6
    AND d.watchers = 0
    AND d.listing_id <> ?
    AND d.vendor_item_id <> ?
    AND ISNULL(l.error_message, '') NOT LIKE ?
    AND NOT EXISTS (
        SELECT 1 FROM trx.ebay_orders o WHERE o.account = d.account AND o.ebay_id = d.listing_id
    )
    {skip}
ORDER BY d.start_time
"""


def _is_selling_limit_resp(resp) -> bool:
    return bool(resp) and not resp.get("success") and bool(resp.get("selling_limit"))


def _pick_price_up_candidate(account, listing_id, sku, skip_ids):
    skip = f"AND d.listing_id NOT IN ({','.join('?' * len(skip_ids))})" if skip_ids else ""
    conn = get_sql_server_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_SELECT_PRICE_UP_CANDIDATE.format(skip=skip),
                        (account, listing_id, sku, PRICE_PENDING_PREFIX_LIKE, *skip_ids))
            row = cur.fetchone()
        return (str(row[0]), str(row[1])) if row else None
    finally:
        conn.close()


def _end_listing_for_price_up(account, listing_id, sku) -> bool:
    """
    出品を1件終了し、GetItem で終了を確認してから DB を削除済みにする。
    終了前に GetItem で出品中（Active・販売数0）であることを確かめ、そうでなければ終了しない。
    """
    st = get_listing_status(account, listing_id)
    if not (st.get("ok") and st.get("listing_status") == "Active" and st.get("quantity_sold") in ("", "0")):
        print(f"[PRICE_UP_SPACE][SKIP] account={account} listing_id={listing_id} sku={sku} "
              f"出品中・販売なしを確認できないため終了しない status={st!r}", flush=True)
        return False

    res = delete_items_from_ebay_batch(account, [listing_id])
    print(f"[PRICE_UP_SPACE][END] account={account} listing_id={listing_id} sku={sku} "
          f"result={res.get('results') or res}", flush=True)
    for wait in PRICE_UP_END_CHECK_WAITS:
        time.sleep(wait)
        if mark_listing_deleted_if_ended(account, listing_id, sku, PRICE_UP_DELETE_REASON):
            conn = get_sql_server_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM ext.ebay_active_download WHERE account = ? AND listing_id = ?",
                                (account, listing_id))
                conn.commit()
            finally:
                conn.close()
            return True
    print(f"[PRICE_UP_SPACE][END_UNCONFIRMED] account={account} listing_id={listing_id} sku={sku} "
          f"eBayの終了を確認できずDB変更なし", flush=True)
    return False


def free_listing_space_for_price_up(account, listing_id, sku, retry_price_up):
    """
    値上げが販売上限で失敗した出品（account / listing_id / sku）について、古い出品を1件ずつ終了して値上げを再試行する。
    retry_price_up: 値上げを1回試し、update_ebay_price_rest の結果を返す関数。
    戻り値: {"resp": 最後の値上げ結果（試していなければ None）, "ended": 終了した listing_id,
             "attempts": 値上げの再試行回数, "waited_sec": 待った秒数, "note": 打ち切った理由}
    """
    out = {"resp": None, "ended": [], "attempts": 0, "waited_sec": 0, "note": ""}
    tag = f"account={account} listing_id={listing_id} sku={sku}"

    def retry():
        out["attempts"] += 1
        out["resp"] = retry_price_up()
        print(f"[PRICE_UP_SPACE][RETRY] {tag} attempt={out['attempts']} ended={len(out['ended'])} "
              f"success={bool(out['resp'] and out['resp'].get('success'))} "
              f"error={(out['resp'] or {}).get('error')}", flush=True)
        return not _is_selling_limit_resp(out["resp"])

    with account_listing_space_lock(account, PRICE_UP_LOCK_TIMEOUT_SEC) as rc:
        if rc < 0:
            out["note"] = f"排他ロックを{PRICE_UP_LOCK_TIMEOUT_SEC}秒で取得できず(rc={rc})"
            print(f"[PRICE_UP_SPACE][LOCK_NG] {tag} {out['note']}", flush=True)
            return out
        # ロックを待った間に、他の処理が出品を終了して枠が空いている可能性があるため先に1回試す
        if rc == 1 and retry():
            return out

        skipped = []
        while len(out["ended"]) < PRICE_UP_MAX_ENDS:
            if len(out["ended"]) + len(skipped) >= PRICE_UP_MAX_CANDIDATES:
                out["note"] = f"候補の確認が上限{PRICE_UP_MAX_CANDIDATES}件に達した"
                break
            cand = _pick_price_up_candidate(account, listing_id, sku, skipped + out["ended"])
            if not cand:
                out["note"] = "終了できる候補（watchers=0）がない"
                break
            cand_id, cand_sku = cand
            if not _end_listing_for_price_up(account, cand_id, cand_sku):
                skipped.append(cand_id)
                continue
            out["ended"].append(cand_id)
            for wait in PRICE_UP_RETRY_WAITS:
                time.sleep(wait)
                out["waited_sec"] += wait
                if retry():
                    return out
        else:
            out["note"] = f"終了の上限{PRICE_UP_MAX_ENDS}件に達した"

    print(f"[PRICE_UP_SPACE][GIVE_UP] {tag} ended={out['ended']} attempts={out['attempts']} {out['note']}", flush=True)
    return out


if __name__ == "__main__":
    make_listing_space()
