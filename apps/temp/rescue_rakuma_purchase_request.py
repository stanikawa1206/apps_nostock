"""
rescue_rakuma_purchase_request.py — 一時プログラム（2026-10-15 ラクマ購入申請誤判定の救済）

## 背景
2026-10-15、ラクマの商品詳細ページのHTML構造が <span class="item__icon request-required">
から <div class="item__icon request-required"> へ変更され、判定コードがタグ固定の
CSSセレクタを使っていたため、購入申請不要の商品まで大量に NG(購入申請あり) と
誤判定された（apps/adapters/rakuma_item_status.py は同日中に修正・全8ワーカーへ反映済み）。

誤判定された商品は trx.vendor_item に NG(購入申請あり) のまま残っており、さらに
fn_take_one_candidates_base の条件⑦-2（30日間再チェック抑制）により、今後30日間
出品候補から除外され続けてしまう。本プログラムは、これらの商品を1件ずつ低速・逐次で
再取得し、実際に「購入申請不要（すぐに購入可）」であることを現在のHTMLで確認できた
商品に限り、通常の出品ワーカーが改めて詳細チェックできる状態（出品状況クリア）に
戻す。

## 対象
  trx.vendor_item
    WHERE vendor_name = N'ラクマ'
      AND [出品状況] = N'NG(購入申請あり)'
      AND [出品状況詳細] = N'request-required span not found'（旧コードの文言のまま
                                                              ＝修正済みコードで一度も
                                                              再評価されていない商品）

  当初 last_checked_at による期間絞り込み（HTML変更時刻〜全8ワーカー再起動完了時刻）を
  試みたが、2026-10-16 の調査で scrape_worker.py の upsert_vendor_items()（在庫スクレイプ
  fetch_active_ebay/fetch_sold_ebay が使うMERGE）が [出品状況]/[出品状況詳細] に触れずに
  last_checked_at だけを更新することが判明し、last_checked_at は「修正済みコードで
  再評価済みか」の指標として使えないと確定した。そのため対象抽出条件からは last_checked_at
  を外し、[出品状況詳細] の文言（修正コミット e9311b1 でメッセージ自体が
  "request-required span not found" → "request-required element not found" に
  変わっている）だけで判定する。

## 判定ロジックの再利用
  判定は apps.adapters.rakuma_item_status.parse_detail_rakuma() をそのまま呼び出す
  （新たな購入申請判定ロジックは実装しない）。同関数が内部で1回だけ行う
  fetch_rakuma_item_html() のHTTP GETがそのまま商品取得の唯一のアクセスになる。

## 更新仕様（購入申請不要と確認できた場合のみ）
  UPDATE trx.vendor_item
     SET [出品状況] = N'', [出品状況詳細] = N'',
         processing_by = NULL, processing_at = NULL
   WHERE vendor_name = ? AND vendor_item_id = ?
     AND [出品状況] = N'NG(購入申請あり)'      -- 更新直前の楽観ロック確認
     AND last_checked_at = ?                   -- 取得時点のスナップショットと一致する場合のみ

  processing_at / processing_by のクリアについて: fn_take_one_candidates_base の条件①は
  processing_at IS NULL を要求するため、出品状況だけクリアしても候補には戻らない。
  2026-10-15、ユーザーの明示的な承認を得た上で、救済確定した商品に限定してクリアする
  （対象外の商品やNG維持・判定不能の商品には一切触れない）。

  「購入申請あり（NG維持）」「判定不能・取得失敗」の場合はDBを一切更新しない。

## 二重処理防止・再開
  本スクリプト自身の実行ログ（RESULT_LOG_PATH, JSON Lines）を唯一の進捗管理に使う。
  各商品ごとに最新の記録を見て、確定済み（rescued / kept_ng）ならスキップ、
  エラー・判定不能は MAX_ATTEMPTS_PER_ITEM 回まで次回実行時に再試行する。

## アクセス制御
  単一ワーカー・逐次処理。1件ごとに PER_ITEM_WAIT_SEC 秒（ランダム）待機。
  HTTP 429/403・タイムアウト・不完全HTMLが連続 CONSECUTIVE_FAILURE_LIMIT 回発生したら
  自動停止する（それ以上ラクマへアクセスしない）。

## 使い方
  python -m apps.temp.rescue_rakuma_purchase_request --mode dry-run
  python -m apps.temp.rescue_rakuma_purchase_request --mode test --limit 5
  python -m apps.temp.rescue_rakuma_purchase_request --mode run [--limit N]
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from apps.common.utils import get_sql_server_connection
from apps.adapters.rakuma_item_status import (
    parse_detail_rakuma,
    RakumaItemUnavailableError,
)

# ======================
# 設定
# ======================
# 2026-10-16 の調査で判明: last_checked_at は scrape_worker.py の upsert_vendor_items()
# （fetch_active_ebay/fetch_sold_ebay が使うMERGE）でも更新されるが、このMERGEは
# [出品状況]/[出品状況詳細] に一切触れない。そのため「last_checked_at が新しい = 修正済み
# コードで再評価済み」とは言えず、last_checked_at を対象抽出条件に使うのは不正確（在庫
# スクレイプにより誤判定が残ったまま対象から漏れる）。
#
# 正しい判定は [出品状況詳細] の文言で行う。修正コミット e9311b1 でメッセージ文言自体が
# 変わっており、これが「修正済みコードで再評価済みか」の確実な指標になる:
#   旧コード(バグ): "request-required span not found"  … 未再評価（対象）
#   新コード(修正済み): "request-required element not found"  … 再評価済み（対象外）
TARGET_DETAIL_TEXT = "request-required span not found"
PER_ITEM_WAIT_MIN_SEC = 8
PER_ITEM_WAIT_MAX_SEC = 15
MAX_ATTEMPTS_PER_ITEM = 3
CONSECUTIVE_FAILURE_LIMIT = 5
BACKOFF_BASE_SEC = 60  # 連続失敗1回あたりの追加待機（60, 120, 180, ...）

RESULT_LOG_PATH = Path(__file__).resolve().parent / "rescue_rakuma_purchase_request_log.jsonl"

TARGET_SQL = """
SELECT vendor_name, vendor_item_id, [出品状況], last_checked_at, processing_at, processing_by
FROM trx.vendor_item
WHERE vendor_name = N'ラクマ'
  AND [出品状況] = N'NG(購入申請あり)'
  AND [出品状況詳細] = ?
ORDER BY last_checked_at ASC
"""

RESCUE_UPDATE_SQL = """
UPDATE trx.vendor_item
SET [出品状況] = N'',
    [出品状況詳細] = N'',
    processing_by = NULL,
    processing_at = NULL
WHERE vendor_name = ?
  AND vendor_item_id = ?
  AND [出品状況] = N'NG(購入申請あり)'
  AND last_checked_at = ?
"""


def log_event(record: dict) -> None:
    record = {"ts": datetime.now().isoformat(timespec="seconds"), **record}
    with open(RESULT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_progress() -> dict:
    """
    vendor_item_id -> {"final": bool, "outcome": str, "attempts": int}
    final=True (rescued / kept_ng / deleted) のものは今後完全スキップ。
    final=False (error / inconclusive) は MAX_ATTEMPTS_PER_ITEM まで再試行対象。
    """
    progress: dict = {}
    if not RESULT_LOG_PATH.exists():
        return progress
    with open(RESULT_LOG_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            vid = rec.get("vendor_item_id")
            if not vid:
                continue
            outcome = rec.get("outcome")
            entry = progress.setdefault(vid, {"final": False, "outcome": None, "attempts": 0})
            if outcome in ("rescued", "kept_ng", "deleted"):
                entry["final"] = True
                entry["outcome"] = outcome
            else:
                entry["attempts"] += 1
                entry["outcome"] = outcome
    return progress


def fetch_targets(conn) -> list:
    cur = conn.cursor()
    cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cur.execute(TARGET_SQL, TARGET_DETAIL_TEXT)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


class StopRequested(Exception):
    pass


def classify_item(vendor_name: str, vendor_item_id: str) -> tuple:
    """
    既存の parse_detail_rakuma() をそのまま呼び出して判定する。
    戻り値: (outcome, detail)
      outcome in {"rescued", "kept_ng", "inconclusive", "deleted", "rate_limited", "error"}
    """
    url = f"https://item.fril.jp/{vendor_item_id}"
    try:
        rec = parse_detail_rakuma(url, preset="", vendor_name=vendor_name)
    except RakumaItemUnavailableError as e:
        return "deleted", str(e)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status in (429, 403):
            return "rate_limited", f"HTTP {status}"
        return "error", f"HTTPError: {e}"
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        return "error", f"network: {e}"
    except Exception as e:
        return "error", f"unexpected: {e}"

    # 不完全HTML・判定不能の簡易サニティチェック:
    # タイトル・画像・価格のいずれも取れていない場合は、ページ自体が壊れている/
    # アクセス制限されている可能性が高く、購入申請の有無を確定させない。
    has_title = bool(rec.get("title_jp"))
    has_images = bool(rec.get("images"))
    has_price = rec.get("price") is not None
    if not (has_title or has_images or has_price):
        return "inconclusive", "page looked incomplete (no title/images/price)"

    if rec.get("purchase_request_required") is True:
        return "kept_ng", "request-required element still absent"
    elif rec.get("purchase_request_required") is False:
        return "rescued", "request-required element present (すぐに購入可)"
    else:
        return "inconclusive", "purchase_request_required unknown"


def try_update_rescued(conn, item: dict) -> bool:
    """
    楽観ロック付きでDB更新する。他ワーカーが既に変更していれば0行更新でFalseを返す。
    """
    cur = conn.cursor()
    cur.execute(
        RESCUE_UPDATE_SQL,
        item["vendor_name"],
        item["vendor_item_id"],
        item["last_checked_at"],
    )
    updated = cur.rowcount
    conn.commit()
    return updated > 0


def run(mode: str, limit: Optional[int]) -> None:
    conn = get_sql_server_connection()
    conn.autocommit = False

    targets = fetch_targets(conn)
    print(f"[TARGET] 対象件数(出品状況=NG(購入申請あり), 出品状況詳細={TARGET_DETAIL_TEXT!r}): {len(targets)}件")

    progress = load_progress()
    pending = []
    skipped_final = 0
    skipped_exhausted = 0
    for item in targets:
        vid = item["vendor_item_id"]
        entry = progress.get(vid)
        if entry is None:
            pending.append(item)
            continue
        if entry["final"]:
            skipped_final += 1
            continue
        if entry["attempts"] >= MAX_ATTEMPTS_PER_ITEM:
            skipped_exhausted += 1
            continue
        pending.append(item)

    print(f"[TARGET] 既に確定済み(rescued/kept_ng)でスキップ: {skipped_final}件")
    print(f"[TARGET] リトライ上限到達でスキップ: {skipped_exhausted}件")
    print(f"[TARGET] 今回処理対象(保留分): {len(pending)}件")

    if mode == "dry-run":
        print("[DRY-RUN] DB更新・ラクマへのアクセスは行いません。")
        print(f"[DRY-RUN] 実行すれば最大 {len(pending)}件 を低速(1件あたり{PER_ITEM_WAIT_MIN_SEC}〜{PER_ITEM_WAIT_MAX_SEC}秒)で処理します。")
        conn.close()
        return

    if limit is not None:
        pending = pending[:limit]
        print(f"[{mode.upper()}] --limit指定により今回の処理対象を{len(pending)}件に制限します。")

    counters = {"processed": 0, "rescued": 0, "kept_ng": 0, "inconclusive": 0, "deleted": 0, "error": 0}
    consecutive_failures = 0

    print(f"[{mode.upper()}] 処理開始。総対象={len(pending)}件")
    try:
        for idx, item in enumerate(pending, 1):
            vid = item["vendor_item_id"]
            outcome, detail = classify_item(item["vendor_name"], vid)

            if outcome == "rate_limited":
                consecutive_failures += 1
                log_event({"vendor_item_id": vid, "outcome": "error", "detail": detail})
                counters["error"] += 1
                counters["processed"] += 1
                wait = BACKOFF_BASE_SEC * consecutive_failures
                print(f"  [{idx}/{len(pending)}] {vid}: rate_limited/{detail} -> {wait}秒待機 (連続失敗{consecutive_failures}回)")
                if consecutive_failures >= CONSECUTIVE_FAILURE_LIMIT:
                    print(f"[STOP] 連続{CONSECUTIVE_FAILURE_LIMIT}回失敗したため自動停止します。")
                    raise StopRequested()
                time.sleep(wait)
                continue

            if outcome == "error":
                consecutive_failures += 1
            else:
                consecutive_failures = 0

            if outcome == "rescued":
                try:
                    updated = try_update_rescued(conn, item)
                except Exception as e:
                    conn.rollback()
                    print(f"  [{idx}/{len(pending)}] {vid}: DB更新失敗: {e}")
                    log_event({"vendor_item_id": vid, "outcome": "error", "detail": f"db_update_failed: {e}"})
                    counters["error"] += 1
                    counters["processed"] += 1
                    continue
                if updated:
                    print(f"  [{idx}/{len(pending)}] {vid}: 救済(NG解除) -> 候補へ復帰")
                    log_event({"vendor_item_id": vid, "outcome": "rescued", "detail": detail})
                    counters["rescued"] += 1
                else:
                    print(f"  [{idx}/{len(pending)}] {vid}: 他ワーカーが既に更新済みのためスキップ")
                    log_event({"vendor_item_id": vid, "outcome": "skipped_modified", "detail": "state changed since snapshot"})
            elif outcome == "kept_ng":
                print(f"  [{idx}/{len(pending)}] {vid}: NG維持(購入申請あり)")
                log_event({"vendor_item_id": vid, "outcome": "kept_ng", "detail": detail})
                counters["kept_ng"] += 1
            elif outcome == "deleted":
                print(f"  [{idx}/{len(pending)}] {vid}: 削除・取得不可 -> 変更なし")
                log_event({"vendor_item_id": vid, "outcome": "deleted", "detail": detail})
                counters["deleted"] += 1
            elif outcome == "inconclusive":
                print(f"  [{idx}/{len(pending)}] {vid}: 判定不能 -> 変更なし ({detail})")
                log_event({"vendor_item_id": vid, "outcome": "inconclusive", "detail": detail})
                counters["inconclusive"] += 1
            else:
                print(f"  [{idx}/{len(pending)}] {vid}: エラー -> 変更なし ({detail})")
                log_event({"vendor_item_id": vid, "outcome": "error", "detail": detail})
                counters["error"] += 1

            counters["processed"] += 1

            if counters["processed"] % 10 == 0 or idx == len(pending):
                remaining = len(pending) - idx
                print(
                    f"  --- 進捗: 処理済み={counters['processed']} 救済={counters['rescued']} "
                    f"NG維持={counters['kept_ng']} 判定不能={counters['inconclusive']} "
                    f"削除={counters['deleted']} エラー={counters['error']} 残り={remaining} ---"
                )

            if idx < len(pending):
                wait = random.uniform(PER_ITEM_WAIT_MIN_SEC, PER_ITEM_WAIT_MAX_SEC)
                time.sleep(wait)

    except StopRequested:
        pass
    finally:
        conn.close()

    print(f"[{mode.upper()}] 終了。処理済み={counters['processed']} 救済={counters['rescued']} "
          f"NG維持={counters['kept_ng']} 判定不能={counters['inconclusive']} "
          f"削除={counters['deleted']} エラー={counters['error']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="ラクマ購入申請誤判定の段階的救済（一時プログラム）")
    parser.add_argument("--mode", choices=["dry-run", "test", "run"], required=True)
    parser.add_argument("--limit", type=int, default=None, help="test/runモードで処理する最大件数")
    args = parser.parse_args()

    if args.mode == "test" and args.limit is None:
        args.limit = 5

    run(args.mode, args.limit)


if __name__ == "__main__":
    main()
