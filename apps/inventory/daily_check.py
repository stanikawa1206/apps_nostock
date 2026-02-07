# inventory_ebay_manager.py
# -*- coding: utf-8 -*-
import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime
import os
import re

if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ======================
# utils の読み込み
# ======================
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # D:/apps_nostock
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.common.utils import send_mail, get_sql_server_connection

# ======================
# 設定
# ======================
PYTHON = sys.executable
BASE_DIR = Path(__file__).resolve().parents[2]  # ← apps_nostock 直下

APPS_INV = BASE_DIR / "apps" / "inventory"
APPS_PUB = BASE_DIR / "apps" / "publish"
APPS_DEL = BASE_DIR / "apps" / "publish" / "delete_ebay_daily.py"

FETCH_ACTIVE = APPS_INV / "fetch_active_ebay.py"
FETCH_SOLD       = APPS_INV / "fetch_sold_ebay.py"
CHECK_REMAINING  = APPS_INV / "check_remaining_ebay.py"

# 在庫チェック後、削除 → 出品 の順で実行
DELETE_SCRIPT = APPS_DEL
PUBLISH_SCRIPT = APPS_PUB / "publish_ebay.py"

WAIT_SECONDS = 3


# ======================
# 共通関数
# ======================
def run_script(path: Path) -> tuple[int, str]:
    print(f"\n=== ▶ {path.name} 実行開始 ===")

    result = subprocess.run(
        [PYTHON, str(path)],
        cwd=str(BASE_DIR),
        capture_output=True,  # 標準出力・エラーをキャプチャ
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    # 子スクリプトの出力をそのままコンソールにも流す
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)

    if result.returncode == 0:
        print(f"=== ✅ {path.name} 正常終了 ===")
    else:
        print(f"=== ❌ {path.name} 異常終了（returncode={result.returncode}） ===")

    return result.returncode, (result.stdout or "")


def format_trx_listings_count_by_account(conn) -> str:
    """
    trx.listings の account 別件数をメール本文に載せるための整形文字列
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT account, COUNT(*) AS cnt
            FROM trx.listings
            GROUP BY account
            ORDER BY account
            """
        )
        rows = cur.fetchall()

        lines = ["【trx.listings 件数（account別）】"]
        for r in rows:
            # r[0]=account, r[1]=cnt
            lines.append(f"- {r[0]}: {r[1]}")
        return "\n".join(lines) + "\n"
    except Exception as e:
        return f"【trx.listings 件数（account別）】取得失敗: {e}\n"


def send_script_mail(
    script: Path,
    start: datetime,
    end: datetime,
    code: int,
    round_no: int | None = None,
    extra_body: str = "",
    warn_continue: bool = False,
    conn=None,  # ★追加
):
    """各スクリプトごとのメール送信用ヘルパー"""
    elapsed = end - start
    round_info = f"（{round_no}回転目）" if round_no is not None else ""

    if code == 0:
        mark = "✅"
        status = "正常終了"
    else:
        mark = "⚠️" if warn_continue else "❌"
        status = "エラー"

    subject = f"{mark} {script.name} {round_info} {status}"

    body = (
        f"スクリプト: {script.name}\n"
        f"{'回転番号: ' + str(round_no) + '\\n' if round_no is not None else ''}"
        f"開始時刻: {start}\n"
        f"終了時刻: {end}\n"
        f"処理時間: {elapsed}\n"
    )

    if warn_continue:
        body += "\n※ エラーが発生しましたが、在庫処理は続行しました。\n"

    if extra_body:
        body += "\n" + extra_body

    # ★追加：trx.listings 件数（account別）
    if conn is not None:
        body += "\n" + format_trx_listings_count_by_account(conn)

    send_mail(subject, body)

def wait_until_no_pending(conn, phase_name="active"):
    print(f"⏳ {phase_name}: pending が 0 になるのを待機中…")
    cur = conn.cursor()

    while True:
        cur.execute("""
            SELECT COUNT(*)
            FROM trx.scrape_job
            WHERE status = 'pending'
        """)
        pending = cur.fetchone()[0]

        if pending == 0:
            print(f"✅ {phase_name}: pending 消滅")
            return

        print(f"… pending={pending} 件")
        time.sleep(30)

# ======================
# メイン処理
# ======================
def main():
    conn = get_sql_server_connection()
    try:
        SET_N = 1
        print(f"=== 🧭 inventory_ebay_manager.py 開始（4工程×{SET_N}回転） ===")

        for set_no in range(1, SET_N + 1):
            print("\n\n==============================")
            print(f"🔁 セット {set_no} / {SET_N} 開始")
            print("   事前sold → フル在庫チェック → delete → publish")
            print("==============================")

            # ------------------------------------------------
            # ① 事前 sold チェック
            # ------------------------------------------------
            #pre_sold_script = APPS_INV / "fetch_sold_ebay.py"
            #print("\n=== ⭐ 事前 sold チェック ===")
            #pre_start = datetime.now()
            #pre_code, _ = run_script(pre_sold_script)
            #pre_end = datetime.now()

            #send_script_mail(
            #    pre_sold_script,
            #    pre_start,
            #    pre_end,
            #    pre_code,
            #    round_no=set_no,
            #    conn=conn,
            #)

            time.sleep(WAIT_SECONDS)

            # ------------------------------------------------
            # ② フル在庫チェック（fetch_active だけ分散）
            # ------------------------------------------------
            print("\n=== 📦 フル在庫チェック（分散版）開始 ===")

            # ②-1 job投入
            active_start = datetime.now()
            active_code, _ = run_script(FETCH_ACTIVE)

            if active_code != 0:
                active_end = datetime.now()
                send_script_mail(
                    FETCH_ACTIVE,
                    active_start,
                    active_end,
                    active_code,
                    round_no=set_no,
                    conn=conn,
                )
                print("[STOP] fetch_active job投入失敗")
                continue

            # ②-2 worker 完了待ち（ここが本体）
            wait_until_no_pending(conn, phase_name="active")

            # ★ ここで active フェーズ完了
            active_end = datetime.now()
            send_script_mail(
                FETCH_ACTIVE,
                active_start,
                active_end,
                0,
                round_no=set_no,
                conn=conn,
            )

            # ②-3 sold（従来どおり）
            sold_start = datetime.now()
            sold_code, _ = run_script(FETCH_SOLD)
            sold_end = datetime.now()

            send_script_mail(
                FETCH_SOLD,
                sold_start,
                sold_end,
                sold_code,
                round_no=set_no,
                warn_continue=(sold_code != 0),
                conn=conn,
            )

            time.sleep(WAIT_SECONDS)

            # ②-4 remaining（従来どおり）
            rem_start = datetime.now()
            rem_code, rem_stdout = run_script(CHECK_REMAINING)
            rem_end = datetime.now()

            send_script_mail(
                CHECK_REMAINING,
                rem_start,
                rem_end,
                rem_code,
                round_no=set_no,
                conn=conn,
            )

            print(f"=== ✅ セット{set_no}: フル在庫チェック完了 ===")
            time.sleep(WAIT_SECONDS)

            # ------------------------------------------------
            # ③ delete_ebay_daily.py を 1 回実行
            # ------------------------------------------------
            print("\n=== 🗑 delete_ebay_daily.py 実行 ===")
            del_start = datetime.now()
            del_code, del_stdout = run_script(DELETE_SCRIPT)
            del_end = datetime.now()

            subject = (
                f"❌ delete_ebay_daily.py エラー（セット{set_no}）"
                if del_code != 0
                else f"✅ delete_ebay_daily.py 正常終了（セット{set_no}）"
            )

            body = (
                f"スクリプト: {DELETE_SCRIPT.name}\n"
                f"セット番号: {set_no}\n"
                f"開始時刻: {del_start}\n"
                f"終了時刻: {del_end}\n"
                f"処理時間: {del_end - del_start}\n"
                f"returncode: {del_code}\n\n"
                + format_trx_listings_count_by_account(conn)
            )

            send_mail(subject, body)
            time.sleep(WAIT_SECONDS)

            print("[STOP] delete までで処理終了（publish は意図的にスキップ）")
            return

            # ------------------------------------------------
            # ④ publish_ebay.py を 1 回実行
            # ------------------------------------------------
            print("\n=== 🚀 publish_ebay.py 実行 ===")
            pub_start = datetime.now()
            pub_code, _ = run_script(PUBLISH_SCRIPT)
            pub_end = datetime.now()

            subject = (
                f"❌ publish_ebay.py エラー（セット{set_no}）"
                if pub_code != 0
                else f"✅ publish_ebay.py 正常終了（セット{set_no}）"
            )

            body = (
                f"スクリプト: {PUBLISH_SCRIPT.name}\n"
                f"セット番号: {set_no}\n"
                f"開始時刻: {pub_start}\n"
                f"終了時刻: {pub_end}\n"
                f"処理時間: {pub_end - pub_start}\n"
                f"returncode: {pub_code}\n\n"
                + format_trx_listings_count_by_account(conn)
            )

            send_mail(subject, body)

            print(f"\n=== 🎊 セット {set_no} / {SET_N} 完了 ===")
            time.sleep(WAIT_SECONDS)

        print(f"\n=== 🎉 全セット完了（4工程×{SET_N}回転） ===")

    finally:
        try:
            conn.close()
        except Exception:
            pass



# ======================
# エントリポイント
# ======================
if __name__ == "__main__":
    main()
