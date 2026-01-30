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

SCRIPTS = [
    #APPS_INV / "fetch_active_ebay.py",
    APPS_INV / "fetch_sold_ebay.py",
    APPS_INV / "check_remaining_ebay.py",
]

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


# ======================
# メイン処理
# ======================
def main():
    conn = get_sql_server_connection()
    try:
        SET_N = 1  # ★この4つ（pre_sold→full1→delete→publish）を6回転
        print(f"=== 🧭 inventory_ebay_manager.py 開始（4工程×{SET_N}回転） ===")

        for set_no in range(1, SET_N + 1):
            print("\n\n==============================")
            print(f"🔁 セット {set_no} / {SET_N} 開始")
            print(f"   事前sold → フル在庫チェック1回転 → delete → publish")
            print("==============================")

            # ------------------------------------------------
            # ① 事前 sold チェック: fetch_sold_ebay.py を 1 回実行
            # ------------------------------------------------
            #pre_sold_script = APPS_INV / "fetch_sold_ebay.py"
            #print("\n=== ⭐ 事前 sold チェック: fetch_sold_ebay.py を実行します ===")
            #pre_start = datetime.now()
            #pre_code, pre_stdout = run_script(pre_sold_script)
            #pre_end = datetime.now()
            #send_script_mail(pre_sold_script, pre_start, pre_end, pre_code, round_no=set_no, conn=conn)

            #time.sleep(WAIT_SECONDS)

            # ------------------------------------------------
            # ② フル在庫チェック（1回転）: active → sold → remaining
            # ------------------------------------------------
            print("\n=== 📦 フル在庫チェック（1回転）開始 ===")
            for script in SCRIPTS:
                script_start = datetime.now()
                code, stdout = run_script(script)
                script_end = datetime.now()

                extra_body = ""

                # check_remaining_ebay.py のときだけ UNRESOLVED= をパース
                if script.name == "check_remaining_ebay.py" and stdout:
                    unresolved_count = None
                    for line in stdout.splitlines():
                        line = line.strip()
                        if line.startswith("UNRESOLVED="):
                            try:
                                unresolved_count = int(line.split("=", 1)[1])
                            except ValueError:
                                unresolved_count = None
                            break

                    if unresolved_count is not None:
                        extra_body += (
                            f"【check_remaining_ebay 結果】\n"
                            f"2回目リトライ後も判定不可のまま残っている件数: {unresolved_count} 件\n"
                        )

                # fetch_sold_ebay.py のエラーは「警告」で続行
                if script.name == "fetch_sold_ebay.py" and code != 0:
                    print(f"[WARN] {script.name} はエラー(code={code}) → 在庫処理は続行します")
                    send_script_mail(
                        script,
                        script_start,
                        script_end,
                        code,
                        round_no=set_no,
                        extra_body=extra_body,
                        warn_continue=True,
                        conn=conn,
                    )
                    time.sleep(WAIT_SECONDS)
                    continue

                # その他スクリプトのエラーは「このセットを中断」して次セットへ
                if code != 0:
                    send_script_mail(
                        script,
                        script_start,
                        script_end,
                        code,
                        round_no=set_no,
                        extra_body=extra_body,
                        warn_continue=False,
                        conn=conn,
                    )
                    print(f"[STOP] セット{set_no} は {script.name} のエラーで中断 → 次セットへ")
                    break

                # 正常終了時
                send_script_mail(
                    script,
                    script_start,
                    script_end,
                    code,
                    round_no=set_no,
                    extra_body=extra_body,
                    conn=conn,
                )

                time.sleep(WAIT_SECONDS)
            else:
                # for が break されず完走した場合のみ delete/publish へ進む
                print(f"=== ✅ セット{set_no}: フル在庫チェック1回転 完了 ===")

                # ------------------------------------------------
                # ③ delete_ebay_daily.py を 1 回実行
                # ------------------------------------------------
                print("\n=== 🗑 delete_ebay_daily.py を実行します ===")
                del_start = datetime.now()
                del_code, del_stdout = run_script(DELETE_SCRIPT)
                del_end = datetime.now()
                del_elapsed = del_end - del_start

                total_deleted = None
                if del_stdout:
                    for line in del_stdout.splitlines():
                        line = line.strip()
                        if line.startswith("✅ 全体合計:"):
                            m = re.search(r"全体合計:\s*(\d+)\s*件削除", line)
                            if m:
                                total_deleted = int(m.group(1))
                            break

                if del_code != 0:
                    subject = f"❌ delete_ebay_daily.py エラー発生（セット{set_no}）"
                    body = (
                        f"スクリプト: {DELETE_SCRIPT.name}\n"
                        f"セット番号: {set_no}\n"
                        f"開始時刻: {del_start}\n"
                        f"終了時刻: {del_end}\n"
                        f"処理時間: {del_elapsed}\n"
                        f"returncode: {del_code}\n"
                    )
                else:
                    subject = f"✅ delete_ebay_daily.py 正常終了（セット{set_no}）"
                    body = (
                        f"スクリプト: {DELETE_SCRIPT.name}\n"
                        f"セット番号: {set_no}\n"
                        f"開始時刻: {del_start}\n"
                        f"終了時刻: {del_end}\n"
                        f"処理時間: {del_elapsed}\n"
                    )
                    if total_deleted is not None:
                        body += f"\n全体で削除した件数: {total_deleted} 件"

                body += "\n\n" + format_trx_listings_count_by_account(conn)
                send_mail(subject, body)

                time.sleep(WAIT_SECONDS)

                # ------------------------------------------------
                # ④ publish_ebay.py を 1 回実行
                # ------------------------------------------------
                print("\n=== 🚀 publish_ebay.py を実行します ===")
                pub_start = datetime.now()
                pub_code, pub_stdout = run_script(PUBLISH_SCRIPT)
                pub_end = datetime.now()
                pub_elapsed = pub_end - pub_start

                if pub_code != 0:
                    subject = f"❌ publish_ebay.py エラー発生（セット{set_no}）"
                    body = (
                        f"スクリプト: {PUBLISH_SCRIPT.name}\n"
                        f"セット番号: {set_no}\n"
                        f"開始時刻: {pub_start}\n"
                        f"終了時刻: {pub_end}\n"
                        f"処理時間: {pub_elapsed}\n"
                        f"returncode: {pub_code}\n"
                    )
                else:
                    subject = f"✅ publish_ebay.py 正常終了（セット{set_no}）"
                    body = (
                        f"スクリプト: {PUBLISH_SCRIPT.name}\n"
                        f"セット番号: {set_no}\n"
                        f"開始時刻: {pub_start}\n"
                        f"終了時刻: {pub_end}\n"
                        f"処理時間: {pub_elapsed}\n"
                    )

                body += "\n\n" + format_trx_listings_count_by_account(conn)
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
