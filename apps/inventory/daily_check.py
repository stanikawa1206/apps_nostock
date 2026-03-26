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
            WHERE is_deleted = 0
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

def launch_remaining_workers():

    print("🟢 ローカル + VPS 複数で remaining worker 起動")

    # -----------------------------
    # ① ローカル側
    # -----------------------------
    local_cmd = (
        'start "LOCAL_CHECK" '
        '/d "D:\\apps_nostock\\apps\\inventory" '
        'check_remaining_ebay.bat'
    )
    subprocess.Popen(local_cmd, shell=True)

    # -----------------------------
    # ② VPS側（複数）
    # -----------------------------
    VPS_LIST = [
        "162.43.42.135",
        "162.43.15.160",
        "162.43.29.154",
        "210.131.209.103",
        "162.43.39.209",
        "85.131.251.127",
        "210.131.209.232",
    ]


    for ip in VPS_LIST:

        vps_cmd = (
            f'start "{ip}" '
            f'ssh -tt root@{ip} '
            '"cd /opt/apps_nostock && '
            'git pull && '
            'cd /opt/apps_nostock/apps/inventory && '
            'chmod +x check_remaining_ebay.sh && '
            './check_remaining_ebay.sh"'
        )

        subprocess.Popen(vps_cmd, shell=True)

    print("🚀 remaining worker を全VPSで起動しました")




def refresh_presets_materialized(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE mst.presets_materialized;")
        cur.execute("""
            INSERT INTO mst.presets_materialized
            SELECT *
            FROM mst.v_presets;
        """)
    conn.commit()

def refresh_presets_and_clear_locks(conn):
    """
    1. mst.v_presets (View) を物理テーブル mst.presets_lookup にコピー
    2. インデックスを再構築して高速化
    3. 前日の残りなどの processing_at / processing_by を一括クリア
    """
    print("--- [MAINTENANCE] Start Refreshing Presets and Clearing Locks ---")
    
    # 実行するSQLをリストにまとめる（トランザクション制御のため）
    queries = [
        # --- 1. 物理テーブルの再作成 ---
        "IF OBJECT_ID('mst.presets_lookup', 'U') IS NOT NULL DROP TABLE mst.presets_lookup;",
        "SELECT * INTO mst.presets_lookup FROM mst.v_presets;",
        
        # --- 2. インデックスの付与（検索高速化の要） ---
        "ALTER TABLE mst.presets_lookup ADD CONSTRAINT PK_presets_lookup PRIMARY KEY CLUSTERED (preset);",
        "CREATE INDEX IX_presets_lookup_cat_group ON mst.presets_lookup (category_group);",
        
        # --- 3. 幽霊ジョブ・ロックの掃除 ---
        # 全ての処理中フラグを落とし、新しい1日の実行に備える
        "UPDATE trx.vendor_item SET processing_by = NULL, processing_at = NULL WHERE processing_at IS NOT NULL;"
    ]

    try:
        # 自動コミットをオフにして一括処理
        conn.autocommit = False
        with conn.cursor() as cur:
            for sql in queries:
                print(f"  Executing: {sql[:50]}...") # ログ出力
                cur.execute(sql)
            
            conn.commit()
            print("--- [MAINTENANCE] Successfully updated presets and cleared all locks. ---")
            
    except Exception as e:
        conn.rollback()
        print(f"--- [ERROR] Maintenance failed. Rolled back. Detail: {e} ---")
        raise
    finally:
        conn.autocommit = True

def reset_remaining_flags(conn):
    """
    remaining フェーズ開始前に
    trx.vendor_item のチェック用フラグをすべてクリアする
    """
    print("🧹 remaining_check フラグ（by/lock/at）を全クリア")
    cur = conn.cursor()
    cur.execute("""
        UPDATE trx.vendor_item
           SET remaining_check_by   = NULL,
               remaining_check_lock = NULL,
               remaining_check_at   = NULL
    """)
    conn.commit()
    print("✅ remaining フラグ初期化完了")

def wait_until_remaining_exhausted(conn):
    """
    remaining 対象がなくなるまで待機
    ＋ 処理件数を返す
    """

    print("⏳ remaining 対象がなくなるのを待機中…")
    cur = conn.cursor()

    # ★ 初期件数
    cur.execute("""
        SELECT COUNT(*)
        FROM trx.vendor_item AS v
        INNER JOIN trx.listings AS l 
            ON v.vendor_name = l.vendor_name
           AND v.vendor_item_id = l.vendor_item_id
        WHERE l.is_deleted = 0
          AND v.vendor_name IN (N'メルカリ', N'メルカリshops')
          AND (v.status IS NULL OR LTRIM(RTRIM(v.status)) = N'')                               
          AND v.remaining_check_at IS NULL
          AND (
                v.remaining_check_lock IS NULL 
             OR v.remaining_check_lock < DATEADD(MINUTE, -15, SYSDATETIME())
          )
    """)
    initial_cnt = cur.fetchone()[0]

    while True:
        cur.execute("""
            SELECT COUNT(*)
            FROM trx.vendor_item AS v
            INNER JOIN trx.listings AS l 
                ON v.vendor_name = l.vendor_name
               AND v.vendor_item_id = l.vendor_item_id
            WHERE l.is_deleted = 0
              AND v.vendor_name IN (N'メルカリ', N'メルカリshops')
              AND (v.status IS NULL OR LTRIM(RTRIM(v.status)) = N'')                               
              AND v.remaining_check_at IS NULL
              AND (
                    v.remaining_check_lock IS NULL 
                 OR v.remaining_check_lock < DATEADD(MINUTE, -15, SYSDATETIME())
              )
        """)
        cnt = cur.fetchone()[0]

        if cnt == 0:
            print("✅ remaining 対象消滅")
            break

        print(f"… 待機中: 残り約 {cnt} 件")
        time.sleep(30)

    # ★ 処理件数 = 初期件数
    return initial_cnt
def refresh_presets_lookup(conn):
    cursor = conn.cursor()

    print("presets_lookup refresh start")

    cursor.execute("""
    TRUNCATE TABLE mst.presets_lookup;

    INSERT INTO mst.presets_lookup
    SELECT *
    FROM mst.v_presets;
    """)

    conn.commit()

# ======================
# メイン処理
# ======================
def main():
    conn = get_sql_server_connection()

    # presets一覧を準備
    refresh_presets_lookup(conn)
    
    try:
        SET_N = 3
        print(f"=== 🧭 inventory_ebay_manager.py 開始（4工程×{SET_N}回転） ===")

        for set_no in range(1, SET_N + 1):
            print("\n\n==============================")
            print(f"🔁 セット {set_no} / {SET_N} 開始")
            print("   事前sold → フル在庫チェック → delete → publish")
            print("==============================")


            # ------------------------------------------------
            # ① 事前 sold チェック（分散版）
            # ------------------------------------------------
            print("\n=== ⭐ 事前 sold チェック（分散版）開始 ===")

            pre_sold_start = datetime.now()
            pre_sold_code, _ = run_script(FETCH_SOLD)  # job投入

            if pre_sold_code != 0:
                pre_sold_end = datetime.now()
                send_script_mail(
                    FETCH_SOLD,
                    pre_sold_start,
                    pre_sold_end,
                    pre_sold_code,
                    round_no=set_no,
                    conn=conn,
                )
                print("[STOP] 事前 fetch_sold job投入失敗")
                continue

            # worker完了待ち
            wait_until_no_pending(conn, phase_name="pre_sold")

            pre_sold_end = datetime.now()

            send_script_mail(
                FETCH_SOLD,
                pre_sold_start,
                pre_sold_end,
                0,
                round_no=set_no,
                conn=conn,
            )

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

            # ------------------------------------------------
            # ②-3 sold（分散版：job投入 → worker完了待ち）
            # ------------------------------------------------
            print("\n=== 🧾 sold チェック（分散版）開始 ===")

            # ②-3-1 job投入
            sold_start = datetime.now()
            sold_code, _ = run_script(FETCH_SOLD)

            if sold_code != 0:
                sold_end = datetime.now()
                send_script_mail(
                    FETCH_SOLD,
                    sold_start,
                    sold_end,
                    sold_code,
                    round_no=set_no,
                    conn=conn,
                )
                print("[STOP] fetch_sold job投入失敗")
                continue

            # ②-3-2 worker 完了待ち（ここが本体）
            wait_until_no_pending(conn, phase_name="sold")

            # ★ ここで sold フェーズ完了
            sold_end = datetime.now()
            send_script_mail(
                FETCH_SOLD,
                sold_start,
                sold_end,
                0,
                round_no=set_no,
                conn=conn,
            )


            time.sleep(WAIT_SECONDS)

            # ------------------------------------------------
            # ②-4 remaining（ローカル＋VPS分散版）
            # ------------------------------------------------
            print("\n=== 🔄 remaining チェック開始 ===")

            rem_start = datetime.now()

            # ★ 1. フラグ初期化
            reset_remaining_flags(conn)

            # ★ 2. worker起動
            launch_remaining_workers()

            # ★ 3. remaining対象が枯渇するまで待つ
            processed_count = wait_until_remaining_exhausted(conn)

            rem_end = datetime.now()

            extra = f"remaining 処理件数: {processed_count} 件"

            send_script_mail(
                CHECK_REMAINING,
                rem_start,
                rem_end,
                0,
                round_no=set_no,
                conn=conn,
                extra_body=extra,   # ←ここ
            )

            print(f"=== ✅ セット{set_no}: フル在庫チェック完了 ===")
            time.sleep(WAIT_SECONDS)

            continue

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

            print("BASE_DIR =", BASE_DIR)
            print("DELETE_SCRIPT =", DELETE_SCRIPT)
            print("cwd =", str(BASE_DIR))

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
            

            # ------------------------------------------------
            # ④ publish_ebay.py を 1 回実行
            # ------------------------------------------------
            print("\n=== 🚀 publish_ebay.py 実行 ===")

            refresh_presets_and_clear_locks(conn)
            return

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
