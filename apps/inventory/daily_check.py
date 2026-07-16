# inventory_ebay_manager.py
# -*- coding: utf-8 -*-
import sys
import subprocess
import time
from pathlib import Path
from datetime import datetime, timedelta
from datetime import time as clock_time
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

FETCH_ACTIVE = APPS_INV / "fetch_active_ebay.py"
FETCH_SOLD       = APPS_INV / "fetch_sold_ebay.py"
CHECK_REMAINING  = APPS_INV / "check_remaining_ebay.py"

# 出品関連（Active Listing取得・LIMIT監視・出品）は publish_manager.py に一本化。
# daily_check は「1日の流れ全体」を管理するオーケストレーターとして、
# publish_manager を呼び出すだけにする。
PUBLISH_MANAGER_SCRIPT = APPS_PUB / "publish_manager.py"

WAIT_SECONDS = 3

# ======================
# 運用方針: 「在庫管理 → 出品」を1サイクルとし、これを複数回実行できるようにする
# ======================

# 1日に回すサイクル数。
# 通常運用は 1。長期不在時などは 2 や 7 のように変更するだけでよい。
TOTAL_CYCLES = 1

# 出品開始予定時刻（時刻のみ）。
# ★ 実際の運用時刻に合わせて必ず設定してください（下記は仮の値）。
PUBLISH_START_US = clock_time(21, 0)

# 在庫管理1回に見込む所要時間。
# ★ 実績に応じて調整してください（下記は仮の値）。
INVENTORY_DURATION = timedelta(hours=2)

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

def refresh_presets_and_clear_locks(conn):
    """
    前日の残りなどの processing_at / processing_by を一括クリア
    """
    print("--- [MAINTENANCE] Start Clearing Locks ---")

    sql = "UPDATE trx.vendor_item SET processing_by = NULL, processing_at = NULL WHERE processing_at IS NOT NULL;"

    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            print(f"  Executing: {sql[:50]}...")
            cur.execute(sql)

            conn.commit()
            print("--- [MAINTENANCE] Successfully cleared all locks. ---")

    except Exception as e:
        conn.rollback()
        print(f"--- [ERROR] Lock clear failed. Rolled back. Detail: {e} ---")
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
        WHERE remaining_check_by IS NOT NULL
            OR remaining_check_lock IS NOT NULL
            OR remaining_check_at IS NOT NULL
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
    cur = conn.cursor()
    print("presets_lookup refresh start")

    cur.execute("TRUNCATE TABLE mst.presets_lookup;")

    vendors = cur.execute("""
        SELECT vendor_code, vendor_name
        FROM mst.vendors
    """).fetchall()

    brands = cur.execute("""
        SELECT brand_id, brand_name_ja, default_brand_en, is_active, is_listing_target
        FROM mst.presets_brand
        WHERE is_active = 1
    """).fetchall()

    categories = cur.execute("""
        SELECT category_id, category_name_ja, type_ebay, category_id_ebay, department, brand_id
        FROM mst.presets_categories
    """).fetchall()

    category_groups = cur.execute("""
        SELECT category_group, mode, low_usd_target, high_usd_target,
               low_jpy_target, high_jpy_target
        FROM mst.category_groups
    """).fetchall()

    cgc_rows = cur.execute("""
        SELECT category_group, category_id, is_brand_dependent
        FROM mst.category_group_categories
    """).fetchall()

    bcg_rows = cur.execute("""
        SELECT brand_id, category_group
        FROM mst.brand_category_groups
    """).fetchall()

    brand_by_id = {r.brand_id: r for r in brands}
    category_by_id = {r.category_id: r for r in categories}
    group_by_name = {r.category_group: r for r in category_groups}

    cgc_by_group = {}
    for r in cgc_rows:
        cgc_by_group.setdefault(r.category_group, []).append(r)

    cgc_by_category_id = {}
    for r in cgc_rows:
        cgc_by_category_id.setdefault(r.category_id, []).append(r)

    bcg_groups = {r.category_group for r in bcg_rows}

    rows = []

    # 1) brand_category_groupsあり、is_brand_dependent=0
    for bcg in bcg_rows:
        brand = brand_by_id.get(bcg.brand_id)
        if not brand:
            continue

        for cgc in cgc_by_group.get(bcg.category_group, []):
            if cgc.is_brand_dependent != 0:
                continue

            pc = category_by_id.get(cgc.category_id)
            cg = group_by_name.get(cgc.category_group)
            if not pc or not cg:
                continue

            for v in vendors:
                preset = (
                    (brand.brand_name_ja or "")
                    + (pc.category_name_ja or "")
                    + ("men" if pc.department == "Men" else "")
                    + (v.vendor_code or "")
                )

                rows.append((
                    preset,
                    v.vendor_name,
                    brand.brand_id,
                    pc.category_id,
                    cg.mode,
                    cg.low_usd_target,
                    cg.high_usd_target,
                    pc.category_id_ebay,
                    pc.department,
                    brand.default_brand_en,
                    pc.type_ebay,
                    cg.low_jpy_target,
                    cg.high_jpy_target,
                    cg.category_group,
                    brand.is_active,
                    brand.is_listing_target,
                ))

    # 2) brand_category_groupsに無いcategory_group、is_brand_dependent=0
    for cgc in cgc_rows:
        if cgc.is_brand_dependent != 0:
            continue

        if cgc.category_group in bcg_groups:
            continue

        pc = category_by_id.get(cgc.category_id)
        cg = group_by_name.get(cgc.category_group)
        if not pc or not cg:
            continue

        for v in vendors:
            preset = (
                (pc.category_name_ja or "")
                + ("men" if pc.department == "Men" else "")
                + (v.vendor_code or "")
            )

            rows.append((
                preset,
                v.vendor_name,
                pc.brand_id,
                pc.category_id,
                cg.mode,
                cg.low_usd_target,
                cg.high_usd_target,
                pc.category_id_ebay,
                pc.department,
                "",
                pc.type_ebay,
                cg.low_jpy_target,
                cg.high_jpy_target,
                cg.category_group,
                1,
                1,
            ))

    # 3) is_brand_dependent=1
    for pc in categories:
        brand = brand_by_id.get(pc.brand_id)
        if not brand:
            continue

        for cgc in cgc_by_category_id.get(pc.category_id, []):
            if cgc.is_brand_dependent != 1:
                continue

            cg = group_by_name.get(cgc.category_group)
            if not cg:
                continue

            for v in vendors:
                preset = (
                    (brand.brand_name_ja or "")
                    + (pc.category_name_ja or "")
                    + ("men" if pc.department == "Men" else "")
                    + (v.vendor_code or "")
                )

                rows.append((
                    preset,
                    v.vendor_name,
                    brand.brand_id,
                    pc.category_id,
                    cg.mode,
                    cg.low_usd_target,
                    cg.high_usd_target,
                    pc.category_id_ebay,
                    pc.department,
                    brand.default_brand_en,
                    pc.type_ebay,
                    cg.low_jpy_target,
                    cg.high_jpy_target,
                    cg.category_group,
                    brand.is_active,
                    brand.is_listing_target,
                ))

    cur.fast_executemany = True
    cur.executemany("""
        INSERT INTO mst.presets_lookup (
            preset,
            vendor_name,
            brand_id,
            category_id,
            mode,
            low_usd_target,
            high_usd_target,
            category_id_ebay,
            department,
            default_brand_en,
            type_ebay,
            low_jpy_target,
            high_jpy_target,
            category_group,
            is_active,
            is_listing_target
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    print(f"presets_lookup refresh done: {len(rows):,} rows")

    cur.execute("SELECT COUNT(*) FROM mst.presets_lookup")
    count = cur.fetchone()[0]
    print(f"presets_lookup count: {count:,}")


# ======================
# 在庫管理（1回分）
# ======================
def run_inventory_management_round(conn, cycle_no, round_no):
    """
    在庫管理を1回分実行する（事前sold → フル在庫チェック → sold → remaining）。

    いずれかの工程でジョブ投入自体に失敗した場合は、その工程以降を打ち切って
    この1回分の在庫管理を終える（プロセス全体は落とさない）。
    """

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
            round_no=round_no,
            conn=conn,
        )
        print("[STOP] 事前 fetch_sold job投入失敗")
        return

    # worker完了待ち
    wait_until_no_pending(conn, phase_name="pre_sold")

    pre_sold_end = datetime.now()

    send_script_mail(
        FETCH_SOLD,
        pre_sold_start,
        pre_sold_end,
        0,
        round_no=round_no,
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
            round_no=round_no,
            conn=conn,
        )
        print("[STOP] fetch_active job投入失敗")
        return

    # ②-2 worker 完了待ち（ここが本体）
    wait_until_no_pending(conn, phase_name="active")

    # ★ ここで active フェーズ完了
    active_end = datetime.now()
    send_script_mail(
        FETCH_ACTIVE,
        active_start,
        active_end,
        0,
        round_no=round_no,
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
            round_no=round_no,
            conn=conn,
        )
        print("[STOP] fetch_sold job投入失敗")
        return

    # ②-3-2 worker 完了待ち（ここが本体）
    wait_until_no_pending(conn, phase_name="sold")

    # ★ ここで sold フェーズ完了
    sold_end = datetime.now()
    send_script_mail(
        FETCH_SOLD,
        sold_start,
        sold_end,
        0,
        round_no=round_no,
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
        round_no=round_no,
        conn=conn,
        extra_body=extra,
    )

    print(f"=== ✅ Cycle{cycle_no} 在庫管理{round_no}回目: 完了 ===")
    time.sleep(WAIT_SECONDS)


# ======================
# 出品開始タイミング判定
# ======================
def get_next_publish_start_datetime(now: datetime) -> datetime:
    """
    PUBLISH_START_US（時刻のみ）を、nowから見て次に到来する日時に変換する。
    今日のPUBLISH_START_USをまだ過ぎていなければ今日、既に過ぎていれば翌日とする
    （在庫管理が日をまたぐ場合を考慮）。
    """
    candidate = datetime.combine(now.date(), PUBLISH_START_US)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def can_start_another_inventory_round(now: datetime) -> bool:
    """
    「今から在庫管理をもう1回開始した場合の終了予定時刻」が
    「出品開始予定時刻 - INVENTORY_DURATION」を超えないかどうかを判定する。

    超えない  → まだ余裕がある  → 在庫管理をもう1回実行してよい
    超える    → 余裕がない      → 在庫管理は開始せず、publish_manager を実行する
    """
    publish_start = get_next_publish_start_datetime(now)
    deadline = publish_start - INVENTORY_DURATION
    projected_finish = now + INVENTORY_DURATION

    return projected_finish <= deadline


# ======================
# 出品処理（publish_manager 呼び出し）
# ======================
def run_publish_manager(conn):
    """
    publish_manager.py を起動し、終了するまで待つ。

    publish_manager は Active Listing取得・LIMIT監視・出品といった
    「出品関連」だけを担当する（daily_check側では中身に立ち入らない）。
    """

    print("🚀 publish_manager開始")

    # publish直前に、前回までの処理ロックをクリアしておく
    refresh_presets_and_clear_locks(conn)

    pub_start = datetime.now()
    pub_code, _ = run_script(PUBLISH_MANAGER_SCRIPT)
    pub_end = datetime.now()

    subject = (
        f"❌ publish_manager.py エラー"
        if pub_code != 0
        else f"✅ publish_manager.py 正常終了"
    )

    body = (
        f"スクリプト: {PUBLISH_MANAGER_SCRIPT.name}\n"
        f"開始時刻: {pub_start}\n"
        f"終了時刻: {pub_end}\n"
        f"処理時間: {pub_end - pub_start}\n"
        f"returncode: {pub_code}\n"
    )

    send_mail(subject, body)

    print("🚀 publish_manager終了")

    return pub_code


def post_publish_check():
    """
    出品後チェック。

    # TODO 出品後チェック
    将来的には以下をここへ追加する:
      ・出品件数確認
      ・LIMIT確認
      ・異常確認
    """
    print("📋 出品後チェック（TODO: 未実装）")


# ======================
# 1サイクル
# ======================
def run_one_cycle(cycle_no: int, conn):
    """
    「在庫管理 → 出品」を1サイクルとして実行する。

    在庫管理は、次の1回を開始しても出品開始予定時刻に間に合う間は
    繰り返し実行し、間に合わなくなった時点で打ち切って publish_manager へ進む。
    """

    print("\n=========================")
    print(f"Cycle {cycle_no} / {TOTAL_CYCLES}")
    print("=========================")

    round_no = 1
    while True:
        # 在庫管理開始
        print(f"\n📦 在庫管理開始（Cycle {cycle_no} - {round_no}回目）")
        run_inventory_management_round(conn, cycle_no, round_no)
        print(f"📦 在庫管理終了（Cycle {cycle_no} - {round_no}回目）")

        now = datetime.now()

        # 次の在庫管理を開始しても、出品開始予定時刻に間に合うか判定する
        if can_start_another_inventory_round(now):
            print("⏱ まだ余裕があるため、在庫管理をもう1回実行します")
            round_no += 1
            continue

        print("⏱ 次の在庫管理は出品開始予定時刻に間に合わないため、出品処理へ進みます")
        break

    # 出品処理を開始する（publish_managerが終了するまで待つ）
    run_publish_manager(conn)

    # 出品後チェック
    post_publish_check()

    print(f"\n✅ Cycle {cycle_no} / {TOTAL_CYCLES} 終了")


# ======================
# メイン処理
# ======================
def main():
    os.system("find /tmp -mindepth 1 -delete")
    conn = get_sql_server_connection()

    # presets一覧を準備
    refresh_presets_lookup(conn)

    try:
        print(f"=== 🧭 daily_check.py 開始（{TOTAL_CYCLES}サイクル） ===")

        for cycle_no in range(1, TOTAL_CYCLES + 1):
            run_one_cycle(cycle_no, conn)

        print(f"\n=== 🎉 全サイクル完了（{TOTAL_CYCLES}サイクル） ===")

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
