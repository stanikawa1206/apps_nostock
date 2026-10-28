# D:\apps_nostock\apps\inventory\watchdog.py

import os
import sys
import time
import glob
import socket
import logging
import subprocess
from datetime import datetime, timezone, timedelta
from apps.common.utils import get_sql_server_connection
import platform
from pathlib import Path
import psutil

# =========================
# 設定
# =========================

# worker_status_*.txt を置く場所（＝監視対象）
# Windowsなら D:/apps_nostock
# Linuxなら /opt/apps_nostock を環境変数で上書き可能
if os.name == "nt":
    default_dir = Path("D:/apps_nostock/logs")
else:
    default_dir = Path("/opt/apps_nostock")

BASE_DIR = os.environ.get("WORKER_STATUS_DIR", default_dir)

# 「この秒数更新がなければ死んだ」とみなす
# → workerが止まってもすぐ再起動しないための猶予
TIMEOUT_SEC = 600  # 10分

# 常に維持したいworker数
TARGET_WORKERS = 2

# ログフォルダ（watchdog自体のprint用）
LOG_DIR = BASE_DIR
os.makedirs(LOG_DIR, exist_ok=True)

# =========================
# 取り残し running ジョブの回収設定
# =========================
# ワーカーが statusファイルを消した後に異常終了すると（release_job()が08S01で失敗した等）、
# ジョブが running のまま statusファイル監視からも外れて取り残される（2026-10-06〜10-16 に5件）。
# DB の running ジョブを起点に、「担当していた可能性のあるワーカーが全員、終了済みか
# 別ジョブへ進んだことを確認できたジョブ」だけを pending へ戻す。
ORPHAN_CHECK_INTERVAL_SEC = 30
ORPHAN_GRACE_SEC = 300     # locked_at からこの秒数未満のジョブは判定しない（取得直後の猶予）
ORPHAN_CONFIRM_SEC = 60    # 1回目の検知からこの秒数以上あけた再確認でも同じ判定なら回収する
CLOCK_MARGIN_SEC = 2       # locked_at は datetime2(0)（秒丸め）なので時刻比較に持たせる余裕
KILL_WAIT_SEC = 10         # kill 後、プロセス終了を待つ秒数

# =========================
# ブラウザ応答待ちの見張り
# =========================
# メルカリ検索の resp.json() などブラウザへの呼び出しが戻らず、ワーカーが止まることがある
# （2026-10-20〜28 に各VPSで1日3〜5回。呼び出しに時間制限がなく、ワーカー自身は時間切れを判定できない）。
# ワーカーは1回の取得の間だけ worker_browserwait_<pid>.txt を置く。開始からこの秒数経っても消えなければ、
# そのワーカーが起動したブラウザ（子孫の chrome プロセス）だけを終了させる。止まっていた呼び出しは
# エラーで戻り、ワーカーの BROWSER_BROKEN（ブラウザ再作成・ジョブを pending へ戻す）に進む。
# 1回の取得の正常な最長は約81秒（応答待ち30秒＋本文の受信完了待ち30秒＋json 実測最大21秒）。
# それでも戻らない場合は、従来どおり statusファイル更新停止（TIMEOUT_SEC）でワーカーごと終了させる。
BROWSER_HANG_SEC = 90
DB_LOCK_TIMEOUT_MS = 10000 # 死んだワーカーの未確定トランザクションがロックを持っていても無期限に待たない

JST = timezone(timedelta(hours=9))


def get_worker_name() -> str:
    # scrape_worker.get_worker_name() と同じ規則（scrape_worker は import 時に
    # stdout差し替え・ログ削除などの副作用があるため import せず同じ処理を持つ）。
    # watchdog が起動するワーカーは環境変数を引き継ぐので、同じ値になる。
    name = os.environ.get("WORKER_NAME")
    if name:
        return name
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-worker"


HOST_WORKER_NAME = get_worker_name()

# 回収の判定・結果は tty だけでなくファイルにも残す
RECOVERY_LOG_PATH = os.path.join(BASE_DIR, "logs", "watchdog_recovery.log")
os.makedirs(os.path.dirname(RECOVERY_LOG_PATH), exist_ok=True)
_recovery_logger = logging.getLogger("watchdog_recovery")
if not _recovery_logger.handlers:
    _recovery_logger.setLevel(logging.INFO)
    _h = logging.FileHandler(RECOVERY_LOG_PATH, encoding="utf-8")
    _h.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _recovery_logger.addHandler(_h)
    _recovery_logger.propagate = False


def rlog(msg: str):
    print(msg, flush=True)
    _recovery_logger.info(f"[{HOST_WORKER_NAME}] {msg}")


# =========================
# worker起動管理
# =========================
def is_scrape_worker_cmdline(cmdline) -> bool:
    # 完全一致判定
    return bool(
        cmdline and
        len(cmdline) >= 3 and
        cmdline[-2] == "-m" and
        cmdline[-1] == "apps.inventory.scrape_worker"
    )


def count_process():
    cnt = 0
    for p in psutil.process_iter(['cmdline']):
        try:
            if is_scrape_worker_cmdline(p.info['cmdline']):
                cnt += 1
        except:
            pass

    return cnt

def start_worker():
    log_path = os.path.join(LOG_DIR, f"worker_{int(time.time())}.log")
    f = open(log_path, "a", encoding="utf-8")

    subprocess.Popen(
        [sys.executable, "-u", "-m", "apps.inventory.scrape_worker"],
        stdout=f,
        stderr=f
    )

    print("[SPAWN] worker started", flush=True)


# =========================
# ワーカー・statusファイル・ジョブ所有の判定
# =========================
def list_live_workers() -> dict:
    """このホストで生きている scrape_worker の {pid: create_time}。ゾンビは終了済みとして除く。"""
    live = {}
    for p in psutil.process_iter(['pid', 'cmdline', 'status', 'create_time']):
        try:
            if p.info['status'] == psutil.STATUS_ZOMBIE:
                continue
            if is_scrape_worker_cmdline(p.info['cmdline']) and p.info['create_time'] is not None:
                live[p.info['pid']] = p.info['create_time']
        except Exception:
            pass
    return live


def get_live_worker_process(pid: int):
    """pid が生きている（ゾンビでない）scrape_worker なら psutil.Process、それ以外は None"""
    try:
        proc = psutil.Process(pid)
        if proc.status() == psutil.STATUS_ZOMBIE:
            return None
        if not is_scrape_worker_cmdline(proc.cmdline()):
            return None
        return proc
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return None


def parse_status_file(path):
    """statusファイルを読む。読めない・書き込み途中で pid がない場合は None"""
    try:
        mtime = os.path.getmtime(path)
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        return None

    data = {}
    for line in lines:
        if "=" in line:
            k, v = line.split("=", 1)
            data[k.strip()] = v.strip()

    try:
        pid = int(data["pid"])
    except (KeyError, ValueError):
        return None

    def _int(key):
        try:
            return int(data[key])
        except (KeyError, ValueError):
            return None

    return {"path": path, "pid": pid, "job_id": _int("job_id"), "page": _int("page"), "mtime": mtime}


def parse_browser_wait_file(path):
    """worker_browserwait_<pid>.txt を読む。読めない・書き込み途中なら None"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        return None
    data = dict(line.split("=", 1) for line in lines if "=" in line)
    try:
        return {"path": path, "pid": int(data["pid"]), "job_id": data.get("job_id"), "started": float(data["started"])}
    except (KeyError, ValueError):
        return None


def worker_browser_processes(proc):
    """ワーカー(proc)が起動したブラウザのプロセス（子孫のうち chrome のもの）。Playwright の driver(node) は含めない"""
    found = []
    for c in proc.children(recursive=True):
        try:
            if "chrome" in c.name().lower():
                found.append(c)
        except psutil.Error:
            pass
    return found


def read_status_files() -> dict:
    """{pid: statusファイル情報}"""
    files = {}
    for path in glob.glob(os.path.join(BASE_DIR, "worker_status_*.txt")):
        info = parse_status_file(path)
        if info is not None:
            files[info["pid"]] = info
    return files


def locked_at_epoch(locked_at: datetime) -> float:
    # locked_at はワーカーが同じホストの時計で now_jst() を入れた値
    return locked_at.replace(tzinfo=JST).timestamp()


def worker_owner_state(pid, create_time, status_file, job_id, t_lock) -> str:
    """
    生きているワーカー(pid)が、t_lock にロックされたジョブ job_id を担当している可能性。
      "owner"     : statusファイルがこのジョブを指している
      "not_owner" : 担当していないことが確定（ロックより後に起動した / ロックより後に別ジョブを書いた）
      "unknown"   : どちらとも言えない（statusファイルなし等）→ 回収しない

    scrape_worker は1回に1ジョブずつ処理し（N=1）、各ページ処理開始時に statusファイルを
    上書きする。そのため、ロック時刻より後に別ジョブで書かれたファイルは、そのワーカーが
    このジョブの処理を抜けて次へ進んだことを意味する。また running のジョブは誰も取得
    しないので、ロック時刻より後に起動したワーカーがこのジョブを持つことはない。
    """
    if create_time > t_lock + CLOCK_MARGIN_SEC:
        return "not_owner"

    # 同じ pid の別（過去）プロセスが残したファイルは、このプロセスのものとして扱わない
    if status_file is None or status_file["mtime"] < create_time - 1:
        return "unknown"

    if status_file["job_id"] == job_id:
        return "owner"

    if status_file["mtime"] > t_lock + CLOCK_MARGIN_SEC:
        return "not_owner"

    return "unknown"


def judge_requeue(job_id, t_lock, live, files):
    """
    生きている全ワーカーが「担当していない」と確定した場合だけ再投入可。
    戻り値: (再投入可否, 状態の内訳 {pid: state})
    """
    states = {
        pid: worker_owner_state(pid, ct, files.get(pid), job_id, t_lock)
        for pid, ct in live.items()
    }
    return all(s == "not_owner" for s in states.values()), states


def format_states(states) -> str:
    return ", ".join(f"pid{pid}={s}" for pid, s in sorted(states.items())) or "生存ワーカーなし"


# =========================
# DB
# =========================
def open_db():
    conn = get_sql_server_connection()
    cur = conn.cursor()
    cur.execute(f"SET LOCK_TIMEOUT {DB_LOCK_TIMEOUT_MS}")
    cur.close()
    return conn


SQL_REQUEUE = """
UPDATE trx.scrape_job
SET status = 'pending',
    worker_name = NULL,
    locked_at = NULL,
    finished_at = NULL,
    error_message = NULL{set_page}
WHERE job_id = ?
  AND status = 'running'
  AND worker_name = ?
  AND locked_at = ?
"""


def requeue_job(conn, job_id, locked_at, page_hint) -> int:
    """
    判定時に見た状態（running・このホスト・同じ locked_at）のままの場合だけ pending へ戻す。
    別のwatchdogが回収済み・別ワーカーが再取得済みなら 0 件更新になり何も変えない。
    戻り値: 更新件数
    """
    cur = conn.cursor()
    try:
        if page_hint is None:
            cur.execute(SQL_REQUEUE.format(set_page=""), job_id, HOST_WORKER_NAME, locked_at)
        else:
            cur.execute(
                SQL_REQUEUE.format(set_page=",\n    current_page = ?"),
                page_hint, job_id, HOST_WORKER_NAME, locked_at,
            )
        n = cur.rowcount
        conn.commit()
        return n
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass


def page_hint_for(job_id, t_lock, files, live):
    """終了済みワーカーの statusファイルがこのロック中のジョブを指していれば、その page を再開位置に使う"""
    for pid, f in files.items():
        if pid in live and f["mtime"] >= live[pid] - 1:
            continue  # 生きているワーカー自身のファイル
        if f["job_id"] == job_id and f["page"] is not None and f["mtime"] + CLOCK_MARGIN_SEC >= t_lock:
            return f["page"]
    return None


def kill_and_confirm(proc) -> bool:
    """kill してプロセス終了（ゾンビ化含む）を確認できたら True"""
    try:
        proc.kill()
    except psutil.NoSuchProcess:
        return True
    except Exception as e:
        rlog(f"[KILL][ERROR] pid={proc.pid} kill失敗: {e}")
        return False

    try:
        proc.wait(timeout=KILL_WAIT_SEC)
        return True
    except psutil.TimeoutExpired:
        try:
            return proc.status() == psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return True
    except psutil.NoSuchProcess:
        return True


def _remove(path):
    try:
        os.remove(path)
    except OSError:
        pass


# =========================
# 停止workerの検知と回収
# =========================
def process_file(path):
    """
    1つの worker_status ファイルを処理

    やっていること：
    1. 最終更新時間を確認
    2. TIMEOUT超えてたら「止まった」と判断
    3. ファイルを書いたプロセスがまだ生きていれば kill し、終了を確認する
       （終了を確認できなければ、ジョブは戻さず次回再確認）
    4. 担当していた可能性のあるワーカーが全員いなくなったことを確認できた場合、または
       終了したこのファイルの書き手が現在のロックの担当だったと確定できた場合（owner_proved_by_file）だけ、
       DBの状態（running・このホスト・同じlocked_at）を条件に pending に戻す
    5. statusファイル削除
    """
    print(f"[CHECK FILE] {path}", flush=True)

    try:
        info = parse_status_file(path)
        if info is None:
            return

        # まだ生きてると判断（更新されてる）
        if time.time() - info["mtime"] < TIMEOUT_SEC:
            return

        pid, job_id, page = info["pid"], info["job_id"], info["page"]
        rlog(f"[STALE] pid={pid} job={job_id} page={page} statusファイル更新停止 "
             f"{int(time.time() - info['mtime'])}秒")

        # =========================
        # 担当プロセスの停止確認（ジョブを戻す前に必ず行う）
        # =========================
        proc = get_live_worker_process(pid)
        if proc is not None:
            if proc.create_time() <= info["mtime"] + 1:
                if not kill_and_confirm(proc):
                    rlog(f"[STALE][HOLD] pid={pid} job={job_id} 終了を確認できないため再投入しない（次回再確認）")
                    return
                rlog(f"[STALE][KILL] pid={pid} 終了確認")
            else:
                # 同じ pid で後から起動した別ワーカー。ファイルの書き手はもういない
                rlog(f"[STALE] pid={pid} はファイルより後に起動した別プロセスのため kill しない")

        if job_id is None:
            _remove(path)
            rlog(f"[CLEAN] {path}")
            return

        # このファイルの書き手が終了済みか（kill 確認済み・既に終了・同じ pid の別プロセス）
        writer = get_live_worker_process(pid)
        writer_gone = writer is None or writer.create_time() > info["mtime"] + 1

        # =========================
        # DB更新（jobをやり直し状態に戻す）
        # =========================
        live = list_live_workers()
        files = read_status_files()
        conn = open_db()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT status, worker_name, locked_at FROM trx.scrape_job WHERE job_id = ?",
                job_id,
            )
            row = cur.fetchone()
            cur.close()
            conn.commit()

            if row is None or row[0] != "running" or row[1] != HOST_WORKER_NAME or row[2] is None:
                rlog(f"[STALE] job={job_id} は再投入不要（status={row[0] if row else None} "
                     f"worker={row[1] if row else None}）→ statusファイルのみ削除")
                _remove(path)
                return

            locked_at = row[2]
            t_lock = locked_at_epoch(locked_at)
            ok, states = judge_requeue(job_id, t_lock, live, files)
            reason = "statusファイル更新停止・担当プロセス終了確認"
            if not ok and owner_proved_by_file(info, t_lock, writer_gone, states):
                # 他の生存ワーカーに担当不明（unknown）がいても、このファイルの書き手が現在のロックの担当だったと
                # 確定でき、その書き手は終了済み。running のジョブは他のワーカーに取られないので戻してよい。
                ok = True
                reason = "終了したワーカーのstatusファイルが現在のロックの担当を記録"
            if not ok:
                if "owner" in states.values():
                    rlog(f"[STALE] job={job_id} は別の生存ワーカーが担当中（{format_states(states)}）→ statusファイルのみ削除")
                    _remove(path)
                else:
                    rlog(f"[STALE][HOLD] job={job_id} 担当していない確認が取れないワーカーあり"
                         f"（{format_states(states)}）→ 次回再確認")
                return

            # このファイルの書き手の試行が現在のロックなら、そのページから再開させる
            page_hint = page if (page is not None and info["mtime"] + CLOCK_MARGIN_SEC >= t_lock) else None
            n = requeue_job(conn, job_id, locked_at, page_hint)
        finally:
            conn.close()

        if n == 1:
            rlog(f"[RECOVER][OK] job={job_id} worker={HOST_WORKER_NAME} pid={pid} "
                 f"理由={reason}（{format_states(states)}）→ pending (current_page={page_hint})")
        else:
            rlog(f"[RECOVER][SKIP] job={job_id} 判定後に状態が変わっていたため更新せず")
        _remove(path)
        rlog(f"[CLEAN] {path}")

    except Exception as e:
        # ファイルは残し、次回ループで再確認する
        rlog(f"[ERROR] {path}: {e}")


def owner_proved_by_file(info, t_lock, writer_gone, states) -> bool:
    """
    statusファイルの書き手が、DB の現在のロック（running・このホスト・locked_at=t_lock）の担当だったと確定できるか。
      - 書き手は終了済み（kill 確認済み、または既にいない）
      - ファイル（このジョブを指す）がロック時刻以降に書かれている
        （ワーカーはジョブ取得後に書き、running のジョブは誰も取得しないので、ロック後に書けるのは担当者だけ）
      - 生きているワーカーに、このジョブを担当中（owner）のものがいない
    """
    return (
        writer_gone
        and info["mtime"] + CLOCK_MARGIN_SEC >= t_lock
        and "owner" not in states.values()
    )


# =========================
# ブラウザ応答待ちの見張り（ハング時にそのワーカーのブラウザだけ終了）
# =========================
_browser_killed = set()  # 終了させた (pid, started)。同じ待ちに対して繰り返さない


def check_browser_waits():
    seen = set()
    for path in glob.glob(os.path.join(BASE_DIR, "worker_browserwait_*.txt")):
        info = parse_browser_wait_file(path)
        if info is None:
            continue
        pid, started = info["pid"], info["started"]
        proc = get_live_worker_process(pid)
        if proc is None or proc.create_time() > started + 1:
            # 書き手のワーカーはもういない（同じ pid の別プロセスを含む）
            _remove(path)
            continue

        key = (pid, started)
        seen.add(key)
        waited = time.time() - started
        if waited < BROWSER_HANG_SEC or key in _browser_killed:
            continue

        _browser_killed.add(key)
        targets = worker_browser_processes(proc)
        if not targets:
            rlog(f"[BROWSER HANG] pid={pid} job={info['job_id']} 応答待ち {int(waited)}秒 だがブラウザのプロセスなし")
            continue
        for t in targets:
            try:
                t.kill()
            except psutil.Error:
                pass
        gone, alive = psutil.wait_procs(targets, timeout=KILL_WAIT_SEC)
        rlog(f"[BROWSER HANG][KILL] pid={pid} job={info['job_id']} 応答待ち {int(waited)}秒 → "
             f"ブラウザ終了 {len(gone)}/{len(targets)} プロセス（ワーカーは BROWSER_BROKEN で復帰する想定）")

    for key in list(_browser_killed):
        if key not in seen:
            _browser_killed.discard(key)


# =========================
# statusファイルのない取り残しジョブの検知と回収
# =========================
_orphan_suspects = {}  # job_id -> (locked_at, 最初に検知した time.time())


def check_orphan_jobs():
    """
    このホスト名で running のままのジョブのうち、担当していた可能性のある生存ワーカーが
    いないと確定したものを、ORPHAN_CONFIRM_SEC 以上あけた2回の確認を経て pending へ戻す。
    DBに接続できない場合は何もしない（次回再確認）。
    """
    live = list_live_workers()
    files = read_status_files()

    try:
        conn = open_db()
    except Exception as e:
        rlog(f"[ORPHAN][DB ERROR] 接続失敗: {e} → 次回再確認")
        return

    try:
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT job_id, locked_at FROM trx.scrape_job WHERE status = 'running' AND worker_name = ?",
                HOST_WORKER_NAME,
            )
            rows = cur.fetchall()
            cur.close()
            conn.commit()
        except Exception as e:
            rlog(f"[ORPHAN][DB ERROR] running取得失敗: {e} → 次回再確認")
            return

        now = time.time()
        candidates = set()
        for job_id, locked_at in rows:
            if locked_at is None:
                continue
            t_lock = locked_at_epoch(locked_at)
            if now - t_lock < ORPHAN_GRACE_SEC:
                continue

            ok, states = judge_requeue(job_id, t_lock, live, files)
            if not ok:
                continue

            candidates.add(job_id)
            prev = _orphan_suspects.get(job_id)
            if prev is None or prev[0] != locked_at:
                _orphan_suspects[job_id] = (locked_at, now)
                rlog(f"[ORPHAN][SUSPECT] job={job_id} locked_at={locked_at} 担当ワーカーなし"
                     f"（{format_states(states)}）→ {ORPHAN_CONFIRM_SEC}秒後に再確認")
                continue
            if now - prev[1] < ORPHAN_CONFIRM_SEC:
                continue

            page_hint = page_hint_for(job_id, t_lock, files, live)
            try:
                n = requeue_job(conn, job_id, locked_at, page_hint)
            except Exception as e:
                rlog(f"[RECOVER][DB ERROR] job={job_id} 再投入失敗: {e} → 次回再試行")
                continue

            _orphan_suspects.pop(job_id, None)
            if n == 1:
                rlog(f"[RECOVER][OK] job={job_id} worker={HOST_WORKER_NAME} locked_at={locked_at} "
                     f"理由=担当ワーカー不在（{format_states(states)}）→ pending (current_page={page_hint})")
            else:
                rlog(f"[RECOVER][SKIP] job={job_id} 判定後に状態が変わっていたため更新せず")

        for job_id in list(_orphan_suspects):
            if job_id not in candidates:
                _orphan_suspects.pop(job_id, None)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =========================
# メインループ
# =========================
def main():
    print("[WATCHDOG START]")

    # watchdog だけを再起動した場合、動いているワーカーはそのまま引き継ぐ（足りない分だけ起動する）
    for _ in range(TARGET_WORKERS - count_process()):
        start_worker()
        time.sleep(1)

    last_orphan_check = 0.0
    while True:
        files = glob.glob(os.path.join(BASE_DIR, "worker_status_*.txt"))
        print(f"[LOOP] worker_files={len(files)}", flush=True)

        for path in files:
            process_file(path)

        try:
            check_browser_waits()
        except Exception as e:
            rlog(f"[BROWSER HANG][ERROR] {e}")

        if time.time() - last_orphan_check >= ORPHAN_CHECK_INTERVAL_SEC:
            last_orphan_check = time.time()
            try:
                check_orphan_jobs()
            except Exception as e:
                rlog(f"[ORPHAN][ERROR] {e}")

        running = count_process()
        if running < TARGET_WORKERS:
            for _ in range(TARGET_WORKERS - running):
                start_worker()

        time.sleep(10)

# =========================
# 実行エントリ
# =========================
if __name__ == "__main__":
    main()
