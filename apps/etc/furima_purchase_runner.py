# -*- coding: utf-8 -*-
"""
Access「到着日入力」フォームの「フリマ情報取得」ボタンから起動する薄いラッパー。

メルカリ・Yahoo!フリマ・ラクマの既存 main()（mercari_purchase.py /
yahoo_furima_purchase.py / rakuma_purchase.py）を順番に呼び出すだけで、
各サイトの取得ロジック（ステータス判定・送り状番号・到着日・メッセージ取得・
DB保存等）は一切変更・複製しない。

1サイトの失敗で残りのサイトの実行を止めない（サイト単位でtry/exceptする）。

二重起動防止:
  ロックファイル(furima_purchase_runner.lock)に自分のPIDを書き込む。
  既存のロックファイルがある場合、そのPIDが実際に生きているプロセスかどうかを
  psutilで確認し、生きていれば「実行中」として二重起動を拒否する。
  PC再起動・強制終了等でプロセスが既に無いのにロックファイルだけ残っている
  （stale lock）場合は無視して実行する。

状態ファイル(furima_purchase_runner_status.txt)に running/done/error と
サイトごとの結果(success/error)を書き込む。Access側のフォームタイマーが
これをポーリングして表示更新・Requeryに使う。VBA側にJSONパーサーを新設
せずに読めるよう、あえてJSONではなく1行1個の"key=value"形式にしている。
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
from datetime import datetime
from pathlib import Path

import psutil

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from apps.etc.mercari_purchase import main as run_mercari  # noqa: E402
from apps.etc.yahoo_furima_purchase import main as run_yahoo_furima  # noqa: E402
from apps.etc.rakuma_purchase import main as run_rakuma  # noqa: E402

LOCK_FILE = Path(__file__).with_name("furima_purchase_runner.lock")
STATUS_FILE = Path(__file__).with_name("furima_purchase_runner_status.txt")

# (状態ファイル上の表示名, 呼び出す既存main())
SITES = [
    ("Mercari", run_mercari),
    ("YahooFurima", run_yahoo_furima),
    ("Rakuma", run_rakuma),
]


def _write_status(state: str, results: dict, started_at: str, finished_at: str = "") -> None:
    lines = [
        f"state={state}",
        f"started_at={started_at}",
        f"finished_at={finished_at}",
    ]
    lines += [f"{name}={value}" for name, value in results.items()]
    STATUS_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _is_locked_by_live_process() -> bool:
    """既存ロックファイルがあり、かつそのPIDが実際に生きているプロセスならTrue。
    ファイルはあるが対応プロセスが存在しない場合（stale lock）はFalseを返す。"""
    if not LOCK_FILE.exists():
        return False
    try:
        pid = int(LOCK_FILE.read_text(encoding="utf-8").strip())
    except (ValueError, OSError):
        return False
    return psutil.pid_exists(pid)


def main() -> None:
    if _is_locked_by_live_process():
        print("[INFO] 既に実行中のプロセスが存在するため、今回は起動しません。")
        return

    LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
    started_at = datetime.now().isoformat()
    results = {name: "pending" for name, _ in SITES}
    _write_status("running", results, started_at)

    try:
        for name, run_func in SITES:
            print(f"\n{'=' * 20} {name} {'=' * 20}", flush=True)
            try:
                run_func()
                results[name] = "success"
            except Exception as e:
                print(f"[ERROR] {name} の実行中にエラーが発生しました: {e}", flush=True)
                results[name] = "error"
            # 1サイト終わるたびに書き込み、Access側が進捗を見られるようにする。
            _write_status("running", results, started_at)

        overall_state = "done" if all(v == "success" for v in results.values()) else "error"
        _write_status(overall_state, results, started_at, finished_at=datetime.now().isoformat())

    finally:
        try:
            LOCK_FILE.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    main()
