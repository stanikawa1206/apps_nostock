# -*- coding: utf-8 -*-
import time, re, csv, os, statistics, concurrent.futures
from datetime import datetime

TARGET = "【名古屋】ルイヴィトン モノグラムアンプラント ブロデリー ポルトフォイユ・クレア M81139 レディース 小物"
INTERVALS_MS = [800, 400, 200, 100]   # 間隔（ミリ秒）
ROUNDS = 30                            # 各間隔での呼び出し回数

# utils.translate_to_english を優先、なければローカル定義
try:
    from utils import translate_to_english  # type: ignore
    USING_LOCAL = False
except Exception:
    USING_LOCAL = True
    from deep_translator import GoogleTranslator

    def translate_to_english(text_jp: str, per_attempt_timeout: float = 8.0,
                             attempts: int = 3, backoff_base: float = 1.0) -> str:
        if not text_jp:
            return ""
        text_jp = re.sub(r"\s+", " ", text_jp).strip()
        if not text_jp:
            return ""
        last_err = None
        def _call():
            return GoogleTranslator(source='ja', target='en').translate(text_jp) or ""
        for i in range(1, attempts + 1):
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    fut = ex.submit(_call)
                    out = fut.result(timeout=per_attempt_timeout)
                time.sleep(0.4)  # 軽いクールダウン
                return out
            except concurrent.futures.TimeoutError as te:
                last_err = te
                wait = backoff_base * (2 ** (i - 1))
                time.sleep(wait)
            except Exception as e:
                last_err = e
                wait = backoff_base * (2 ** (i - 1))
                time.sleep(wait)
        raise RuntimeError(f"Translation failed after {attempts} attempts. Last error: {last_err!r}")

def run_round(interval_ms: int, rounds: int, writer):
    latencies = []
    ok = empty = failed = 0
    for idx in range(1, rounds + 1):
        t0 = time.time()
        status = "ok"
        exc_name = ""
        out = ""
        try:
            out = translate_to_english(TARGET)
            if not out.strip():
                status = "empty"
                empty += 1
            else:
                ok += 1
        except Exception as e:
            status = "error"
            exc_name = type(e).__name__
            failed += 1
        elapsed_ms = int((time.time() - t0) * 1000)
        latencies.append(elapsed_ms)
        writer.writerow([datetime.now().isoformat(timespec="seconds"),
                         interval_ms, idx, status, exc_name, elapsed_ms, len(out)])
        # 次の呼び出しまでインターバル
        time.sleep(max(0, interval_ms / 1000.0))
    return ok, empty, failed, latencies

def main():
    log_name = f"translate_burst_{int(time.time())}.csv"
    with open(log_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ts","interval_ms","nth","status","exception","elapsed_ms","len_en"])
        print(f"Using local translate func? {USING_LOCAL}")
        print(f"Target: {TARGET}")
        print(f"Log: {os.path.abspath(log_name)}\n")

        for iv in INTERVALS_MS:
            ok, empty, failed, lats = run_round(iv, ROUNDS, writer)
            p50 = int(statistics.median(lats))
            p95 = int(sorted(lats)[int(len(lats)*0.95)-1])
            print(f"[interval {iv} ms] total={ROUNDS} ok={ok} empty={empty} err={failed} "
                  f"p50={p50}ms p95={p95}ms")

if __name__ == "__main__":
    main()
