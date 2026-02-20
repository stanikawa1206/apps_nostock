# run_pipeline.py
# -*- coding: utf-8 -*-
import os
import sys
import time
import subprocess
import multiprocessing
from multiprocessing import Process, Manager

# --- パス設定 ---
BASE_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

STEP1_KEEPA = os.path.join(BASE_DIR, "step1_keepa_jp_to_asin.py")
STEP2_SCRIPT = os.path.join(BASE_DIR, "step2_SP_API_amazon_jp_data.py")
STEP3_SCRIPT = os.path.join(BASE_DIR, "step3_check_brand_wakarunda.py")
STEP4_SCRIPT = os.path.join(BASE_DIR, "step4_SP_API_check_us_existence.py")

RESTART_INTERVAL = 3000  # 1時間

def run_step1_once():
    """Step1を一度だけ別ウィンドウで起動する"""
    print(f"[Step1] 起動します...")
    # タイトルを固定して起動
    cmd = f'start "PIPELINE_STEP1" python "{STEP1_KEEPA}"'
    subprocess.run(cmd, shell=True)

def run_follower_process(script_path, title_name):
    """個別のプロセスとして実行（ループはメイン側で制御）"""
    print(f"[{title_name}] ウィンドウを起動します...")
    # titleコマンドでウィンドウ名を固定し、後でキルしやすくする
    cmd = f'start "{title_name}" python "{script_path}"'
    subprocess.run(cmd, shell=True)

def kill_step_processes(titles):
    """指定したタイトルを持つウィンドウを強制終了する"""
    for title in titles:
        print(f"[{title}] を終了させています...")
        # タイトルが完全に一致するウィンドウを狙い撃ち
        cmd = f'taskkill /F /FI "WINDOWTITLE eq {title}" /T'
        subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    multiprocessing.freeze_support()

    # 1. Step1 を最初に一回だけ実行
    run_step1_once()
    
    # 再起動対象の管理
    targets = [
        {"path": STEP2_SCRIPT, "title": "PIPELINE_STEP2"},
        {"path": STEP3_SCRIPT, "title": "PIPELINE_STEP3"},
        {"path": STEP4_SCRIPT, "title": "PIPELINE_STEP4"},
    ]
    target_titles = [t["title"] for t in targets]

    try:
        while True:
            print(f"\n{'='*50}\nStep 2-4 の実行サイクルを開始します\n{'='*50}")
            
            # 2. Step2-4 を起動
            for target in targets:
                run_follower_process(target["path"], target["title"])
                time.sleep(2) # 起動の衝突防止

            print(f"\n次回の再起動まで {RESTART_INTERVAL/60:.0f} 分間待機します...")
            time.sleep(RESTART_INTERVAL)

            # 3. 指定したタイトルのプロセスのみをキル
            print(f"\n--- 定時リフレッシュ: プロセスを終了します ---")
            kill_step_processes(target_titles)
            
            time.sleep(5) # 終了処理の完了待ち
            print("再起動中...")

    except KeyboardInterrupt:
        print("\n親プロセスが停止されました。")
        # 必要に応じて Step1 も終了させる場合は以下を有効にしてください
        # kill_step_processes(["PIPELINE_STEP1"])