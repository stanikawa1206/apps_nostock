# run_pipeline.py
# -*- coding: utf-8 -*-
import os
import sys
import time
import subprocess
import multiprocessing

# --- パス設定 ---
BASE_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

STEP1_KEEPA = os.path.join(BASE_DIR, "step1_keepa_jp_to_asin.py")
STEP2_SCRIPT = os.path.join(BASE_DIR, "step2_SP_API_amazon_jp_data.py")
STEP3_SCRIPT = os.path.join(BASE_DIR, "step3_SP_API_check_na_existence.py")
STEP4_SCRIPT = os.path.join(BASE_DIR, "step4_check_brand_wakarunda.py")
STEP5_SCRIPT = os.path.join(BASE_DIR, "step5_evaluate_trade_direction.py")

RESTART_INTERVAL = 3000  # 1時間

def launch_persistent_chrome():
    """拡張機能の状態を保持するための専用Chromeを起動する"""
    print("[System] 永続化Chromeをデバッグモードで起動します...")
    # プロファイルを保存するフォルダ（wakarunda_utils.pyと一致させる）
    user_data_dir = r"C:\chrome_wakurunda_profile"
    os.makedirs(user_data_dir, exist_ok=True)
    
    # Chromeの標準的なインストールパス
    chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    
    # ポート9222でデバッグ待ち受け状態にして起動
    cmd = f'start "" "{chrome_path}" --remote-debugging-port=9222 --user-data-dir="{user_data_dir}"'
    subprocess.run(cmd, shell=True)
    time.sleep(3) # 起動完了まで少し待機

def run_step1_once(min_rank, max_rank):
    """Step1を引数とともに一度だけ別ウィンドウで起動する"""
    print(f"[Step1] 起動します... (指定ランク: {min_rank}位 - {max_rank}位)")
    cmd = f'start "PIPELINE_STEP1" python "{STEP1_KEEPA}" {min_rank} {max_rank}'
    subprocess.run(cmd, shell=True)

def run_follower_process(script_path, title_name):
    """個別のプロセスとして実行（ループはメイン側で制御）"""
    print(f"[{title_name}] ウィンドウを起動します...")
    cmd = f'start "{title_name}" python "{script_path}"'
    subprocess.run(cmd, shell=True)

def kill_step_processes(titles):
    """指定したタイトルを持つウィンドウを強制終了する"""
    for title in titles:
        print(f"[{title}] を終了させています...")
        cmd = f'taskkill /F /FI "WINDOWTITLE eq {title}" /T'
        subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def run_step5_evaluation():
    """Step5を現在のウィンドウ内で同期的に実行（完了するまで待機）"""
    print(f"\n{'='*40}")
    print("=== Step5: 輸出入判定の評価処理を開始します ===")
    print(f"{'='*40}")
    cmd = f'python "{STEP5_SCRIPT}"'
    subprocess.run(cmd, shell=True)

if __name__ == "__main__":
    multiprocessing.freeze_support()

    if len(sys.argv) < 3:
        print("エラー: ランキング閾値が指定されていません。")
        print("使用法: python run_pipeline.py [min_rank] [max_rank]")
        sys.exit(1)

    try:
        target_min_rank = int(sys.argv[1])
        target_max_rank = int(sys.argv[2])
    except ValueError:
        print("エラー: ランクは数値で指定してください。")
        sys.exit(1)

    # 💡 パイプラインの一番最初でChromeを永続起動
    launch_persistent_chrome()

    # 1. Step1 を引数付きで最初に一回だけ実行
    run_step1_once(target_min_rank, target_max_rank)

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

            print(f"\n次回の再起動・評価(Step5)まで {RESTART_INTERVAL/60:.0f} 分間待機します...")
            time.sleep(RESTART_INTERVAL)

            # 3. 指定したタイトルのプロセスのみをキル (Chrome本体はキルされない)
            print(f"\n--- 定時リフレッシュ: プロセスを終了します ---")
            kill_step_processes(target_titles)
            
            time.sleep(5) # 終了処理の完了待ち

            # 4. Step5 を実行して、この1時間で取得したデータを評価・計算
            run_step5_evaluation()

            print("再起動中...")

    except KeyboardInterrupt:
        print("\n親プロセスが停止されました。")