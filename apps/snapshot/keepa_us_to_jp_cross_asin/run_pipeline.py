# run_pipeline.py
import subprocess
import sys
import time
import os
from multiprocessing import Process, Manager

# ==========================================
# グローバル設定: 実行ファイル名の一元管理
# ==========================================
# スクリプトが置いてあるディレクトリを絶対パスで取得
if hasattr(sys, 'frozen'):
    # 実行ファイル形式(.exe)にしている場合
    BASE_DIR = os.path.dirname(sys.executable)
else:
    # 通常の.py実行の場合
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STEP2_SCRIPT = os.path.join(BASE_DIR, "step2_SP_API_amazon_jp_data.py")
STEP3_SCRIPT = os.path.join(BASE_DIR, "step3_check_brand_wakarunda.py")
STEP4_SCRIPT = os.path.join(BASE_DIR, "step4_SP_API_check_us_existence.py")

STEP3_INTERVAL = 30
STEP4_INTERVAL = 20

def run_follower(script_name, shared_status, parent_key, interval):
    """
    監視対象（parent_key）が True である間、追いかけ実行を繰り返す。
    """
    my_key = 'p3' if "step3" in script_name else 'p4'
    
    while shared_status.get(parent_key, True):
        print(f"[{os.path.basename(script_name)}] 追いかけ実行中...")
        subprocess.run([sys.executable, script_name])
        time.sleep(interval)
    
    print(f"[{os.path.basename(script_name)}] 前工程 {parent_key} の終了を確認しました。")
    print(f"[{os.path.basename(script_name)}] 最終チェック（全件網羅確認）を実行して終了します。")
    subprocess.run([sys.executable, script_name])
    
    # 自分の完了を報告
    shared_status[my_key] = False

def run_main_process(script_name, shared_status, my_key):
    """ Step 2 専用の実行関数 """
    print(f"[{os.path.basename(script_name)}] メインプロセス開始...")
    subprocess.run([sys.executable, script_name])
    shared_status[my_key] = False 
    print(f"[{os.path.basename(script_name)}] メインプロセス終了。")

if __name__ == "__main__":
    # Windowsでのマルチプロセス実行を安定させる設定
    import multiprocessing
    multiprocessing.freeze_support()

    print(f"Working Directory: {BASE_DIR}")
    print("=== 順次連動パイプライン (2 -> 3 -> 4) を開始します ===")

    with Manager() as manager:
        shared_status = manager.dict({
            'p2': True,
            'p3': True,
            'p4': True
        })

        # プロセスの定義
        p2 = Process(target=run_main_process, args=(STEP2_SCRIPT, shared_status, 'p2'))
        p3 = Process(target=run_follower, args=(STEP3_SCRIPT, shared_status, 'p2', STEP3_INTERVAL))
        p4 = Process(target=run_follower, args=(STEP4_SCRIPT, shared_status, 'p3', STEP4_INTERVAL))

        # 起動
        p2.start()
        p3.start()
        p4.start()

        # 待機
        p2.join()
        p3.join()
        p4.join()

    print("\n=== すべての工程が順番通りに完了しました ===")