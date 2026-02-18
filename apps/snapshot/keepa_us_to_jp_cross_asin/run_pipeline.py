# run_pipeline.py
# -*- coding: utf-8 -*-
import os
import sys
import time
import subprocess
import multiprocessing
from multiprocessing import Process, Manager, Value

# --- パス設定 ---
BASE_DIR = r"\\MOUSE\apps_nostock\apps\snapshot\keepa_us_to_jp_cross_asin"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from step0_category_manager import get_next_category, update_category_fetched_at

# --- スクリプトパス設定 ---
STEP1_KEEPA = os.path.join(BASE_DIR, "step1_keepa_jp_to_asin.py")
STEP2_SCRIPT = os.path.join(BASE_DIR, "step2_SP_API_amazon_jp_data.py")
STEP3_SCRIPT = os.path.join(BASE_DIR, "step3_check_brand_wakarunda.py")
STEP4_SCRIPT = os.path.join(BASE_DIR, "step4_SP_API_check_us_existence.py")
STEP5_SCRIPT = os.path.join(BASE_DIR, "step5_get_ATS_input_asin.py")

# 各工程の間隔設定
STEP2_INTERVAL = 10
STEP3_INTERVAL = 30
STEP4_INTERVAL = 20

def run_step1_window(shared_status):
    """
    カテゴリが尽きるまで Step 1 を新しいウィンドウで繰り返し実行する
    """
    while True:
        category = get_next_category()
        if not category:
            print("[Step 1] 処理対象のカテゴリがすべて終了しました。")
            break
        
        cat_id = category['id']
        cat_name = category['name']
        print(f"[Step 1 Monitor] 次のカテゴリを開始します: {cat_name} (ID: {cat_id})")

        # Step 1 を別窓で実行し、終了を待つ (/wait を付与)
        title = f"STEP1_{cat_name}"
        cmd = f'start /wait "{title}" cmd /c python -u "{STEP1_KEEPA}" {cat_id}'
        
        result = subprocess.run(cmd, shell=True)

        # 正常終了(ウィンドウが閉じられた)した場合に日付更新
        if result.returncode == 0:
            update_category_fetched_at(cat_id)
            print(f"[Step 1 Monitor] 完了: {cat_name}")
        else:
            print(f"[Step 1 Monitor] エラーまたは中断: {cat_name}")
            time.sleep(10)

    shared_status['p1_running'] = False

def run_follower_window(script_name, shared_status, parent_key, interval, my_key):
    """
    新しいウィンドウで追いかけスクリプトを実行し、終了を待ってから待機する
    """
    script_basename = os.path.basename(script_name)
    
    while shared_status.get(parent_key, True):
        print(f"[{script_basename} Monitor] 実行中...")
        
        title = f"PROCESS_{script_basename}"
        # /wait を付けて、スクリプトが終わるまでこのループを止める
        cmd = f'start /wait "{title}" cmd /c python -u "{script_name}"'
        
        subprocess.run(cmd, shell=True)
        
        # インターバル待機
        wait_start = time.time()
        while time.time() - wait_start < interval:
            if not shared_status.get(parent_key, True):
                break
            time.sleep(1)
    
    # 最終実行
    print(f"[{script_basename} Monitor] 最終実行中...")
    subprocess.run([sys.executable, "-u", script_name])
    shared_status[my_key] = False

if __name__ == "__main__":
    multiprocessing.freeze_support()

    with Manager() as manager:
        shared_status = manager.dict({
            'p1_running': True, 
            'p2_running': True, 
            'p3_running': True, 
            'p4_running': True
        })
        
        # すべてを Process として定義（すべて別窓で開く）
        p1 = Process(target=run_step1_window, args=(shared_status,))
        p2 = Process(target=run_follower_window, args=(STEP2_SCRIPT, shared_status, 'p1_running', STEP2_INTERVAL, 'p2_running'))
        p3 = Process(target=run_follower_window, args=(STEP3_SCRIPT, shared_status, 'p2_running', STEP3_INTERVAL, 'p3_running'))
        p4 = Process(target=run_follower_window, args=(STEP4_SCRIPT, shared_status, 'p3_running', STEP4_INTERVAL, 'p4_running'))

        print("=== 全工程を別ウィンドウで起動します ===")
        p1.start()
        p2.start()
        p3.start()
        p4.start()

        # 全プロセスの終了を待機
        p1.join()
        p2.join()
        p3.join()
        p4.join()

        # 最後に Step 5 を実行
        print("\n=== 全工程完了。最終の Step 5 を実行します ===")
        subprocess.run([sys.executable, "-u", STEP5_SCRIPT])

    print("=== 全パイプライン処理が終了しました ===")