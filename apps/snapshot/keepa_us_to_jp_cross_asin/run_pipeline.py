# run_pipeline.py
# -*- coding: utf-8 -*-
import os
import sys
import time
import subprocess
import multiprocessing
from multiprocessing import Process, Manager, Value

# --- パス設定 (ネットワークパスを直接指定) ---
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

# 各工程の設定
STEP3_INTERVAL = 30
STEP4_INTERVAL = 20
STEP5_THRESHOLD = 10000

def run_step1_main(shared_status):
    """
    カテゴリが尽きるまで Step 1 を繰り返す主工程
    """
    while True:
        category = get_next_category()
        if not category:
            print("[Step 1] 処理対象のカテゴリがすべて終了しました。")
            break
        
        cat_id = category['id']
        cat_name = category['name']
        print(f"\n[Step 1] 開始: {cat_name} (ID: {cat_id})")

        # Step 1 実行
        result = subprocess.run([sys.executable, STEP1_KEEPA, str(cat_id)])

        # 正常終了した場合のみ日付を更新
        if result.returncode == 0:
            update_category_fetched_at(cat_id)
            print(f"[Step 1] 成功: {cat_name} の取得完了日を更新しました。")
        else:
            print(f"[Step 1] エラー終了: {cat_name} (ID: {cat_id}) は日付を更新せずにスキップします。")
            time.sleep(10)

    # 全カテゴリ終了フラグを立てる
    shared_status['p1_running'] = False

def run_follower(script_name, shared_status, parent_key, interval, my_key, shared_counter=None):
    """
    前工程が動いている間、または未処理データがある限りループ実行する
    """
    script_basename = os.path.basename(script_name)
    
    # parent_key(前工程)がTrueの間はループし続ける
    while shared_status.get(parent_key, True):
        print(f"[{script_basename}] 追いかけ実行中...")
        result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
        
        # ログ出力（標準出力の表示）
        if result.stdout:
            print(result.stdout.strip())
        
        # Step 4 の場合、米国存在件数をカウントしてStep 5へ連携
        if shared_counter is not None and "US_EXIST_COUNT:" in result.stdout:
            try:
                # スクリプト内で print("US_EXIST_COUNT: 10") のように出力する想定
                count = int(result.stdout.split("US_EXIST_COUNT:")[1].split()[0])
                with shared_counter.get_lock():
                    shared_counter.value += count
            except: pass

        time.sleep(interval)
    
    # 前工程が終了した後の「最終掃き出し」実行
    print(f"[{script_basename}] 前工程終了につき、最終実行を行います...")
    subprocess.run([sys.executable, script_name])
    shared_status[my_key] = False

if __name__ == "__main__":
    multiprocessing.freeze_support()

    with Manager() as manager:
        # 各工程の稼働状態（p1〜p4）を共有
        shared_status = manager.dict({
            'p1_running': True, 
            'p2_running': True, 
            'p3_running': True, 
            'p4_running': True
        })
        
        # Step 5 発動用の共有カウンタ
        us_exist_counter = Value('i', 0)

        # 各工程をプロセスとして定義
        # p1 が動いている間、p2〜p4 が並列で DB を監視して処理する
        p1 = Process(target=run_step1_main, args=(shared_status,))
        p2 = Process(target=run_follower, args=(STEP2_SCRIPT, shared_status, 'p1_running', 10, 'p2_running'))
        p3 = Process(target=run_follower, args=(STEP3_SCRIPT, shared_status, 'p2_running', STEP3_INTERVAL, 'p3_running'))
        p4 = Process(target=run_follower, args=(STEP4_SCRIPT, shared_status, 'p3_running', STEP4_INTERVAL, 'p4_running', us_exist_counter))

        # 全工程一斉スタート
        p1.start()
        p2.start()
        p3.start()
        p4.start()

        # --- Step 5 監視ループ (メインプロセス) ---
        while shared_status['p4_running']:
            if us_exist_counter.value >= STEP5_THRESHOLD:
                print(f"\n[Monitor] 積算件数が {us_exist_counter.value} に達しました。Step 5 を実行します。")
                subprocess.run([sys.executable, STEP5_SCRIPT])
                with us_exist_counter.get_lock():
                    us_exist_counter.value = 0 # リセット
            time.sleep(30)

        # 全プロセスの終了を待機
        p1.join()
        p2.join()
        p3.join()
        p4.join()

        # すべて終了後の最終書き出し
        print("\n=== 全工程完了。最終の Step 5 を実行中... ===")
        subprocess.run([sys.executable, STEP5_SCRIPT])

    print("=== 全カテゴリのパイプライン処理が終了しました ===")