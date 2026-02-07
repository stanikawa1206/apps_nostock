# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import time
from typing import Any, Dict, List, Optional
from pathlib import Path
import sys
import requests
import pandas as pd
import openpyxl

# プロジェクトルート設定（既存踏襲）
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ★テスト用なのでDB接続は一旦不要だが、importエラー防止のため残すか、ダミーにする
try:
    from apps.common.utils import get_sql_server_connection
except ImportError:
    def get_sql_server_connection(): return None

# ============================================================
# 設定値 & テスト用パラメータ
# ============================================================

TASK_FILE = r"Y:amazon_カテゴリリスト.xlsx"  # 同じフォルダにある前提

# ランキング範囲
RANK_MIN = 1
RANK_MAX = 1_000_000
OVERLAP_RANK = 1000  # 1000位のオーバーラップ

# Keepa設定
PRICE90_NEW_JPY_MIN = 10_000
PRICE90_NEW_JPY_MAX = 300_000
KEEPA_QUERY_LIMIT = 10000 
LIMIT_TOKEN = 150

# 物理フィルタ（API検索用）
MAX_EDGE_CM = 160
MAX_WEIGHT_G = 30_000
MAX_EDGE_MM = MAX_EDGE_CM * 10

KEEPA_API_KEY = os.getenv("KEEPA_API_KEY") or os.getenv("KEEPA_KEY")
KEEPA_BASE = "https://api.keepa.com"
DOMAIN_JP = 5

# ============================================================
# APIリクエスト関連
# ============================================================
def get_token_status() -> dict:
    url = f"{KEEPA_BASE}/token"
    try:
        r = requests.get(url, params={"key": KEEPA_API_KEY}, timeout=30)
        r.raise_for_status()
        data = r.json()
        return {"tokens_left": data.get("tokensLeft", 0), "refill_in_ms": data.get("refillIn", 0)}
    except:
        return {"tokens_left": 0, "refill_in_ms": 5000}

def ensure_tokens():
    while True:
        status = get_token_status()
        tokens = status["tokens_left"]
        if tokens >= LIMIT_TOKEN: break
        wait_sec = (status["refill_in_ms"] / 1000.0) + 2.0
        print(f"   [Token Wait] 残り{tokens} - {wait_sec:.1f}秒待機")
        time.sleep(wait_sec)

def keepa_request(endpoint: str, method: str = "GET", params: dict = None, data: dict = None):
    ensure_tokens()
    time.sleep(0.5)
    url = f"{KEEPA_BASE}/{endpoint}"
    p = {"key": KEEPA_API_KEY}
    if params: p.update(params)
    
    for attempt in range(3):
        try:
            if method.upper() == "POST":
                r = requests.post(url, params=p, data=json.dumps(data), timeout=180)
            else:
                r = requests.get(url, params=p, timeout=180)
            
            if r.status_code == 429:
                time.sleep(30 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2: raise e
            time.sleep(5)
    return {}

# ============================================================
# ロジック（テスト用改修版）
# ============================================================

def fetch_recursive_test(cat_id, min_rank, max_rank) -> int:
    """
    再帰的に取得し、ASIN数をカウントして返す（DB保存はしない）。
    """
    print(f"      [Query] Rank {min_rank} - {max_rank} ... ", end="", flush=True)
    
    selection = {
        "categories_include": [cat_id], "productType": 0,
        "avg90_NEW_gte": PRICE90_NEW_JPY_MIN, "avg90_NEW_lte": PRICE90_NEW_JPY_MAX,
        "current_SALES_gte": min_rank, "current_SALES_lte": max_rank,
        "packageLength_lte": MAX_EDGE_MM, "packageWeight_lte": MAX_WEIGHT_G,
        "perPage": KEEPA_QUERY_LIMIT
    }
    
    # APIリクエスト
    res = keepa_request("query", method="POST", params={"domain": DOMAIN_JP}, data=selection)
    total = res.get("totalResults", 0)
    asins = res.get("asinList", [])
    count = len(asins)

    # 上限チェック & 分割ロジック
    if total >= KEEPA_QUERY_LIMIT and (max_rank - min_rank) > 1:
        print(f"HIT {total} (上限到達) -> 分割再帰")
        mid = (min_rank + max_rank) // 2
        
        # 分割して合計を返す
        c1 = fetch_recursive_test(cat_id, min_rank, mid)
        c2 = fetch_recursive_test(cat_id, mid + 1, max_rank)
        return c1 + c2
    else:
        # 正常取得
        print(f"HIT {count} -> OK")
        if count > 0:
            print(f"         Top10: {asins[:10]}")
        return count

# ============================================================
# メイン処理（Excel連携テスト）
# ============================================================
def find_start_column_index(df):
    for i, col in enumerate(df.columns):
        if str(col).strip() == "1":
            return i
    return 12 

def main():
    print("=== Keepa Split Logic TEST START ===")
    
    if not os.path.exists(TASK_FILE):
        print(f"Error: {TASK_FILE} が見つかりません。")
        return

    # Excel読み込み
    try:
        df = pd.read_excel(TASK_FILE)
    except Exception as e:
        print(f"Excel読み込みエラー: {e}")
        return

    start_col_idx = find_start_column_index(df)
    
    # 各行（カテゴリ）ループ
    for i, row in df.iterrows():
        cat_id = row.get("カテゴリID")
        cat_name = row.get("カテゴリ名")
        split_count = row.get("分割数", 0)
        done_count = row.get("済分割", 0) # テストなのでここが0でもOK
        rank_step = row.get("ランキング分割", 0)
        target_asin_count = row.get("該当ASIN", 0) # 比較用の参考値

        # 0分割（対象外）はスキップ
        if split_count == 0:
            continue
            
        # テストのため、済分割に関わらず「最初の数ブロック」だけ試すか、
        # あるいは特定のカテゴリだけ試す制御を入れても良い
        # ここでは「DIY・工具」など特定の巨大カテゴリだけ動かす例にするなら if cat_id != ...: continue を入れる
        
        if split_count - done_count <= 0:
            print(f"SKIP: {cat_name} (完了済み)")
            continue

        print(f"\n--------------------------------------------------")
        print(f"Category: {cat_name} ({cat_id})")
        print(f"設定: 全{split_count}分割 / 幅{rank_step} / 済{done_count}")
        print(f"目標総ASIN数: {target_asin_count} (目安)")
        print(f"--------------------------------------------------")

        category_total_hits = 0
        
        # 残りのブロックをループ
        remaining = split_count - done_count
        
        # ★テスト短縮用: 全部は長いので、最初の3ブロックだけ試す設定（必要なら外してください）
        # remaining = min(remaining, 3) 
        
        for j in range(remaining):
            current_block_idx = done_count + j
            
            # --- ランク計算ロジック（下から上へ） ---
            # 例: 全100万, step 1万, idx 0
            # Base Max = 100万 - (0 * 1万) = 100万
            # Min = 100万 - 1万 = 99万
            
            r_max_base = RANK_MAX - (current_block_idx * rank_step)
            r_min = r_max_base - rank_step
            
            if r_max_base > RANK_MAX: r_max_base = RANK_MAX
            if r_min < RANK_MIN: r_min = RANK_MIN
            
            # オーバーラップ +1000
            r_max_query = r_max_base + OVERLAP_RANK
            
            # 範囲チェック
            if r_min >= r_max_query: 
                print("   [End] ランク範囲終了")
                break

            print(f"[{j+1}/{remaining}] Block {current_block_idx+1}: {r_min} ~ {r_max_query} (BaseMax: {r_max_base})")
            
            # API実行 & カウント
            hits = fetch_recursive_test(cat_id, r_min, r_max_query)
            category_total_hits += hits
            
            # Excelへの書き込みシミュレーション
            write_col = start_col_idx + current_block_idx
            print(f"      -> Block完了。Excel列 {write_col+1} ({df.columns[write_col]}) に書き込み予定")
            
            time.sleep(1) # API負荷軽減

        print(f"\n>>> {cat_name} テスト結果合計: {category_total_hits} ASIN取得")
        # 比較（全件回した場合のみ意味がある）
        # if remaining == (split_count - done_count):
        #     print(f"    (Excel想定: {target_asin_count} vs 実績: {category_total_hits})")

if __name__ == "__main__":
    main()