import sys
import pandas as pd
import os
import re

def process_files(prefix, file_paths, chunk_size=10000):
    # yymmdd（日付）ごとの連番を管理する辞書
    file_counters = {}
    
    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"エラー: ファイルが見つかりません - {file_path}")
            continue
        
        print(f"処理中: {file_path}")
        
        # ファイル名から8桁の日付(YYYYMMDD)を正規表現で抽出
        filename = os.path.basename(file_path)
        match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
        if match:
            # 西暦の下2桁 + 月 + 日 (例: 20260304 -> 260304)
            yymmdd = match.group(1)[2:] + match.group(2) + match.group(3)
        else:
            yymmdd = "000000" # 日付が見つからない場合のデフォルト
            print(f"警告: ファイル名から日付を抽出できませんでした。'{yymmdd}'を使用します。")
        
        # その日付のカウンターが未登録なら初期化
        if yymmdd not in file_counters:
            file_counters[yymmdd] = 1
            
        try:
            # CSVを読み込む
            df = pd.read_csv(file_path)
            
            # ASIN列が存在するか確認
            if 'ASIN' not in df.columns:
                print(f"スキップ: 'ASIN'列が {file_path} に存在しません。")
                continue
            
            # ASIN列を抽出し、空のデータを除外
            asins = df['ASIN'].dropna()
            total_rows = len(asins)
            
            # 指定された行数(デフォルト10000)ずつ分割して保存
            for i in range(0, total_rows, chunk_size):
                chunk = asins.iloc[i:i+chunk_size]
                
                # 出力ファイル名の作成 (例: us_260304_1.csv)
                output_filename = f"{prefix}_{yymmdd}_{file_counters[yymmdd]}.csv"
                
                # ASIN列のみを保存
                chunk.to_csv(output_filename, index=False, header=['ASIN'])
                print(f"  -> {len(chunk)}件のASINを {output_filename} に保存しました。")
                
                # その日付の連番をカウントアップ
                file_counters[yymmdd] += 1
                
        except Exception as e:
            print(f"エラー: {file_path} の処理中に問題が発生しました。詳細: {e}")

if __name__ == "__main__":
    # 引数の数を確認（スクリプト名 + us/ca + ファイル名1つ以上）
    if len(sys.argv) < 3:
        print("使い方: python extract_asins.py <us|ca> <ファイル1.csv> [ファイル2.csv ...]")
        sys.exit(1)
        
    # 第一引数にプレフィックス (us や ca)
    prefix = sys.argv[1]
    # 第二引数以降をすべて入力ファイルリストとして扱う
    input_files = sys.argv[2:]
    
    process_files(prefix, input_files, chunk_size=10000)