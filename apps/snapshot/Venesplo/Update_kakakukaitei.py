import os
import glob
import pandas as pd

# ==========================================
# 設定
# ==========================================
# Venesploのベースディレクトリ
BASE_DIR = r"Y:\Amazon輸出"

# レポートファイルが格納されているディレクトリ（スクリプトと同じ階層の場合は "./"）
INPUT_DIR = r"X:\apps\snapshot\Venesplo\reportfile" 

# ==========================================
# 関数
# ==========================================
def select_file_interactive(file_list, description):
    """
    ファイルリストからユーザーに選択させるプロンプトを表示します。
    """
    if not file_list:
        return None
    
    # 候補が1つしかない場合はそれをそのまま返す
    if len(file_list) == 1:
        return file_list[0]
    
    # 更新日時が新しい順に並び替え
    file_list.sort(key=os.path.getmtime, reverse=True)
    
    print(f"\n複数の{description}が見つかりました。使用するファイルを選択してください：")
    for i, file_path in enumerate(file_list, 1):
        print(f"[{i}] {file_path}")
        
    while True:
        choice = input("番号を入力してください (1, 2, 3...): ")
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(file_list):
                return file_list[idx]
            else:
                print("無効な番号です。リストにある番号を入力してください。")
        except ValueError:
            print("半角数字を入力してください。")

def get_target_csv_path(region_code):
    """
    指定リージョン(ca/us)のShudouKakakukaiteiASIN.csvパスを取得します。
    """
    pattern = os.path.join(BASE_DIR, f"Venesplo_Ver.*_{region_code}", "report", "ShudouKakakukaiteiASIN.csv")
    csv_files = glob.glob(pattern)
    
    return select_file_interactive(csv_files, f"更新対象のCSV ({region_code.upper()})")

def process_report(report_path, target_csv_path):
    """
    レポートを読み込み、条件に合致する未知のASINをCSVに追記します。
    """
    print(f"\n--- 処理開始 ---")
    print(f"入力レポート: {os.path.basename(report_path)}")
    print(f"更新対象CSV: {target_csv_path}")

    # 1. 既存のCSVから登録済みASINのリストを取得
    try:
        existing_df = pd.read_csv(target_csv_path, encoding='utf-8', header=None, names=['ASIN', 'Price', 'Note'], on_bad_lines='skip')
    except UnicodeDecodeError:
        existing_df = pd.read_csv(target_csv_path, encoding='shift_jis', header=None, names=['ASIN', 'Price', 'Note'], on_bad_lines='skip')
    
    existing_asins = set(existing_df['ASIN'].dropna().astype(str))

    # 2. レポートファイル（TSV）を読み込む
    try:
        report_df = pd.read_csv(report_path, sep='\t', encoding='shift_jis', dtype=str)
    except UnicodeDecodeError:
        report_df = pd.read_csv(report_path, sep='\t', encoding='utf-8', dtype=str)

    # 3. データのクレンジングと条件フィルタリング
    # レポートの種類がブレても対応できるよう列名を柔軟に取得
    asin_col = 'asin1' if 'asin1' in report_df.columns else 'asin'
    sku_col = 'seller-sku' if 'seller-sku' in report_df.columns else 'sku'

    # 必須列の存在確認
    missing_cols = []
    if asin_col not in report_df.columns: missing_cols.append(asin_col)
    if sku_col not in report_df.columns: missing_cols.append(sku_col)
    if 'quantity' not in report_df.columns: missing_cols.append('quantity')
    if 'price' not in report_df.columns: missing_cols.append('price')

    if missing_cols:
        print(f"エラー: レポートに必要な列が見つかりません: {missing_cols}")
        print(f"実際の列一覧: {list(report_df.columns)}")
        return

    report_df['quantity'] = pd.to_numeric(report_df['quantity'], errors='coerce').fillna(0)
    
    # 抽出条件: asin列 == sku列 かつ quantity != 0 かつ priceが空白(NaN/Null)ではない
    condition = (
        (report_df[asin_col] == report_df[sku_col]) &
        (report_df['quantity'] != 0) &
        (report_df['price'].notna()) &
        (report_df['price'].str.strip() != '')
    )
    filtered_df = report_df[condition]

    # 4. 既存CSVに存在しないASINのみを抽出
    new_items = filtered_df[~filtered_df[asin_col].isin(existing_asins)]

    if new_items.empty:
        print(" -> 追加する新しいASINはありませんでした。")
        return

    # 5. CSVへの追記
    with open(target_csv_path, 'a', encoding='shift_jis', newline='') as f:
        for _, row in new_items.iterrows():
            f.write(f"{row[asin_col]},{row['price']},\n")

    print(f" -> {len(new_items)}件のASINを追記しました。")

# ==========================================
# メイン処理
# ==========================================
def main():
    regions = {'us': 'US', 'ca': 'CA'}
    
    for region_code, prefix in regions.items():
        print(f"\n================ {prefix} の処理 ================")
        
        # 検索パターンを「すべての出品商品のレポート」に変更
        report_pattern = os.path.join(INPUT_DIR, f"{prefix}_すべての出品商品のレポート_*.txt")
        report_files = glob.glob(report_pattern)
        
        if not report_files:
            print(f"[{prefix}] レポートファイルが見つかりません。スキップします。")
            continue
            
        # レポートファイルの選択
        latest_report = select_file_interactive(report_files, f"入力レポート ({prefix})")
        
        # 追記先のCSVファイルパスの選択
        target_csv = get_target_csv_path(region_code)
        
        if target_csv:
            process_report(latest_report, target_csv)
        else:
            print(f"[{prefix}] 更新対象の ShudouKakakukaiteiASIN.csv が見つかりませんでした。")

if __name__ == "__main__":
    main()