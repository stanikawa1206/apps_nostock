import pandas as pd

def clean_ebay_data(input_file, output_file):
    # 処理対象のシート名
    target_sheets = ['API', 'eBay']
    
    print(f"▶ データの読み込みと変換を開始します: {input_file}")
    
    # 新しいExcelファイルに書き込むためのライターを準備
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        
        for sheet_name in target_sheets:
            try:
                # 1. シートの読み込み
                df = pd.read_excel(input_file, sheet_name=sheet_name)
                
                # 2. "--" を空白（空文字列）に完全置換
                df = df.replace('--', '')
                
                # 3. 日付列を yy/mm/dd 形式に変換
                date_columns = ['Transaction creation date', 'Payout date']
                
                for col in date_columns:
                    if col in df.columns:
                        # 一旦日付型(datetime)に変換。空欄などはNaT（欠損値）になる
                        parsed_dates = pd.to_datetime(df[col], errors='coerce')
                        
                        # yy/mm/dd フォーマットに変換。変換できない部分(NaT)は空白で埋める
                        df[col] = parsed_dates.dt.strftime('%y/%m/%d').fillna('')
                
                # 4. 新しいファイルに同名シートとして書き込み
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  ✅ シート '{sheet_name}' の変換が完了しました。")
                
            except Exception as e:
                print(f"  ❌ シート '{sheet_name}' の処理中にエラーが発生しました: {e}")

    print(f"\n🎉 すべての処理が完了しました！\n出力先: 📁 {output_file}")


if __name__ == "__main__":
    # 入力ファイルと出力ファイルのパスを指定してください
    # (例: Y:\ebay\data\API分析.xlsx)
    input_path = r"Y:\ebay\data\API分析.xlsx"
    output_path = r"Y:\ebay\data\API分析_変換後.xlsx"
    
    clean_ebay_data(input_path, output_path)