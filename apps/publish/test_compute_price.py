import sys
import os
from pathlib import Path

# プロジェクトのルートディレクトリを絶対パスで追加
project_root = r"D:\apps_nostock"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from apps.common.utils import compute_start_price_usd
    import apps.common.utils
    print(f"DEBUG: 使用中のファイル -> {apps.common.utils.__file__}\n")
except ImportError as e:
    print(f"❌ インポート失敗: {e}")
    sys.exit(1)

def run_test(cost, mode, low=None, high=None, label=""):
    print(f"--- {label} ---")
    print(f"仕入れ値: {cost} JPY | モード: {mode} | レンジ: {low}～{high}")
    
    result = compute_start_price_usd(cost, mode, low_usd_target=low, high_usd_target=high)
    
    if result is None:
        print(f"結果: ❌ None (レンジ外、または実装不備)")
    else:
        print(f"結果: ✅ {result} $")
    print("-" * 30)
    return result

if __name__ == "__main__":
    # ケース1: 元々の問題 (引数なしで None になってしまうパターン)
    run_test(79000, "GA", label="テストケース1: 引数なし (既存バグ確認)")

    # ケース2: ご要望の追加テスト (高額商品 + レンジ指定内)
    run_test(200000, "GA", low=80, high=2500, label="テストケース2: 高額 + レンジ内")

    # ケース3: 正常に None を返すべきパターン (レンジ外)
    run_test(200000, "GA", low=80, high=1000, label="テストケース3: 高額 + レンジ外 (None期待)")

    print("\n※ すべての結果が期待通りでない場合は utils.py の return 文のインデントを修正してください。")