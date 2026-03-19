def check_export_profitability(cost_jpy, shipping_jpy, fee_rate, lowest_price_foreign, exchange_rate):
    """
    輸出で利益が確保できるかを判定する関数
    
    cost_jpy: 仕入値（日本円）
    shipping_jpy: 送料（日本円）
    fee_rate: Amazon手数料率（例: 0.15）
    lowest_price_foreign: 現状の最安出品者の価格（現地通貨）
    exchange_rate: 為替レート（例: 1ドル=150円なら 150）
    """
    
    # 手数料率が75%以上の場合は、数学的に利益率25%を達成不可能
    if fee_rate >= 0.75:
        return False, None

    # 日本円での目標売価を算出
    target_price_jpy = (cost_jpy + shipping_jpy) / (0.75 - fee_rate)
    
    # 目標売価を現地通貨（USDやCAD）に変換
    target_price_foreign = target_price_jpy / exchange_rate
    
    # 最安値の1.25倍の価格を算出
    max_allowed_price = lowest_price_foreign * 1.25
    
    # 判定
    is_profitable = target_price_foreign <= max_allowed_price
    
    return is_profitable, target_price_foreign

# 使用例
cost = 3000        # 仕入値 3000円
shipping = 1500    # 送料 1500円
fee_rate = 0.15    # 手数料率 15%
lowest_price = 45  # 最安値 45ドル
exchange_rate = 150 # 1ドル150円の場合

is_profitable, target_price = check_export_profitability(cost, shipping, fee_rate, lowest_price, exchange_rate)

print(f"利益確保可能か: {is_profitable}")
print(f"目標売価(現地通貨): {target_price:.2f}")