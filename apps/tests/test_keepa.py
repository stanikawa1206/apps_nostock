#!/usr/bin/env python3
"""
Keepa商品情報取得テストプログラム
指定されたASINの商品情報を取得して表示します
"""
import json
from utils import fetch_keepa_product_snapshot
def main():
    # テスト対象のASIN
    test_asin = "B071W13D4C"
    
    print("=" * 80)
    print(f"Keepa商品情報取得テスト: {test_asin}")
    print("=" * 80)
    print()
    
    # Keepa APIを呼び出し
    result = fetch_keepa_product_snapshot(
        asin=test_asin,
        domain="JP"
    )
    
    print(f"ASIN: {result.get('asin')}")
    print(f"タイトル: {result.get('title')}")
    print(f"ブランド: {result.get('brand')}")
    print(f"UPC: {result.get('upc')}")
    print(f"カテゴリ: {result.get('category_path')}")
    print(f"親ASIN: {result.get('parent_asin')}")
    print(f"成人向け: {result.get('is_adult_product')}")  # 【新規追加】
    print()
    
    print(f"バイボックス価格（JPY）: {result.get('buybox_price_jpy')}")
    print(f"バイボックス出品者ID: {result.get('buybox_seller_id')}")
    print(f"バイボックス - FBA: {result.get('buybox_is_fba')}")
    print(f"バイボックス - Amazon: {result.get('buybox_is_amazon')}")
    print(f"バイボックス - バックオーダー: {result.get('buybox_is_backorder')}")
    # 【削除】buybox_is_preorder
    # 【削除】buybox_is_shippable
    print(f"バイボックス - 在庫メッセージ: {result.get('buybox_availability_message')}")
    print(f"新品価格（JPY）: {result.get('new_current_price')}")
    print()
    
    print(f"新品出品者数: {result.get('count_new_total')}")
    print(f"  FBA: {result.get('count_new_fba')}")
    print(f"  FBM: {result.get('count_new_fbm')}")
    print(f"中古出品者数: {result.get('count_used_total')}")
    print(f"  FBA: {result.get('count_used_fba')}")
    print(f"  FBM: {result.get('count_used_fbm')}")
    print(f"月間販売数: {result.get('monthly_sold')}")
    print(f"送料無料対象: {result.get('super_saver_shipping')}")
    print(f"手数料率（%）: {result.get('referral_fee_percentage')}")
    print()
    
    print(f"セールスランク: {result.get('current_sales_rank')}")
    print(f"評価スコア: {result.get('current_rating')}")
    print(f"レビュー数: {result.get('current_review_count')}")
    print(f"OOS90日率（新品）: {result.get('oos90_new_pct')}%")
    print()
    
    print(f"重量（g）: {result.get('weight_g')}")
    print(f"長さ（mm）: {result.get('length_mm')}")
    print(f"幅（mm）: {result.get('width_mm')}")
    print(f"高さ（mm）: {result.get('height_mm')}")
    print(f"FBA手数料: {result.get('fba_fee')}")
    print(f"発売日: {result.get('release_date')}")
    print()
    
    print(f"最終更新時刻: {result.get('last_update')}")
    print(f"最終価格変更時刻: {result.get('last_price_change')}")
    print()
    
    print(f"画像数: {len(result.get('images', []))}")
    if result.get('images'):
        print("画像URL:")
        for i, img_url in enumerate(result.get('images', []), 1):
            print(f"  {i}. {img_url}")
    print()
    
    print(f"取得日時: {result.get('checked_at')}")
    print()
    
    # ========== JSON形式でも出力（デバッグ用） ==========
    #print("=" * 80)
    #print("【完全な結果（JSON形式）】")
    #print("=" * 80)
    #print(json.dumps(result, indent=2, ensure_ascii=False))
if __name__ == "__main__":
    main()