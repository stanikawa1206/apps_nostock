from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    print("[INFO] 手動で起動された実Chrome(ポート9222)に接続します...")
    browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
    context = browser.contexts[0]
    page = context.pages[0]

    print("\n" + "!" * 60)
    print(" 【手動操作の手順】")
    print(" 1. 自分で起動したChrome側で、ログインを完了させてください。")
    print(" 2. 拡張機能が正常に動いて【出品日時などが画面に表示されている商品ページ】を開いてください。")
    print(" 3. 画面に日時が見えたら、この画面に戻って【Enterキー】を押してください。")
    print("!" * 60 + "\n")

    input("👉 画面に拡張機能の日時が表示されたら、ここをクリックしてEnter...")

    print("\n[INFO] 拡張機能が画面に書き込んだHTMLの要素をスキャンします...")

    try:
        # 商品ページのメイン部分（例: 商品名や商品説明の周辺）のHTMLを切り取って表示
        # 拡張機能がどこに文字を埋め込んだかをあぶり出します
        
        # まずは手始めに、商品タイトル周辺のHTMLを取得してみる
        # (ラクマの仕様に合わせて調整する骨組みです)
        html_snapshot = page.evaluate("""
            () => {
                // 商品の基本情報が入っているエリア（適宜変更される可能性あり）
                const itemInfo = document.querySelector('.item-info__description') || document.body;
                return itemInfo.innerHTML;
            }
        """)
        
        # 拡張機能が追加した「created_at」などの文字がこの中に残っているか確認するためのファイル保存
        output_path = "d:/apps_nostock/apps/publish/ext_dom_snapshot.txt"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_snapshot)
            
        print(f"[SUCCESS] 画面の文字データを保存しました ➡️ {output_path}")
        print("[INFO] このファイルを開いて、拡張機能が表示してくれている『日時』の文字がどこにあるか探してください。")

    except Exception as e:
        print(f"[ERROR] 例外発生: {e}")