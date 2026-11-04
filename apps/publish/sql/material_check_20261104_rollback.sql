-- material_check_20261104.sql の取り消し。
-- 1) 先にコードを git revert で戻す（新しいコードは trx.material_check を参照する）。
-- 2) 候補抽出関数を変更前に戻す: E:\backup\sqlserver\material_check_20261104\fn_take_one_candidates_base_before.sql を実行する。
--    （保留(素材確認) の商品は候補に戻る。出品させたくない場合は先に 出品状況 を NG(危険素材) に変える）
-- 3) 判定結果の表を削除する（必要なら先に内容を保存する）。
IF OBJECT_ID('trx.material_check', 'U') IS NOT NULL
    DROP TABLE trx.material_check;
