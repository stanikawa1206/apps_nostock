# step0_category_manager.py
from my_utils import get_sql_server_connection
from datetime import datetime

def get_next_category(target_min_rank, target_max_rank):
    """
    指定されたランキング閾値とDBの値が一致しない、
    かつ is_at_least_one_asin_exists = 1 のカテゴリを1つ取得する。
    取得日は参照しない。
    """
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    query = """
        SELECT TOP 1 category_id, category_name 
        FROM mst.amazon_category 
        WHERE is_at_least_one_asin_exists = 1 
          AND (
              min_rank_threshold IS NULL OR 
              max_rank_threshold IS NULL OR 
              min_rank_threshold != ? OR 
              max_rank_threshold != ?
          )
        ORDER BY category_id ASC
    """
    cursor.execute(query, (target_min_rank, target_max_rank))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {"id": row[0], "name": row[1]}
    return None

def update_category_status(category_id, min_rank, max_rank):
    """指定されたカテゴリIDの取得日とランキング閾値を更新する"""
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        cursor.execute("""
            UPDATE mst.amazon_category 
            SET fetched_at = ?,
                min_rank_threshold = ?,
                max_rank_threshold = ?
            WHERE category_id = ?
        """, (today, min_rank, max_rank, category_id))
        conn.commit()
    except Exception as e:
        print(f"!!! [Step 0] DB更新エラー (ID: {category_id}): {e}")
        conn.rollback()
    finally:
        conn.close()