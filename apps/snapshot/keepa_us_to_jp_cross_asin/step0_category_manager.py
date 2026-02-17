# step0_category_manager.py
from my_utils import get_sql_server_connection
from datetime import datetime

def get_next_category():
    """未取得かつ対象のカテゴリを1つ取得する (SELECTのみ)"""
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    # 取得対象かつ未取得(NULL)のものを1つ抽出
    cursor.execute("""
        SELECT TOP 1 category_id, category_name 
        FROM mst.amazon_category 
        WHERE is_at_least_one_asin_exists = 1 AND fetched_at IS NULL
        ORDER BY category_id ASC
    """)
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {"id": row[0], "name": row[1]}
    return None

def update_category_fetched_at(category_id):
    """指定されたカテゴリIDの取得日を今日の日付で更新する"""
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        cursor.execute("""
            UPDATE mst.amazon_category 
            SET fetched_at = ? 
            WHERE category_id = ?
        """, (today, category_id))
        conn.commit()
    except Exception as e:
        print(f"!!! [Step 0] DB更新エラー (ID: {category_id}): {e}")
        conn.rollback()
    finally:
        conn.close()