import sqlite3
from config import REQUEST_DELAY, MAX_RETRIES, DATABASE_PATH


def get_retries():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, product_id, status_code, retry_after, retry_count
        FROM raw_books
        WHERE (
            status_code IN (429, 500)
            OR status_code IS NULL
            )
        AND retry_count < ?
    """, (MAX_RETRIES,)
    )

    rows = cursor.fetchall()
    conn.close()

    return rows

def calculate_wait_time(status_code, retry_count, retry_after):
    if status_code == 429:
        if retry_after:
            return int(retry_after)
        return 2 ** retry_count
    
    if status_code == 500:
        return 2 ** retry_count
    
    return 1


