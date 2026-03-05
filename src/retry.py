import sqlite3
from config import REQUEST_DELAY, MAX_RETRIES, DATABASE_PATH

conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

cursor.execute("""
    SELECT product_id, status_code
    FROM raw_books
    WHERE status_code IN (429, 500)
    OR status_code IS NULL;
"""
)


