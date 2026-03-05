import sqlite3
from config import DATABASE_PATH

def init_db():
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("PRAGMA foreign_keys = ON")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT,
        request_url TEXT,
        status_code INTEGER,
        response_json TEXT,
        retry_after INTEGER,
        error_message TEXT,
        retry_count INTEGER DEFAULT 0, 
        scraped_at TEXT
    );
    """)

    conn.commit()
    conn.close()

def already_scraped(conn, plid):
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT 1
          FROM raw_books
            WHERE product_id=? AND status_code = 200 
                LIMIT 1""",
        (plid,)
    )

    return cursor.fetchone() is not None

