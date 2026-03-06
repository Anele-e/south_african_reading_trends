from db import init_db, already_scraped
from sitemap import get_book_sitemaps, get_urls_from_sitemaps
from ingest import ingest_product
from parsing import get_table_data
from config import DATABASE_PATH, REQUEST_DELAY, MAX_RETRIES
from retry import get_retries, calculate_wait_time
import time
import random

import sqlite3


def run():
    init_db()
    conn = sqlite3.connect(DATABASE_PATH)
    sitemaps = get_book_sitemaps()

    for sitemap in sitemaps[:2]:
        urls = get_urls_from_sitemaps(sitemap, limit=200)

        for i, url in enumerate(urls):
            plid = url.split("/")[-1]
            if already_scraped(conn, plid):
                continue
            ingest_product(plid, conn)
            

            if (i+1) % 50 == 0:
                conn.commit()
            time.sleep(random.uniform(1,2))
    conn.commit()
    

    retries = get_retries()

    for row_id, product_id, status_code, retry_after, retry_count in retries:
        wait_time = calculate_wait_time(status_code, retry_count, retry_after)
        print(f"Retrying {product_id} in {wait_time}s")
        time.sleep(wait_time)
        ingest_product(plid=product_id, conn=conn)

        conn.execute("""
        UPDATE raw_books
        SET retry_count = retry_count + 1
        WHERE id = ?
        """, (row_id,))
    get_table_data(conn=conn)
    
    
    conn.close()

if __name__ == "__main__":
    run()
