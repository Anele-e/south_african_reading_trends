from db import init_db, already_scraped
from sitemap import get_book_sitemaps, get_urls_from_sitemaps
from ingest import ingest_product
from config import DATABASE_PATH
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
    conn.close()

if __name__ == "__main__":
    run()
