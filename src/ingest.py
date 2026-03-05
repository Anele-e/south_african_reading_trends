import requests
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO
import random
import time
import sqlite3
from datetime import datetime
import json



def ingest_product(plid, conn):
    url = f"https://api.takealot.com/rest/v-1-16-0/product-details/{plid}?platform=desktop&display_credit=true"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Origin": "https://www.takealot.com",
        "Referer": "https://www.takealot.com/"
    }

    try:
        r = requests.get(url, headers=headers, timeout=10)
        retry_after = r.headers.get("Retry-After")
        retry_after = int(retry_after) if retry_after else None
        status_code = r.status_code
        try:
            response_json = json.dumps(r.json())
        except ValueError:
            response_json = None

        error_message = None
    except Exception as e:
        status_code = None
        response_json = None
        error_message = str(e)

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO raw_books (
            product_id,
            request_url,
            status_code,
            response_json,
            retry_after,    
            error_message,
            scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        plid,
        url,
        status_code,
        response_json,
        retry_after,
        error_message,
        datetime.utcnow().isoformat()
    ))

   
    

    


    
