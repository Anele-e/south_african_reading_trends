import sqlite3
import json


def get_table_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT product_id, response_json FROM raw_books")
    rows = cursor.fetchall()

    for product_id, response_json in rows:
        data = json.loads(response_json)

        title = data.get("title")
        authors = data.get("core", {}).get("authors", [])
        author_name = authors[0].get("Author") if authors else None

        rating = data.get("core", {}).get("star_rating")
        rating_count = data.get("core", {}).get("reviews")

        items = data.get("buybox", {}).get("items", [])
        price = items[0].get("pretty_price") if items else None

        breadcrumbs = data.get("breadcrumbs", {}).get("items", [])
        breadcrumb = " > ".join(b.get("name") for b in breadcrumbs if b.get("name"))

        languages = None
        publisher = None
        isbn = None
        book_format = None

        for item in data.get("product_information", {}).get("items", []):
            name = item.get("display_name")
            value = item.get("displayable_text")

            if name == "Languages":
                languages = value
            elif name == "Publisher":
                publisher = value
            elif name == "Barcode":
                isbn = value
            elif name == "Book Format":
                book_format = value
       
        scrape_date = data.get("meta", {}).get("date_retrieved")

    cursor.execute("""
    INSERT OR IGNORE INTO authors (name)
    VALUES (?)
    """, (author_name,))

    cursor.execute("""
    SELECT id FROM authors
    WHERE name = ?
    """, (author_name,))

    author_id = cursor.fetchone()[0]

    cursor.execute("""
    INSERT INTO books (
    title,
    author_id,
    price,
    rating,
    rating_count,
    publisher,
    isbn,
    language,
    category,
    scrape_date
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
    title,
    author_id,
    price,
    rating,
    rating_count,
    publisher,
    isbn,
    languages,
    breadcrumb,
    scrape_date
))
    
    conn.commit()

