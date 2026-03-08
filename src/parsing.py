import requests
import sqlite3
import random
import time
import json
import re


AUTHORS_TO_IGNORE =  {'Various', 'Anonymous', 'Unknown', 'Multi-user', 'Staff'}
NORMALIZE = {
    "Kingdom of Great Britain": "United Kingdom",
    "United Kingdom of Great Britain and Ireland": "United Kingdom",
    "British Raj": "India",
    "Russian Empire": "Russia",
    "South African": "South Africa",
    "American": "United States",
    "British": "United Kingdom",
    "Scottish": "United Kingdom",
    "Irish": "Ireland",
    "Australian": "Australia",
    "Canadian": "Canada",
    "Indian": "India"
}

session = requests.Session()
session.headers.update({
    "User-Agent": "SouthAfricanReadingTrends/1.0 (contact: aneledindili4@gmail.com)"
})

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def fetch_author_extracts(authors):
    if not authors:
        return None
    url = "https://en.wikipedia.org/w/api.php"

    params = {
        "action": "query",
        "format": "json",
        "titles": "|".join(authors),
        "prop": "pageprops|extracts",
        "ppprop": "wikibase_item",
        "exintro": True,
        "explaintext": True,
        "formatversion": "2"
    }

    try:
        response = session.get(url, params=params).json()
    except Exception:
        return {}
    mapping = {}
    for page in response["query"]["pages"]:
        title = page["title"]
        qid = page.get("pageprops", {}).get("wikibase_item")
        intro = page.get("extract")
        
        mapping[title] = {"qid": qid, 
                          "intro": intro
        }
    print(mapping)
    return mapping


def fetch_wikidata_entities(qids):
    if not qids:
        return {}
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{'|'.join(qids)}.json"

    try:
        r = session.get(url).json()
    except Exception:
        return {}

    return r.get("entities", {})

def extract_nationality_qids(entities):
    author_nationalities = {}
    country_qids = set()
    birth_places = {}
    place_qids = set()

    for qid, entity in entities.items():

        claims = entity.get("claims", {})

        if "P27" in claims:
            try:
                country_qid = claims["P27"][0]["mainsnak"]["datavalue"]["value"]["id"]
                author_nationalities[qid] = country_qid
                country_qids.add(country_qid)
                continue
            except Exception:
                pass


        if "P19" in claims:
            try:
                place_qid = claims["P19"][0]["mainsnak"]["datavalue"]["value"]["id"]
                birth_places[qid] = place_qid
                place_qids.add(place_qid)
            except Exception:
                pass

    return author_nationalities, birth_places, list(country_qids), list(place_qids)

def fetch_country_labels(country_qids):
    if not country_qids:
        return {}

    url = f"https://www.wikidata.org/wiki/Special:EntityData/{'|'.join(country_qids)}.json"

    try:
        r = session.get(url).json()
    except Exception:
        return {}

    entities = r.get("entities", {})

    labels = {}

    for qid, entity in entities.items():

        label = entity.get("labels", {}).get("en", {}).get("value")

        if label:
            labels[qid] = label

    return labels

def parse_intro_nationality(text):
    if not text:
        return None

    match = re.search(
        r"\b(South African|American|British|Australian|Canadian|Indian|Irish|Scottish)\b",
        text
    )

    if match:
        return match.group(1)

    return None


def resolve_place_to_country(place_qids):

    if not place_qids:
        return {}

    entities = fetch_wikidata_entities(place_qids)

    place_to_country = {}

    for qid, entity in entities.items():

        claims = entity.get("claims", {})

        if "P17" in claims:
            try:
                country_qid = claims["P17"][0]["mainsnak"]["datavalue"]["value"]["id"]
                place_to_country[qid] = country_qid
            except Exception:
                pass

    return place_to_country
   

def parse_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT product_id, response_json FROM raw_books WHERE status_code = 200")
    rows = cursor.fetchall()

    for product_id, response_json in rows:
        try:
            data = json.loads(response_json)
        except Exception:
            continue

        title = data.get("title")
        authors = data.get("core", {}).get("authors", [])
        author_name = authors[0].get("Author") if authors else None

        rating = data.get("core", {}).get("star_rating")
        rating_count = data.get("core", {}).get("reviews")

        items = data.get("buybox", {}).get("items", [])
        price = items[0].get("price") if items else None
        if price:
            price = float(price)

        breadcrumbs = data.get("breadcrumbs", {}).get("items", [])
        breadcrumb = " > ".join(b.get("name") for b in breadcrumbs if b.get("name")) if breadcrumbs else None

        if not breadcrumb or not breadcrumb.startswith("Books > Books >"):
            continue

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


        author_id = None
        if author_name:
            cursor.execute("""
            INSERT OR IGNORE INTO authors (name)
            VALUES (?)
            """, (author_name,))

            cursor.execute("""
            SELECT id FROM authors
            WHERE name = ?
            """, (author_name,))

            row = cursor.fetchone()
            if row:
                author_id = row[0]

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



def enrich_authors_table(conn):
    cursor = conn.cursor()

    cursor.execute("""
    SELECT name FROM authors
    WHERE nationality IS NULL
    """)


    authors = [row[0] for row in cursor.fetchall() if row[0] not in AUTHORS_TO_IGNORE]
    for batch in chunk(authors, 20):
        wiki_data = fetch_author_extracts(batch)
      

        if not wiki_data:
            continue

        qids = [v["qid"] for v in wiki_data.values() if v["qid"]]

        entities = fetch_wikidata_entities(qids)
        author_country, birth_places, country_qids, place_qids = extract_nationality_qids(entities)

        place_to_country = resolve_place_to_country(place_qids)
        for author_qid, place_qid in birth_places.items():
            country_qid = place_to_country.get(place_qid)

            if country_qid:
                author_country[author_qid] = country_qid
                country_qids.append(country_qid)
        

        country_labels = fetch_country_labels(country_qids)

        
        for author_name, data in wiki_data.items():
            if len(author_name.split()) < 2:
                continue
            qid = data["qid"]
            intro = data["intro"]
            nationality = None

            country_qid = author_country.get(qid)
            nationality = country_labels.get(country_qid)
            if author_name == "Rorisang Thandekiso":
                print(country_qid)
                print(nationality)
                print(author_country)
                print(country_qids)
            
            if not nationality:
                nationality = parse_intro_nationality(intro)
                print(nationality, 2)

            if nationality:
                nationality = NORMALIZE.get(nationality, nationality)
           
                cursor.execute("""
                    UPDATE authors
                    SET nationality = ?
                    WHERE name = ?
                """, (nationality, author_name))
    
            
        conn.commit()
        print(f"Processed batch of {len(batch)} authors...")
        time.sleep(1)

    

