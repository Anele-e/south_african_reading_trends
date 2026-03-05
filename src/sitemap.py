import requests
import xml.etree.ElementTree as ET
import gzip
from io import BytesIO
from config import SITEMAP_INDEX_URL



def get_book_sitemaps():
    response = requests.get(SITEMAP_INDEX_URL)
    root = ET.fromstring(response.content)

    namespace = root.tag.split("}")[0].strip("{")

    ns = {"ns": namespace}
    sitemap_urls = [
        sitemap.find("ns:loc", ns).text
        for sitemap in root.findall("ns:sitemap", ns)
        ]


    book_sitemaps = [url for url in sitemap_urls if "sitemap_book_" in url.lower()]
    return book_sitemaps

def get_urls_from_sitemaps(sitemap, limit=None):
    response = requests.get(sitemap)
    root = ET.fromstring(response.content)

    namespace = root.tag.split("}")[0].strip("{")
    ns = {"ns": namespace}

    urls = [
        url.find("ns:loc", ns).text
        for url in root.findall("ns:url", ns)
    ]

    if limit:
        return urls[:limit]

    return urls

