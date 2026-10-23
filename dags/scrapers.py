"""Scrapers for 3 sources: IHAVECPU (Next.js JSON), JIB (HTML), Notebookspec (HTML).

All scrapers respect robots.txt (no /api, /jibapi, /go-to-shop/ ...) and sleep between requests.
Only product name / price are collected - no personal data.
"""
import json
import os
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (KU student project; Airflow DAG)"}


def _get(url, delay=2.0):
    time.sleep(delay)
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r


def _now():
    return datetime.now(timezone.utc).isoformat()


def _price(text):
    m = re.search(r"[0-9][0-9,]*(?:\.[0-9]+)?", text or "")
    return float(m.group(0).replace(",", "")) if m else None


# ---------------------------------------------------------------- IHAVECPU
def _find_products(obj):
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and "price_sale" in obj[0] and "name_th" in obj[0]:
            return obj
        for v in obj:
            r = _find_products(v)
            if r:
                return r
    elif isinstance(obj, dict):
        for v in obj.values():
            r = _find_products(v)
            if r:
                return r
    return []


def scrape_ihavecpu(max_pages=4):
    rows = []
    for slug, category in (("storage", "SSD"), ("ram", "RAM")):
        for page in range(1, max_pages + 1):
            url = f"https://www.ihavecpu.com/category/{slug}" + (f"?page={page}" if page > 1 else "")
            html = _get(url).text
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
            if not m:
                break
            items = _find_products(json.loads(m.group(1))["props"]["pageProps"])
            if not items:
                break
            for it in items:
                price = _price(str(it.get("price_sale")))
                rows.append({
                    "source": "ihavecpu", "shop": "ihavecpu", "category": category,
                    "name": it["name_th"].strip(), "price": price,
                    "url": f"https://www.ihavecpu.com/product/{it.get('product_id')}",
                    "scraped_at": _now(),
                })
    return rows


# --------------------------------------------------------------------- JIB
JIB_PAGES = [("3/1471", "SSD"), ("3/1028", "SSD"), ("2/53", "RAM")]


def scrape_jib():
    rows = []
    for path, category in JIB_PAGES:
        soup = BeautifulSoup(_get(f"https://www.jib.co.th/web/product/product_list/{path}").text, "html.parser")
        seen = set()
        for a in soup.select("a[href*=readProduct][title]"):
            href = a["href"]
            if href in seen:
                continue
            seen.add(href)
            card = a
            for _ in range(6):
                card = card.parent
                if card is None or card.select_one(".price_total"):
                    break
            if card is None or card.select_one(".price_total") is None:
                continue
            rows.append({
                "source": "jib", "shop": "jib", "category": category,
                "name": a["title"].strip(), "price": _price(card.select_one(".price_total").get_text()),
                "url": href, "scraped_at": _now(),
            })
    return rows


# ------------------------------------------------------------- Notebookspec
def scrape_notebookspec(pages_per_category=None):
    n = pages_per_category or int(os.getenv("NSPEC_PAGES_PER_CATEGORY", "25"))
    rows = []
    for sitemap, category in (("ssd", "SSD"), ("ram", "RAM")):
        xml = _get(f"https://notebookspec.com/sitemaps/{sitemap}.xml", delay=3).text
        urls = re.findall(r"<loc>([^<]+)</loc>", xml)[-n:]  # newest products
        for url in urls:
            soup = BeautifulSoup(_get(url, delay=3).text, "html.parser")  # Crawl-delay: 3
            h1 = soup.h1.get_text(strip=True) if soup.h1 else None
            if not h1:
                continue
            for li in soup.select("li.shop-item"):
                btn = li.select_one("a.button")
                if btn is None or "soldout" in (btn.get("class") or []):
                    continue
                img = li.select_one(".brand img")
                shop = re.sub(r".*component-shop-|\.\w+$", "", img["src"]) if img and img.get("src") else "unknown"
                price = _price(li.select_one(".price").get_text() if li.select_one(".price") else "")
                rows.append({
                    "source": "notebookspec", "shop": shop, "category": category,
                    "name": h1, "price": price, "url": url, "scraped_at": _now(),
                })
    return rows


SCRAPERS = {"ihavecpu": scrape_ihavecpu, "jib": scrape_jib, "notebookspec": scrape_notebookspec}
