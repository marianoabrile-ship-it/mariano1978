"""
test_scrapers.py — Prueba los 3 scrapers con Playwright.
Correr: python3 test_scrapers.py
"""
import logging
import sys
import database as db_module
from scrapers.mercadolibre import MercadoLibreScraper
from scrapers.zonaprop import ZonapropScraper
from scrapers.argenprop import ArgenpropScraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
db_module.init_db()

def test(ScraperClass, limit=3):
    name = ScraperClass.SOURCE_NAME
    print(f"\n{'='*50}")
    print(f"  {name.upper()}")
    print(f"{'='*50}")
    scraper = ScraperClass()
    count = 0
    try:
        for item in scraper.scrape():
            count += 1
            print(f"  [{count}] {item.get('title','')[:60]}")
            print(f"       Barrio : {item.get('neighborhood') or item.get('address','')[:40]}")
            print(f"       Precio : {item.get('currency')} {item.get('price')}")
            if count >= limit:
                print(f"  ... (mostrando primeros {limit})")
                break
    except Exception as e:
        print(f"  ERROR: {e}")
    print(f"  → {'✅ ' + str(count) + '+ resultados' if count else '❌ Sin resultados'}")

# Solo el que se pasa como argumento, o todos
scraper_arg = sys.argv[1] if len(sys.argv) > 1 else "all"

if scraper_arg in ("ml", "mercadolibre", "all"):
    test(MercadoLibreScraper)
if scraper_arg in ("zp", "zonaprop", "all"):
    test(ZonapropScraper)
if scraper_arg in ("ap", "argenprop", "all"):
    test(ArgenpropScraper)

print("\nUso: python3 test_scrapers.py [ml|zp|ap|all]")
