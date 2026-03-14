"""
scheduler.py — Scraping automático usando solo threading + time.
Sin APScheduler, sin dependencias externas.
"""

import logging
import time
import threading

import database as db_module
from scrapers import ALL_SCRAPERS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("scheduler")

INTERVAL_HOURS = 4


def run_all():
    logger.info("=== Iniciando ciclo de scraping ===")
    for ScraperClass in ALL_SCRAPERS:
        scraper = ScraperClass()
        try:
            result = scraper.run()
            logger.info(f"✓ {result}")
        except Exception as e:
            logger.error(f"✗ {ScraperClass.__name__}: {e}")
    logger.info("=== Ciclo finalizado ===")


def loop():
    while True:
        run_all()
        logger.info(f"Próxima ejecución en {INTERVAL_HOURS} horas")
        time.sleep(INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    db_module.init_db()
    t = threading.Thread(target=loop, daemon=False)
    t.start()
    t.join()
