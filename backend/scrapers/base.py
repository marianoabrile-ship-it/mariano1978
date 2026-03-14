"""
scrapers/base.py — Clase base. Solo usa httpx + beautifulsoup4.
"""

import logging
import random
import re
import time
from abc import ABC, abstractmethod
from typing import Generator

import httpx
from bs4 import BeautifulSoup

import database as db_module

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

PROPERTY_TYPE_MAP = {
    "departamento": "departamento", "depto": "departamento", "ph": "departamento",
    "casa": "casa", "chalet": "casa", "quinta": "casa",
    "habitacion": "habitacion", "habitación": "habitacion",
    "pension": "habitacion", "pensión": "habitacion", "cuarto": "habitacion",
    "local": "local_comercial", "comercial": "local_comercial",
    "oficina": "local_comercial", "galpon": "local_comercial", "galpón": "local_comercial",
}


class BaseScraper(ABC):
    SOURCE_NAME: str = ""
    BASE_URL:    str = ""
    DELAY_MIN:   float = 1.5
    DELAY_MAX:   float = 3.5

    def __init__(self):
        self.client = httpx.Client(
            headers={"User-Agent": random.choice(USER_AGENTS)},
            follow_redirects=True, timeout=30.0,
        )
        self.logger = logging.getLogger(f"scraper.{self.SOURCE_NAME}")

    def _delay(self):
        time.sleep(random.uniform(self.DELAY_MIN, self.DELAY_MAX))

    def _get(self, url: str):
        try:
            self._delay()
            resp = self.client.get(url)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None

    def _normalize_price(self, s: str) -> tuple:
        if not s:
            return None, "ARS"
        s = s.strip().replace("\xa0", " ")
        currency = "USD" if "USD" in s.upper() or "U$S" in s.upper() else "ARS"
        digits = "".join(c for c in s if c.isdigit() or c in ".,").replace(".", "").replace(",", ".")
        try:
            return float(digits), currency
        except ValueError:
            return None, currency

    def _normalize_property_type(self, raw: str) -> str:
        raw = raw.lower().strip()
        for key, val in PROPERTY_TYPE_MAP.items():
            if key in raw:
                return val
        return "otro"

    def _normalize_area(self, s: str):
        if not s:
            return None
        digits = "".join(c for c in str(s) if c.isdigit() or c == ".")
        try:
            return float(digits)
        except ValueError:
            return None

    def _normalize_int(self, s):
        if s is None:
            return None
        digits = "".join(c for c in str(s) if c.isdigit())
        return int(digits) if digits else None

    @abstractmethod
    def scrape(self) -> Generator[dict, None, None]:
        pass

    def run(self) -> dict:
        log_id = db_module.log_start(self.SOURCE_NAME)
        found = new = updated = 0
        error = None
        try:
            self.logger.info(f"[{self.SOURCE_NAME}] Iniciando...")
            for data in self.scrape():
                found += 1
                is_new = db_module.upsert_listing(data)
                if is_new:
                    new += 1
                else:
                    updated += 1
            self.logger.info(f"[{self.SOURCE_NAME}] ✓ {found} encontradas, {new} nuevas")
        except Exception as e:
            self.logger.exception(f"[{self.SOURCE_NAME}] Error: {e}")
            error = str(e)
        finally:
            db_module.log_finish(log_id, found, new, updated, error is None, error)
        return {"source": self.SOURCE_NAME, "found": found, "new": new, "updated": updated, "error": error}
