"""
scrapers/local_agencies.py — Scraper para inmobiliarias locales de Córdoba.

Cubre: Cuatro Casas, Interurbana, RE/MAX Córdoba, y otros sitios locales.
Cada agencia tiene su propio método de scraping.
"""

import hashlib
import re
from typing import Generator

from scrapers.base import BaseScraper


class LocalAgenciesScraper(BaseScraper):
    SOURCE_NAME = "local"
    MAX_PAGES   = 3

    def scrape(self) -> Generator[dict, None, None]:
        yield from self._scrape_cuatrocasas()
        yield from self._scrape_interurbana()
        yield from self._scrape_remax_cordoba()

    # ─────────────────────────────────────────────────────────────────
    # Cuatro Casas — https://www.cuatrocasas.com.ar
    # ─────────────────────────────────────────────────────────────────
    def _scrape_cuatrocasas(self) -> Generator[dict, None, None]:
        base_url = "https://www.cuatrocasas.com.ar/alquileres"
        for page in range(1, self.MAX_PAGES + 1):
            url = base_url if page == 1 else f"{base_url}?page={page}"
            soup = self._get(url)
            if not soup:
                break

            cards = soup.select(".property-item") or soup.select("[class*='property']")
            if not cards:
                break

            for card in cards:
                try:
                    link = card.select_one("a[href]")
                    if not link:
                        continue
                    href = link["href"]
                    full_url = "https://www.cuatrocasas.com.ar" + href if href.startswith("/") else href
                    external_id = f"cc_{hashlib.md5(full_url.encode()).hexdigest()[:12]}"

                    title_el = card.select_one("h2, h3, .title")
                    title = title_el.get_text(strip=True) if title_el else ""

                    price_el = card.select_one(".price, [class*='price']")
                    price_str = price_el.get_text(strip=True) if price_el else ""
                    price, currency = self._normalize_price(price_str)

                    location_el = card.select_one(".location, .address, [class*='location']")
                    address = location_el.get_text(strip=True) if location_el else ""
                    neighborhood = address.split(",")[-1].strip() if "," in address else ""

                    prop_type_raw = title.lower()
                    prop_type = self._normalize_property_type(prop_type_raw)

                    feat_text = card.get_text(" ", strip=True)
                    rooms     = self._extract_int(feat_text, r"(\d+)\s*amb")
                    area_m2   = self._extract_area(feat_text)

                    img = card.select_one("img[src]")
                    thumbnail = img["src"] if img else None

                    yield {
                        "external_id":   external_id,
                        "source":        "cuatrocasas",
                        "url":           full_url,
                        "title":         title,
                        "property_type": prop_type,
                        "address":       address,
                        "neighborhood":  neighborhood,
                        "price":         price,
                        "currency":      currency,
                        "rooms":         rooms,
                        "area_m2":       area_m2,
                        "thumbnail_url": thumbnail,
                    }
                except Exception as e:
                    self.logger.warning(f"[cuatrocasas] Error: {e}")

    # ─────────────────────────────────────────────────────────────────
    # Interurbana — https://www.interurbana.com.ar
    # ─────────────────────────────────────────────────────────────────
    def _scrape_interurbana(self) -> Generator[dict, None, None]:
        base_url = "https://www.interurbana.com.ar/alquileres-cordoba"
        for page in range(1, self.MAX_PAGES + 1):
            url = base_url if page == 1 else f"{base_url}/pagina-{page}"
            soup = self._get(url)
            if not soup:
                break

            cards = soup.select(".item-box") or soup.select("[class*='listing']")
            if not cards:
                break

            for card in cards:
                try:
                    link = card.select_one("a[href]")
                    if not link:
                        continue
                    href = link["href"]
                    full_url = "https://www.interurbana.com.ar" + href if href.startswith("/") else href
                    external_id = f"iu_{hashlib.md5(full_url.encode()).hexdigest()[:12]}"

                    title_el = card.select_one("h2, h3, .item-title")
                    title = title_el.get_text(strip=True) if title_el else ""

                    price_el = card.select_one(".item-price, .precio")
                    price_str = price_el.get_text(strip=True) if price_el else ""
                    price, currency = self._normalize_price(price_str)

                    address_el = card.select_one(".item-address, .direccion")
                    address = address_el.get_text(strip=True) if address_el else ""

                    barrio_el = card.select_one(".item-location, .barrio")
                    neighborhood = barrio_el.get_text(strip=True) if barrio_el else ""

                    prop_type = self._normalize_property_type(title)
                    feat_text = card.get_text(" ", strip=True)
                    rooms     = self._extract_int(feat_text, r"(\d+)\s*amb")
                    area_m2   = self._extract_area(feat_text)

                    img = card.select_one("img[src]")
                    thumbnail = img["src"] if img else None

                    yield {
                        "external_id":   external_id,
                        "source":        "interurbana",
                        "url":           full_url,
                        "title":         title,
                        "property_type": prop_type,
                        "address":       address,
                        "neighborhood":  neighborhood,
                        "price":         price,
                        "currency":      currency,
                        "rooms":         rooms,
                        "area_m2":       area_m2,
                        "thumbnail_url": thumbnail,
                    }
                except Exception as e:
                    self.logger.warning(f"[interurbana] Error: {e}")

    # ─────────────────────────────────────────────────────────────────
    # RE/MAX Córdoba — https://www.remax.com.ar
    # ─────────────────────────────────────────────────────────────────
    def _scrape_remax_cordoba(self) -> Generator[dict, None, None]:
        base_url = "https://www.remax.com.ar/listings/rent?locationId=AR.X009&pageNumber=1&pageSize=24"
        for page in range(1, self.MAX_PAGES + 1):
            url = base_url.replace("pageNumber=1", f"pageNumber={page}")
            soup = self._get(url)
            if not soup:
                break

            cards = soup.select(".listing-card") or soup.select("[class*='listing']")
            if not cards:
                break

            for card in cards:
                try:
                    link = card.select_one("a[href]")
                    if not link:
                        continue
                    href = link["href"]
                    full_url = "https://www.remax.com.ar" + href if href.startswith("/") else href
                    external_id = f"rmx_{hashlib.md5(full_url.encode()).hexdigest()[:12]}"

                    title_el = card.select_one(".listing-card__title, h2, h3")
                    title = title_el.get_text(strip=True) if title_el else ""

                    price_el = card.select_one(".listing-card__price, [class*='price']")
                    price_str = price_el.get_text(strip=True) if price_el else ""
                    price, currency = self._normalize_price(price_str)

                    location_el = card.select_one(".listing-card__location, [class*='location']")
                    address = location_el.get_text(strip=True) if location_el else ""
                    neighborhood = address.split(",")[0].strip() if "," in address else address

                    prop_type = self._normalize_property_type(title)
                    feat_text = card.get_text(" ", strip=True)
                    rooms     = self._extract_int(feat_text, r"(\d+)\s*amb")
                    bedrooms  = self._extract_int(feat_text, r"(\d+)\s*dorm")
                    bathrooms = self._extract_int(feat_text, r"(\d+)\s*ba[ñn]")
                    area_m2   = self._extract_area(feat_text)

                    img = card.select_one("img[src]")
                    thumbnail = img["src"] if img else None

                    yield {
                        "external_id":   external_id,
                        "source":        "remax",
                        "url":           full_url,
                        "title":         title,
                        "property_type": prop_type,
                        "address":       address,
                        "neighborhood":  neighborhood,
                        "price":         price,
                        "currency":      currency,
                        "rooms":         rooms,
                        "bedrooms":      bedrooms,
                        "bathrooms":     bathrooms,
                        "area_m2":       area_m2,
                        "thumbnail_url": thumbnail,
                    }
                except Exception as e:
                    self.logger.warning(f"[remax] Error: {e}")

    # ─── Helpers ──────────────────────────────────────────────────────
    def _extract_int(self, text: str, pattern: str) -> int | None:
        m = re.search(pattern, text, re.IGNORECASE)
        return int(m.group(1)) if m else None

    def _extract_area(self, text: str) -> float | None:
        m = re.search(r"(\d+[\.,]?\d*)\s*m[²2]", text, re.IGNORECASE)
        return float(m.group(1).replace(",", ".")) if m else None
