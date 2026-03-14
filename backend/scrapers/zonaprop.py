"""
scrapers/zonaprop.py — Fix barrio específico.
"""
import hashlib, re
from typing import Generator
from scrapers.base import BaseScraper

URLS = {
    "departamento":    "https://www.zonaprop.com.ar/departamentos-alquiler-cordoba.html",
    "casa":            "https://www.zonaprop.com.ar/casas-alquiler-cordoba.html",
    "habitacion":      "https://www.zonaprop.com.ar/habitaciones-alquiler-cordoba.html",
    "local_comercial": "https://www.zonaprop.com.ar/locales-alquiler-cordoba.html",
}

class ZonapropScraper(BaseScraper):
    SOURCE_NAME = "zonaprop"
    BASE_URL    = "https://www.zonaprop.com.ar"
    MAX_PAGES   = 5

    def scrape(self) -> Generator[dict, None, None]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.logger.error("[zonaprop] Corré: pip install playwright && playwright install chromium")
            return

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                locale="es-AR", viewport={"width": 1280, "height": 900},
            )
            page = ctx.new_page()
            for prop_type, base_url in URLS.items():
                yield from self._scrape_type(page, prop_type, base_url)
            browser.close()

    def _scrape_type(self, page, prop_type, base_url):
        for page_num in range(1, self.MAX_PAGES + 1):
            url = base_url if page_num == 1 else base_url.replace(".html", f"-pagina-{page_num}.html")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2500)

                cards_html = page.evaluate("""() => {
                    let els = document.querySelectorAll('.postingsList-module__card-container');
                    if (els.length > 0) return Array.from(els).map(e => e.outerHTML);
                    els = document.querySelectorAll('[data-qa="posting PROPERTY"]');
                    return Array.from(els).map(e => e.outerHTML);
                }""")

                if not cards_html:
                    break

                from bs4 import BeautifulSoup
                for html in cards_html:
                    card = BeautifulSoup(html, "lxml").find()
                    if card:
                        item = self._parse(card, prop_type)
                        if item:
                            yield item
            except Exception as e:
                self.logger.error(f"[zonaprop] {url}: {e}")
                break

    def _parse(self, card, prop_type):
        link = card.select_one("[data-to-posting]")
        if not link:
            link = card.select_one("a[href]")
        if not link:
            return None

        href = link.get("data-to-posting") or link.get("href", "")
        full_url = self.BASE_URL + href if href.startswith("/") else href
        external_id = f"zp_{hashlib.md5(full_url.encode()).hexdigest()[:12]}"

        title_el = (
            card.select_one('[data-qa="POSTING_CARD_DESCRIPTION"]') or
            card.select_one("[class*='postingCard-module__posting-description']") or
            card.select_one("h2, h3")
        )
        title = title_el.get_text(strip=True) if title_el else ""

        # Precio: tomar cada hijo del bloque por separado
        price_block = card.select_one("[class*='postingPrices-module__posting-card-price-block']")
        price    = None
        currency = "ARS"
        expenses = None
        if price_block:
            # Buscar elementos de precio hoja (sin hijos precio)
            all_price_candidates = price_block.select("h2, [class*='price-item'], [class*='firstPrice']")
            prices_found = []
            for el in all_price_candidates:
                # saltar si es contenedor de otros precios
                if el.select("h2, [class*='price-item']"):
                    continue
                raw = el.get_text(strip=True)
                p, c = self._normalize_price(raw)
                if p and p < 50_000_000:
                    prices_found.append((p, c))
            if prices_found:
                price, currency = prices_found[0]
            if len(prices_found) > 1:
                expenses = prices_found[1][0]

        # Dirección y barrio
        addr_el = (
            card.select_one('[data-qa="POSTING_CARD_LOCATION"]') or
            card.select_one("[class*='postingCard-module__posting-location']") or
            card.select_one("[class*='location']")
        )
        address = addr_el.get_text(strip=True) if addr_el else ""

        # Barrio: en Zonaprop la dirección suele ser "Calle 123, Barrio, Córdoba"
        parts = [p.strip() for p in address.split(",")]
        if len(parts) >= 2:
            neighborhood = parts[-2] if parts[-1].lower() in ("córdoba","cordoba") else parts[-1]
        else:
            neighborhood = address

        text = card.get_text(" ", strip=True)
        rooms     = self._re_int(text, r"(\d+)\s*amb")
        bedrooms  = self._re_int(text, r"(\d+)\s*dorm")
        bathrooms = self._re_int(text, r"(\d+)\s*ba[ñn]")
        area_m2   = self._re_float(text, r"(\d+[\.,]?\d*)\s*m[²2]")

        img = card.select_one("img[src]")
        thumbnail = img["src"] if img and img.get("src","").startswith("http") else None

        return {
            "external_id": external_id, "source": self.SOURCE_NAME,
            "url": full_url, "title": title, "property_type": prop_type,
            "address": address, "neighborhood": neighborhood,
            "price": price, "currency": currency, "expenses": expenses,
            "rooms": rooms, "bedrooms": bedrooms, "bathrooms": bathrooms,
            "area_m2": area_m2, "thumbnail_url": thumbnail,
        }

    def _re_int(self, text, pattern):
        m = re.search(pattern, text, re.IGNORECASE)
        return int(m.group(1)) if m else None

    def _re_float(self, text, pattern):
        m = re.search(pattern, text, re.IGNORECASE)
        return float(m.group(1).replace(",", ".")) if m else None
