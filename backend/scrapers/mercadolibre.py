"""
scrapers/mercadolibre.py — Filtrar Córdoba en el parser.
"""
import hashlib, re
from typing import Generator
from scrapers.base import BaseScraper

CORDOBA_KEYWORDS = {"córdoba", "cordoba", "villa carlos paz", "rio ceballos", "cosquín"}

URLS = {
    "departamento":    "https://inmuebles.mercadolibre.com.ar/departamentos/alquiler/cordoba/",
    "casa":            "https://inmuebles.mercadolibre.com.ar/casas/alquiler/cordoba/",
    "habitacion":      "https://inmuebles.mercadolibre.com.ar/habitaciones/alquiler/cordoba/",
    "local_comercial": "https://inmuebles.mercadolibre.com.ar/locales-y-fondos-de-comercio/alquiler/cordoba/",
}

NON_CORDOBA = {
    "palermo","belgrano","recoleta","almagro","caballito","flores","floresta",
    "villa crespo","boedo","san telmo","montserrat","retiro","balvanera",
    "villa urquiza","coghlan","saavedra","nunez","nuñez","colegiales",
    "paternal","devoto","villa del parque","liniers","mataderos",
    "capital federal","caba","ciudad autonoma",
    "rosario","santa fe","mendoza","la plata","mar del plata","bahia blanca"
}

class MercadoLibreScraper(BaseScraper):
    SOURCE_NAME = "mercadolibre"
    BASE_URL    = "https://inmuebles.mercadolibre.com.ar"
    MAX_PAGES   = 5

    def scrape(self) -> Generator[dict, None, None]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.logger.error("[mercadolibre] Corré: pip install playwright && playwright install chromium")
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
            offset = (page_num - 1) * 48
            url = base_url if page_num == 1 else f"{base_url}_Desde_{offset+1}_NoIndex_True"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2500)

                cards_html = page.evaluate("""() => {
                    const els = document.querySelectorAll('.poly-card');
                    if (els.length > 0) return Array.from(els).map(e => e.outerHTML);
                    return Array.from(document.querySelectorAll('.andes-card')).map(e => e.outerHTML);
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
                self.logger.error(f"[mercadolibre] {url}: {e}")
                break

    def _is_cordoba(self, text: str) -> bool:
        text_lower = text.lower()
        if "córdoba" not in text_lower and "cordoba" not in text_lower:
            return False
        return True

    def _parse(self, card, prop_type):
        link = card.select_one("a[href]")
        if not link:
            return None
        full_url = link["href"]
        if not full_url.startswith("http"):
            full_url = self.BASE_URL + full_url
        external_id = f"ml_{hashlib.md5(full_url.encode()).hexdigest()[:12]}"

        # Título
        title_el = card.select_one("[class*='poly-component__title']")
        if not title_el:
            img = card.select_one("img[alt]")
            title = img["alt"] if img else ""
        else:
            title = title_el.get_text(strip=True)

        # Ubicación — filtrar Buenos Aires antes de continuar
        loc_el = card.select_one("[class*='poly-component__location']")
        address = loc_el.get_text(strip=True) if loc_el else ""
        if not self._is_cordoba(address + " " + title):
            return None
        neighborhood = address.split(",")[0].strip() if "," in address else address

        # Precio — tomar solo el primer elemento de precio, no concatenar
        price    = None
        currency = "ARS"
        price_el = card.select_one(".poly-price__current, [class*='poly-price__current']")
        if price_el:
            # Tomar solo fraction del precio principal (ignorar tachado/anterior)
            fraction_el  = price_el.select_one("[class*='fraction']")
            currency_el  = price_el.select_one("[class*='currency-symbol'], [class*='__currency']")
            frac_str     = re.sub(r'[^\d]', '', fraction_el.get_text(strip=True) if fraction_el else "")
            currency_sym = currency_el.get_text(strip=True) if currency_el else "$"
            try:
                price = float(frac_str) if frac_str else None
            except ValueError:
                price = None
            currency = "USD" if currency_sym.strip() in ("U$S","USD","US$","u$s","U$s") else "ARS"

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
            "price": price, "currency": currency, "expenses": None,
            "rooms": rooms, "bedrooms": bedrooms, "bathrooms": bathrooms,
            "area_m2": area_m2, "thumbnail_url": thumbnail,
        }

    def _re_int(self, text, pattern):
        m = re.search(pattern, text, re.IGNORECASE)
        return int(m.group(1)) if m else None

    def _re_float(self, text, pattern):
        m = re.search(pattern, text, re.IGNORECASE)
        return float(m.group(1).replace(",", ".")) if m else None
