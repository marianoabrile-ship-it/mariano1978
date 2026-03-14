"""
scrapers/argenprop.py — Filtrar por Córdoba + precio por regex.
"""
import hashlib, re, time
from typing import Generator
from scrapers.base import BaseScraper

URLS = {
    "departamento":    "https://www.argenprop.com/departamentos/alquiler/cordoba-arg",
    "casa":            "https://www.argenprop.com/casas/alquiler/cordoba-arg",
    "local_comercial": "https://www.argenprop.com/locales-comerciales/alquiler/cordoba-arg",
    "habitacion":      "https://www.argenprop.com/cuartos/alquiler/cordoba-arg",
}

# Paginación nueva: ?pagina=2
def _paginate(base_url: str, page: int) -> str:
    return base_url if page == 1 else f"{base_url}?pagina={page}"


# Ciudades que NO son Córdoba — filtrar resultados de otras provincias
NON_CORDOBA = {
    # CABA
    "palermo","belgrano","recoleta","almagro","caballito","flores","villa crespo",
    "boedo","san telmo","retiro","balvanera","villa urquiza","coghlan","saavedra",
    "nuñez","colegiales","devoto","liniers","capital federal","caba",
    # Rosario / Santa Fe
    "rosario","santa fe","rafaela","venado tuerto","reconquista",
    # Mendoza
    "mendoza","godoy cruz","maipu",
    # Buenos Aires provincia
    "mar del plata","bahia blanca","la plata","quilmes","lomas de zamora",
    "moron","tigre","san isidro","vicente lopez","tres de febrero",
}

class ArgenpropScraper(BaseScraper):
    SOURCE_NAME = "argenprop"
    BASE_URL    = "https://www.argenprop.com"
    MAX_PAGES   = 5

    def scrape(self) -> Generator[dict, None, None]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.logger.error("[argenprop] Corré: pip install playwright && playwright install chromium")
            return

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled","--no-sandbox"]
            )
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                locale="es-AR", viewport={"width": 1366, "height": 768},
                extra_http_headers={"Accept-Language": "es-AR,es;q=0.9"},
            )
            ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = ctx.new_page()
            for prop_type, base_url in URLS.items():
                yield from self._scrape_type(page, prop_type, base_url)
            browser.close()

    def _scrape_type(self, page, prop_type, base_url):
        for page_num in range(1, self.MAX_PAGES + 1):
            url = base_url if page_num == 1 else f"{base_url}?pagina={page_num}"
            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(2000)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                page.wait_for_timeout(1000)

                cards_html = page.evaluate("""() => {
                    const sels = ['.listing__item','[class*="listing__item"]','.card','article'];
                    for (const sel of sels) {
                        const valid = Array.from(document.querySelectorAll(sel))
                            .filter(e => e.querySelector('a[href]') && e.innerText.length > 50);
                        if (valid.length > 2) return valid.map(e => e.outerHTML);
                    }
                    const cls = new Set();
                    document.querySelectorAll('*').forEach(e => e.classList.forEach(c => cls.add(c)));
                    return {title: document.title, classes: [...cls].filter(c => /card|item|prop|list/i.test(c)).slice(0,20)};
                }""")

                if isinstance(cards_html, dict):
                    self.logger.warning(f"[argenprop] Sin cards. Título='{cards_html.get('title')}' Clases={cards_html.get('classes')}")
                    break
                if not cards_html:
                    break

                from bs4 import BeautifulSoup
                parsed = 0
                for html in cards_html:
                    card = BeautifulSoup(html, "lxml").find()
                    if card:
                        item = self._parse(card, prop_type)
                        if item:
                            parsed += 1
                            yield item

                self.logger.info(f"[argenprop] pág {page_num} ({prop_type}): {parsed} props")
                time.sleep(2)
            except Exception as e:
                self.logger.error(f"[argenprop] {url}: {e}")
                break

    def _is_cordoba(self, text: str) -> bool:
        """Whitelist: aceptar solo si el texto menciona Córdoba explícitamente."""
        text_lower = text.lower()
        # Debe mencionar córdoba
        if "córdoba" not in text_lower and "cordoba" not in text_lower:
            return False
        # Y no debe ser otra ciudad con "córdoba" en el nombre (ej: "Córdoba, España")
        if "españa" in text_lower or "spain" in text_lower:
            return False
        return True

    def _extract_prices_from_text(self, text: str):
        """Extrae precios con regex directo del texto completo de la card."""
        # Buscar patrones como: $ 450.000 / USD 500 / $1.200.000
        patterns = [
            r'(USD|U\$S|u\$s)\s*[\$]?\s*([\d]{1,3}(?:[.,]\d{3})*)',  # USD primero
            r'\$\s*([\d]{1,3}(?:[.,]\d{3})+)',                         # $ con puntos miles
            r'\$\s*([\d]{3,7})',                                        # $ sin puntos
        ]
        found = []
        for pat in patterns:
            for m in re.finditer(pat, text, re.IGNORECASE):
                groups = m.groups()
                if len(groups) == 2:  # USD pattern
                    num_str = groups[1].replace(".", "").replace(",", "")
                    try:
                        found.append((float(num_str), "USD"))
                    except ValueError:
                        pass
                else:
                    num_str = groups[0].replace(".", "").replace(",", "")
                    try:
                        val = float(num_str)
                        if 10_000 < val < 50_000_000:  # rango razonable
                            found.append((val, "ARS"))
                    except ValueError:
                        pass

        price    = found[0][0] if found else None
        currency = found[0][1] if found else "ARS"
        expenses = found[1][0] if len(found) > 1 and found[1][1] == "ARS" else None
        return price, currency, expenses

    def _parse(self, card, prop_type):
        link = card.select_one("a[href]")
        if not link:
            return None
        href = link["href"]
        full_url = self.BASE_URL + href if href.startswith("/") else href
        external_id = f"ap_{hashlib.md5(full_url.encode()).hexdigest()[:12]}"

        title_el = card.select_one("h2, h3, [class*='title'], [class*='Title']")
        title = title_el.get_text(strip=True) if title_el else ""

        addr_el = card.select_one("[class*='address'], [class*='location'], [class*='Address']")
        address = addr_el.get_text(strip=True) if addr_el else ""

        # Filtrar si no es Córdoba
        if not self._is_cordoba(address + " " + title):
            return None

        neighborhood = address.split(",")[-1].strip() if "," in address else address

        # Precio por regex sobre el texto completo de la card
        text = card.get_text(" ", strip=True)
        price, currency, expenses = self._extract_prices_from_text(text)

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
