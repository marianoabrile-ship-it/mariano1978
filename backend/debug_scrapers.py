"""
debug_scrapers.py — Diagnóstico de selectores CSS para cada sitio.
Correr con: python debug_scrapers.py
Copiar el output y compartirlo para ajustar los scrapers.
"""

import httpx
import random
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

SITES = {
    "zonaprop":     "https://www.zonaprop.com.ar/departamentos-alquiler-cordoba.html",
    "argenprop":    "https://www.argenprop.com/departamento--alquiler--en-cordoba",
    "mercadolibre": "https://inmuebles.mercadolibre.com.ar/departamentos/alquiler/cordoba/",
}

# Selectores candidatos a probar en cada sitio
CANDIDATES = {
    "zonaprop": [
        '[data-qa="posting PROPERTY"]',
        '.postingCard',
        '[class*="postingCard"]',
        '[class*="PostingCard"]',
        'article',
        '.sc-1tt2vbg-4',
    ],
    "argenprop": [
        '.listing-card',
        '.card--vertical',
        '[class*="listing-card"]',
        '[class*="card--vertical"]',
        '.card',
        'article',
    ],
    "mercadolibre": [
        '.ui-search-layout__item',
        '[class*="ui-search-layout__item"]',
        '.ui-search-result',
        '[class*="ui-search-result"]',
        'li.ui-search-layout__item',
        '.andes-card',
    ],
}


def fetch(url: str):
    try:
        with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=20) as client:
            r = client.get(url)
            r.raise_for_status()
            return r.text, r.status_code
    except Exception as e:
        return None, str(e)


def diagnose(name: str, url: str):
    print(f"\n{'='*60}")
    print(f"  {name.upper()}  →  {url}")
    print(f"{'='*60}")

    html, status = fetch(url)
    if not html:
        print(f"  ❌ Error al fetch: {status}")
        return

    print(f"  ✓ HTTP {status} — {len(html):,} bytes recibidos")
    soup = BeautifulSoup(html, "lxml")

    # Título de la página
    title = soup.find("title")
    print(f"  Título: {title.get_text()[:80] if title else '(sin título)'}")

    # Probar selectores candidatos
    print(f"\n  Selectores candidatos:")
    found_any = False
    for sel in CANDIDATES[name]:
        elements = soup.select(sel)
        status_icon = "✅" if elements else "  "
        print(f"    {status_icon} {sel!r:45s} → {len(elements)} elementos")
        if elements:
            found_any = True

    # Si encontramos algo, mostrar un preview del primer elemento
    for sel in CANDIDATES[name]:
        elements = soup.select(sel)
        if elements:
            print(f"\n  Preview del primer elemento con {sel!r}:")
            first = elements[0]
            # Mostrar clases de hijos directos
            for child in first.children:
                if hasattr(child, 'get'):
                    classes = child.get("class", [])
                    tag     = child.name
                    text    = child.get_text(strip=True)[:60]
                    if tag:
                        print(f"    <{tag} class='{' '.join(classes) if classes else ''}'> {text}")
            break

    if not found_any:
        # Mostrar las primeras clases únicas del documento para detectar patrones
        print(f"\n  ⚠️  Ningún selector funcionó. Clases más comunes en el body:")
        all_classes = []
        for tag in soup.find_all(True):
            classes = tag.get("class", [])
            all_classes.extend(classes)
        from collections import Counter
        top = Counter(all_classes).most_common(20)
        for cls, count in top:
            if any(k in cls.lower() for k in ["post","card","list","item","result","prop","inm"]):
                print(f"    .{cls} ({count}x)")

        # También mostrar data-qa attrs
        qa_attrs = [t.get("data-qa") for t in soup.find_all(attrs={"data-qa": True})]
        if qa_attrs:
            print(f"\n  data-qa attrs encontrados:")
            for qa in Counter(qa_attrs).most_common(10):
                print(f"    [{qa[0]}] ({qa[1]}x)")


def main():
    print("🔍 Diagnóstico de scrapers — Alquileres Córdoba")
    print("   Probando conectividad y selectores CSS...\n")

    for name, url in SITES.items():
        diagnose(name, url)

    print(f"\n{'='*60}")
    print("  Copiá este output completo y compartilo.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
