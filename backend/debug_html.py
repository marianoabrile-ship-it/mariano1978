"""
debug_html.py — Vuelca el HTML real que ve Playwright en cada sitio.
Correr: python3 debug_html.py
"""
from playwright.sync_api import sync_playwright

SITES = {
    "argenprop":    "https://www.argenprop.com/departamento--alquiler--en-cordoba",
    "mercadolibre": "https://inmuebles.mercadolibre.com.ar/departamentos/alquiler/cordoba-capital/",
    "zonaprop":     "https://www.zonaprop.com.ar/departamentos-alquiler-cordoba.html",
}

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        locale="es-AR", viewport={"width": 1280, "height": 900},
    )
    page = ctx.new_page()

    for name, url in SITES.items():
        print(f"\n{'='*60}\n  {name.upper()}\n{'='*60}")
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        info = page.evaluate("""() => {
            // Todas las clases únicas del documento
            const classes = new Set();
            document.querySelectorAll('*').forEach(el => {
                el.classList.forEach(c => classes.add(c));
            });

            // Primer elemento con precio
            const priceEl = document.querySelector(
                '[class*="price"], [class*="Price"], [class*="monto"], [class*="valor"]'
            );

            // Primer elemento que parezca una card de propiedad
            const cardSels = [
                'article','[class*="card"]','[class*="Card"]',
                '[class*="posting"]','[class*="Posting"]',
                '[class*="listing"]','[class*="result"]',
                '[class*="property"]','[class*="item"]',
            ];
            let firstCard = null;
            let cardSel = '';
            for (const sel of cardSels) {
                const els = document.querySelectorAll(sel);
                if (els.length > 2) {
                    firstCard = els[0].outerHTML.slice(0, 800);
                    cardSel = sel + ' (' + els.length + ' encontrados)';
                    break;
                }
            }

            return {
                title: document.title,
                url: location.href,
                classCount: classes.size,
                relevantClasses: [...classes].filter(c =>
                    /card|posting|listing|result|property|item|price|monto/i.test(c)
                ).slice(0, 30),
                priceEl: priceEl ? priceEl.outerHTML.slice(0, 200) : null,
                cardSel,
                firstCard,
            };
        }""")

        print(f"  Título   : {info['title'][:70]}")
        print(f"  URL final: {info['url'][:70]}")
        print(f"  Clases relevantes ({info['classCount']} total):")
        for c in info['relevantClasses']:
            print(f"    .{c}")
        print(f"\n  Mejor selector de card: {info['cardSel'] or 'NINGUNO'}")
        if info['firstCard']:
            print(f"\n  Primera card (800 chars):\n{info['firstCard']}")
        if info['priceEl']:
            print(f"\n  Primer elemento precio:\n{info['priceEl']}")

    browser.close()
