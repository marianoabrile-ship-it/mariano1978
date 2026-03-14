"""
api.py — API REST con Flask puro. Sin pydantic, sin uvicorn, sin SQLAlchemy.
Correr con: python api.py
"""

import json
import logging
import os
import threading
from flask import Flask, request, jsonify, send_from_directory

import database as db_module
import threading
import time as _time
from scrapers import ALL_SCRAPERS

SCRAPING_INTERVAL_MINUTES = 60  # Cambiar a 1 para pruebas, 60 para producción

def _auto_scraping_loop():
    """Corre los scrapers automáticamente cada N minutos en background."""
    _time.sleep(10)  # Esperar que el servidor arranque
    while True:
        try:
            logging.info("=== Auto-scraping iniciado ===")
            _run_scraping()
            logging.info(f"=== Auto-scraping finalizado. Próximo en {SCRAPING_INTERVAL_MINUTES} min ===")
        except Exception as e:
            logging.error(f"Error en auto-scraping: {e}")
        _time.sleep(SCRAPING_INTERVAL_MINUTES * 60)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = Flask(__name__, static_folder="../frontend")
db_module.init_db()


# ── Frontend ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("../frontend", "index.html")


# ── Stats ──────────────────────────────────────────────────────────────────────
@app.route("/api/stats")
def stats():
    return jsonify(db_module.get_stats())


# ── Listings ───────────────────────────────────────────────────────────────────
@app.route("/api/listings")
def list_listings():
    filters = {
        "property_type": request.args.get("property_type"),
        "source":        request.args.get("source"),
        "neighborhood":  request.args.get("neighborhood"),
        "currency":      request.args.get("currency"),
        "min_price":     request.args.get("min_price", type=float),
        "max_price":     request.args.get("max_price", type=float),
        "rooms":         request.args.get("rooms", type=int),
        "sort_by":       request.args.get("sort_by", "first_seen_at"),
        "order":         request.args.get("order", "desc"),
        "page":          request.args.get("page", 1, type=int),
        "page_size":     request.args.get("page_size", 20, type=int),
    }
    return jsonify(db_module.get_listings(filters))


@app.route("/api/listings/<int:listing_id>")
def get_listing(listing_id):
    listing = db_module.get_listing_by_id(listing_id)
    if not listing:
        return jsonify({"error": "No encontrado"}), 404
    return jsonify(listing)


@app.route("/api/neighborhoods")
def neighborhoods():
    return jsonify(db_module.get_neighborhoods())


# ── Scraper logs ───────────────────────────────────────────────────────────────
@app.route("/api/scraper/logs")
def scraper_logs():
    limit = request.args.get("limit", 20, type=int)
    return jsonify(db_module.get_logs(limit))


# ── Trigger scraping ───────────────────────────────────────────────────────────
@app.route("/api/scraper/run", methods=["POST"])
def trigger_scraping():
    source = request.args.get("source")
    if not source:
        try:
            body = request.get_json(silent=True) or {}
            source = body.get("source")
        except Exception:
            source = None
    thread = threading.Thread(target=_run_scraping, args=(source,), daemon=True)
    thread.start()
    return jsonify({"message": f"Scraping iniciado"})


def _run_scraping(source=None):
    for ScraperClass in ALL_SCRAPERS:
        scraper = ScraperClass()
        if source and scraper.SOURCE_NAME != source:
            continue
        try:
            result = scraper.run()
            logging.info(f"Scraper {result}")
        except Exception as e:
            logging.error(f"Error en scraper {ScraperClass.__name__}: {e}")


# Inicializar DB y arrancar auto-scraping
db_module.init_db()
logger.info("Base de datos inicializada")
_t = threading.Thread(target=_auto_scraping_loop, daemon=True)
_t.start()
logger.info(f"Auto-scraping activado cada {SCRAPING_INTERVAL_MINUTES} minutos")

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    print(f"\n🏢 Agente Alquileres Córdoba")
    print(f"   Dashboard: http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)
