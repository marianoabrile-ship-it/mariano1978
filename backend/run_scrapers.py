"""
run_scrapers.py — Corre todos los scrapers y guarda en la base de datos.
Uso: python3 run_scrapers.py
"""
import logging
import database as db
from scrapers import ALL_SCRAPERS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

db.init_db()
total_new = 0

for S in ALL_SCRAPERS:
    s = S()
    result = s.run()
    total_new += result.get("new", 0)
    print(f"✓ {s.SOURCE_NAME}: {result.get('new',0)} nuevas, {result.get('updated',0)} actualizadas")

print(f"\n✅ Listo. {total_new} publicaciones nuevas guardadas.")
print("   Recargá http://localhost:8080 para verlas.")
