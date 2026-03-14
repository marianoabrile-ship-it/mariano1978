"""
database.py — Base de datos con sqlite3 nativo (sin SQLAlchemy).
Sin dependencias externas, funciona en cualquier versión de Python 3.x.
"""

import sqlite3
import threading
from datetime import datetime
from contextlib import contextmanager

DB_PATH = "rentals.db"
_local = threading.local()


def get_conn() -> sqlite3.Connection:
    """Una conexión por hilo."""
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
    return _local.conn


@contextmanager
def db():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db():
    with db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS listings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id     TEXT    UNIQUE NOT NULL,
                source          TEXT    NOT NULL,
                url             TEXT    NOT NULL,
                title           TEXT,
                description     TEXT,
                property_type   TEXT,
                address         TEXT,
                neighborhood    TEXT,
                city            TEXT    DEFAULT 'Córdoba',
                rooms           INTEGER,
                bedrooms        INTEGER,
                bathrooms       INTEGER,
                area_m2         REAL,
                floor           INTEGER,
                has_parking     INTEGER DEFAULT 0,
                has_pool        INTEGER DEFAULT 0,
                is_furnished    INTEGER DEFAULT 0,
                price           REAL,
                currency        TEXT    DEFAULT 'ARS',
                price_ars       REAL,
                expenses        REAL,
                thumbnail_url   TEXT,
                is_active       INTEGER DEFAULT 1,
                first_seen_at   TEXT,
                last_seen_at    TEXT,
                price_changed   INTEGER DEFAULT 0,
                previous_price  REAL
            );
            CREATE INDEX IF NOT EXISTS ix_source      ON listings(source);
            CREATE INDEX IF NOT EXISTS ix_type        ON listings(property_type);
            CREATE INDEX IF NOT EXISTS ix_hood        ON listings(neighborhood);
            CREATE INDEX IF NOT EXISTS ix_price       ON listings(price_ars);
            CREATE TABLE IF NOT EXISTS scraper_logs (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                source            TEXT    NOT NULL,
                started_at        TEXT,
                finished_at       TEXT,
                listings_found    INTEGER DEFAULT 0,
                listings_new      INTEGER DEFAULT 0,
                listings_updated  INTEGER DEFAULT 0,
                error             TEXT,
                success           INTEGER DEFAULT 1
            );
        """)


def row_to_dict(row) -> dict:
    return dict(row) if row else {}


def upsert_listing(data: dict, usd_to_ars_rate: float = 1000.0):
    price     = data.get("price") or 0
    currency  = data.get("currency", "ARS") or "ARS"
    price_ars = price * usd_to_ars_rate if currency == "USD" else price
    now       = datetime.utcnow().isoformat()

    with db() as conn:
        existing = conn.execute(
            "SELECT * FROM listings WHERE external_id = ?", (data["external_id"],)
        ).fetchone()

        if existing:
            existing = dict(existing)
            price_changed = bool(
                existing.get("price") and price and abs(existing["price"] - price) > 0.01
            )
            conn.execute("""
                UPDATE listings SET
                    source=?, url=?, title=?, property_type=?, address=?,
                    neighborhood=?, rooms=?, bedrooms=?, bathrooms=?, area_m2=?,
                    price=?, currency=?, price_ars=?, expenses=?, thumbnail_url=?,
                    last_seen_at=?, price_changed=?,
                    previous_price=CASE WHEN ? THEN price ELSE previous_price END
                WHERE external_id=?
            """, (
                data.get("source"), data.get("url"), data.get("title"),
                data.get("property_type"), data.get("address"), data.get("neighborhood"),
                data.get("rooms"), data.get("bedrooms"), data.get("bathrooms"),
                data.get("area_m2"), price, currency, price_ars,
                data.get("expenses"), data.get("thumbnail_url"),
                now, int(price_changed), int(price_changed), data["external_id"],
            ))
            return False
        else:
            conn.execute("""
                INSERT INTO listings (
                    external_id, source, url, title, property_type, address,
                    neighborhood, rooms, bedrooms, bathrooms, area_m2,
                    price, currency, price_ars, expenses, thumbnail_url,
                    is_active, first_seen_at, last_seen_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)
            """, (
                data["external_id"], data.get("source"), data.get("url"),
                data.get("title"), data.get("property_type"),
                data.get("address"), data.get("neighborhood"),
                data.get("rooms"), data.get("bedrooms"), data.get("bathrooms"),
                data.get("area_m2"), price, currency, price_ars,
                data.get("expenses"), data.get("thumbnail_url"), now, now,
            ))
            return True


def get_listings(filters: dict) -> dict:
    conditions, params = ["is_active = 1"], []
    for field, col in [("property_type","property_type"),("source","source"),("currency","currency")]:
        if filters.get(field):
            val = filters[field].upper() if field == "currency" else filters[field]
            conditions.append(f"{col} = ?"); params.append(val)
    if filters.get("neighborhood"):
        conditions.append("neighborhood LIKE ?"); params.append(f"%{filters['neighborhood']}%")
    if filters.get("min_price") is not None:
        conditions.append("price_ars >= ?"); params.append(filters["min_price"])
    if filters.get("max_price") is not None:
        conditions.append("price_ars <= ?"); params.append(filters["max_price"])
    if filters.get("rooms") is not None:
        conditions.append("rooms = ?"); params.append(int(filters["rooms"]))

    where    = " AND ".join(conditions)
    sort_col = {"price_ars":"price_ars","area_m2":"area_m2"}.get(filters.get("sort_by",""), "first_seen_at")
    order    = "ASC" if filters.get("order") == "asc" else "DESC"
    page     = max(int(filters.get("page", 1)), 1)
    size     = min(max(int(filters.get("page_size", 20)), 1), 100)
    offset   = (page - 1) * size

    with db() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM listings WHERE {where}", params).fetchone()[0]
        rows  = conn.execute(
            f"SELECT * FROM listings WHERE {where} ORDER BY {sort_col} {order} LIMIT ? OFFSET ?",
            params + [size, offset]
        ).fetchall()

    return {"total": total, "page": page,
            "pages": max((total + size - 1) // size, 1),
            "results": [row_to_dict(r) for r in rows]}


def get_listing_by_id(listing_id):
    with db() as conn:
        row = conn.execute("SELECT * FROM listings WHERE id = ?", (listing_id,)).fetchone()
    return row_to_dict(row) if row else None


def get_neighborhoods():
    with db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT neighborhood FROM listings "
            "WHERE is_active=1 AND neighborhood IS NOT NULL AND neighborhood != '' ORDER BY neighborhood"
        ).fetchall()
    return [r[0] for r in rows]


def get_stats():
    today = datetime.utcnow().date().isoformat()
    with db() as conn:
        total     = conn.execute("SELECT COUNT(*) FROM listings WHERE is_active=1").fetchone()[0]
        avg_price = conn.execute("SELECT AVG(price_ars) FROM listings WHERE is_active=1 AND price_ars>0").fetchone()[0]
        new_today = conn.execute("SELECT COUNT(*) FROM listings WHERE is_active=1 AND first_seen_at >= ?", (today,)).fetchone()[0]
        by_source = conn.execute("SELECT source, COUNT(*) FROM listings WHERE is_active=1 GROUP BY source").fetchall()
        by_type   = conn.execute("SELECT property_type, COUNT(*) FROM listings WHERE is_active=1 GROUP BY property_type").fetchall()
    return {
        "total": total, "avg_price": round(avg_price or 0),
        "new_today": new_today,
        "by_source": {r[0]: r[1] for r in by_source},
        "by_type":   {r[0]: r[1] for r in by_type},
    }


def log_start(source):
    with db() as conn:
        cur = conn.execute("INSERT INTO scraper_logs (source, started_at) VALUES (?,?)",
                           (source, datetime.utcnow().isoformat()))
        return cur.lastrowid


def log_finish(log_id, found, new, updated, success=True, error=None):
    with db() as conn:
        conn.execute(
            "UPDATE scraper_logs SET finished_at=?,listings_found=?,listings_new=?,"
            "listings_updated=?,success=?,error=? WHERE id=?",
            (datetime.utcnow().isoformat(), found, new, updated, int(success), error, log_id)
        )


def get_logs(limit=20):
    with db() as conn:
        rows = conn.execute("SELECT * FROM scraper_logs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
    return [row_to_dict(r) for r in rows]
