"""SQLite storage for BDTI and AIS ledger."""
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "teletanker.db"


def _conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bdti (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value REAL NOT NULL,
                previous REAL,
                index_date TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bdti_index_date ON bdti(index_date)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silence_alerts (
                mmsi INTEGER PRIMARY KEY,
                alerted_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ledger (
                mmsi INTEGER PRIMARY KEY,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                zone TEXT NOT NULL,
                speed REAL,
                last_seen REAL NOT NULL,
                name TEXT,
                ship_type INTEGER,
                is_stationary INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vessel_cache (
                mmsi INTEGER PRIMARY KEY,
                ship_type INTEGER,
                name TEXT,
                updated_at TEXT NOT NULL
            )
        """)


def insert_bdti(value: float, previous: float | None, index_date: str):
    """Store BDTI. Skips if index_date already exists (avoid duplicates)."""
    latest = get_latest_bdti()
    if latest and latest.get("index_date") == index_date:
        return False
    with _conn() as conn:
        conn.execute(
            "INSERT INTO bdti (value, previous, index_date) VALUES (?, ?, ?)",
            (value, previous, index_date),
        )
    return True


def get_latest_bdti() -> dict | None:
    with _conn() as conn:
        row = conn.execute(
            "SELECT value, previous, index_date, created_at FROM bdti ORDER BY index_date DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def get_bdti_history(limit: int = 100):
    with _conn() as conn:
        rows = conn.execute(
            "SELECT value, previous, index_date, created_at FROM bdti ORDER BY index_date DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# --- AIS silence alerts ---
def add_silence_alert(mmsi: int) -> bool:
    """Record that we alerted for this MMSI. Returns True if already alerted (skip)."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        with _conn() as conn:
            conn.execute("INSERT INTO silence_alerts (mmsi, alerted_at) VALUES (?, ?)", (mmsi, now))
        return False  # New alert
    except sqlite3.IntegrityError:
        return True  # Already alerted


def remove_silence_alert(mmsi: int):
    """Remove silence alert when vessel resumes transmitting."""
    with _conn() as conn:
        conn.execute("DELETE FROM silence_alerts WHERE mmsi = ?", (mmsi,))


def remove_ledger(mmsi: int):
    """Remove vessel from ledger (DB)."""
    with _conn() as conn:
        conn.execute("DELETE FROM ledger WHERE mmsi = ?", (mmsi,))


def get_ledger() -> dict:
    """Load ledger from DB. Returns {mmsi: {lat, lon, zone, speed, last_seen, name, ...}}."""
    out = {}
    with _conn() as conn:
        rows = conn.execute(
            "SELECT mmsi, lat, lon, zone, speed, last_seen, name, ship_type, is_stationary FROM ledger"
        ).fetchall()
        for row in rows:
            out[row["mmsi"]] = {
                "lat": row["lat"],
                "lon": row["lon"],
                "zone": row["zone"],
                "speed": row["speed"],
                "last_seen": row["last_seen"],
                "name": row["name"] or "",
                "ship_type": row["ship_type"],
                "is_stationary": bool(row["is_stationary"]),
            }
    return out


def upsert_ledger(mmsi: int, lat: float, lon: float, zone: str, speed: float | None, last_seen: float, name: str, ship_type: int | None, is_stationary: bool):
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO ledger (mmsi, lat, lon, zone, speed, last_seen, name, ship_type, is_stationary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mmsi) DO UPDATE SET
                lat = excluded.lat, lon = excluded.lon, zone = excluded.zone,
                speed = excluded.speed, last_seen = excluded.last_seen,
                name = COALESCE(NULLIF(excluded.name, ''), ledger.name),
                ship_type = COALESCE(excluded.ship_type, ledger.ship_type),
                is_stationary = excluded.is_stationary
            """,
            (mmsi, lat, lon, zone, speed, last_seen, name or "", ship_type, 1 if is_stationary else 0),
        )


def upsert_vessel(mmsi: int, ship_type: int | None, name: str):
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO vessel_cache (mmsi, ship_type, name, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(mmsi) DO UPDATE SET
                ship_type = COALESCE(excluded.ship_type, vessel_cache.ship_type),
                name = COALESCE(NULLIF(excluded.name, ''), vessel_cache.name),
                updated_at = excluded.updated_at
            """,
            (mmsi, ship_type, name or "", now),
        )


def load_vessel_cache() -> dict:
    out = {}
    with _conn() as conn:
        rows = conn.execute("SELECT mmsi, ship_type, name FROM vessel_cache").fetchall()
        for row in rows:
            out[row["mmsi"]] = {"type": row["ship_type"], "name": row["name"] or ""}
    return out
