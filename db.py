"""
SQLite storage for teletankerbot.
- crossings: tanker enter/exit events
- vessel_cache: MMSI -> ship_type, name
- silence_alerts: MMSI we've alerted for AIS silence
- ledger: current vessel state (position, zone, speed, stationary)
- position_updates: position history for activity/traffic (24h retention)
"""
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "teletanker.db"
_local = threading.local()
POSITION_RETENTION_HOURS = 24

def _conn():
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


@contextmanager
def get_cursor():
    conn = _conn()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def init_db():
    with get_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crossings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mmsi INTEGER NOT NULL,
                direction TEXT NOT NULL,
                vessel_name TEXT,
                lat REAL,
                lon REAL,
                ts REAL NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crossings_ts ON crossings(ts)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vessel_cache (
                mmsi INTEGER PRIMARY KEY,
                ship_type INTEGER,
                name TEXT,
                updated_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silence_alerts (
                mmsi INTEGER PRIMARY KEY,
                alerted_at TEXT NOT NULL
            )
        """)
        cur.execute("""
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS position_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mmsi INTEGER NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                zone TEXT NOT NULL,
                speed REAL,
                ts REAL NOT NULL,
                is_stationary INTEGER NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_position_updates_mmsi ON position_updates(mmsi)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_position_updates_ts ON position_updates(ts)")
    cleanup_position_updates()


def insert_crossing(mmsi: int, direction: str, vessel_name: str, lat: float, lon: float, ts: float):
    now = datetime.now(timezone.utc).isoformat()
    with get_cursor() as cur:
        cur.execute(
            "INSERT INTO crossings (mmsi, direction, vessel_name, lat, lon, ts, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (mmsi, direction, vessel_name, lat, lon, ts, now),
        )


def upsert_vessel(mmsi: int, ship_type: int | None, name: str):
    now = datetime.now(timezone.utc).isoformat()
    with get_cursor() as cur:
        cur.execute(
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
    with get_cursor() as cur:
        cur.execute("SELECT mmsi, ship_type, name FROM vessel_cache")
        for row in cur.fetchall():
            out[row["mmsi"]] = {"type": row["ship_type"], "name": row["name"] or ""}
    return out


def get_rate_events(hours: float = 24) -> tuple[list[float], list[float]]:
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    enters, exits = [], []
    with get_cursor() as cur:
        cur.execute("SELECT direction, ts FROM crossings WHERE ts >= ? ORDER BY ts", (cutoff,))
        for row in cur.fetchall():
            if row["direction"] == "enter":
                enters.append(row["ts"])
            else:
                exits.append(row["ts"])
    return enters, exits


def add_silence_alert(mmsi: int) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    with get_cursor() as cur:
        try:
            cur.execute("INSERT INTO silence_alerts (mmsi, alerted_at) VALUES (?, ?)", (mmsi, now))
            return False
        except sqlite3.IntegrityError:
            return True


def remove_silence_alert(mmsi: int):
    with get_cursor() as cur:
        cur.execute("DELETE FROM silence_alerts WHERE mmsi = ?", (mmsi,))


def get_crossings(limit: int = 100) -> list[dict]:
    with get_cursor() as cur:
        cur.execute(
            "SELECT mmsi, direction, vessel_name, lat, lon, ts, created_at FROM crossings ORDER BY ts DESC LIMIT ?",
            (limit,),
        )
        return [dict(zip(row.keys(), row)) for row in cur.fetchall()]


# --- Ledger (current vessel state) ---
def upsert_ledger(mmsi: int, lat: float, lon: float, zone: str, speed: float | None, last_seen: float, name: str, ship_type: int | None, is_stationary: bool):
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO ledger (mmsi, lat, lon, zone, speed, last_seen, name, ship_type, is_stationary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mmsi) DO UPDATE SET
                lat = excluded.lat,
                lon = excluded.lon,
                zone = excluded.zone,
                speed = excluded.speed,
                last_seen = excluded.last_seen,
                name = COALESCE(NULLIF(excluded.name, ''), ledger.name),
                ship_type = COALESCE(excluded.ship_type, ledger.ship_type),
                is_stationary = excluded.is_stationary
            """,
            (mmsi, lat, lon, zone, speed, last_seen, name or "", ship_type, 1 if is_stationary else 0),
        )


def remove_ledger(mmsi: int):
    with get_cursor() as cur:
        cur.execute("DELETE FROM ledger WHERE mmsi = ?", (mmsi,))


def get_ledger() -> dict:
    """Load full ledger from DB. Returns {mmsi: {lat, lon, zone, speed, last_seen, name, ship_type, is_stationary}}."""
    out = {}
    with get_cursor() as cur:
        cur.execute("SELECT mmsi, lat, lon, zone, speed, last_seen, name, ship_type, is_stationary FROM ledger")
        for row in cur.fetchall():
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


# --- Position history (activity / traffic) ---
def insert_position_update(mmsi: int, lat: float, lon: float, zone: str, speed: float | None, ts: float, is_stationary: bool):
    with get_cursor() as cur:
        cur.execute(
            "INSERT INTO position_updates (mmsi, lat, lon, zone, speed, ts, is_stationary) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (mmsi, lat, lon, zone, speed, ts, 1 if is_stationary else 0),
        )


def cleanup_position_updates():
    """Remove position_updates older than retention period."""
    cutoff = datetime.now(timezone.utc).timestamp() - POSITION_RETENTION_HOURS * 3600
    with get_cursor() as cur:
        cur.execute("DELETE FROM position_updates WHERE ts < ?", (cutoff,))


def get_position_updates(mmsi: int | None = None, hours: float = 24, limit: int = 5000) -> list[dict]:
    """Get position history for activity analysis. Optionally filter by mmsi."""
    cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
    with get_cursor() as cur:
        if mmsi is not None:
            cur.execute(
                "SELECT mmsi, lat, lon, zone, speed, ts, is_stationary FROM position_updates WHERE mmsi = ? AND ts >= ? ORDER BY ts DESC LIMIT ?",
                (mmsi, cutoff, limit),
            )
        else:
            cur.execute(
                "SELECT mmsi, lat, lon, zone, speed, ts, is_stationary FROM position_updates WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
                (cutoff, limit),
            )
        return [dict(zip(row.keys(), row)) for row in cur.fetchall()]


def get_stationary_vessels() -> list[dict]:
    """Vessels currently stationary (speed < 0.5 kn)."""
    with get_cursor() as cur:
        cur.execute(
            "SELECT mmsi, lat, lon, zone, speed, last_seen, name, ship_type FROM ledger WHERE is_stationary = 1 ORDER BY last_seen DESC"
        )
        return [dict(zip(row.keys(), row)) for row in cur.fetchall()]


def get_moving_vessels() -> list[dict]:
    """Vessels currently moving."""
    with get_cursor() as cur:
        cur.execute(
            "SELECT mmsi, lat, lon, zone, speed, last_seen, name, ship_type FROM ledger WHERE is_stationary = 0 ORDER BY last_seen DESC"
        )
        return [dict(zip(row.keys(), row)) for row in cur.fetchall()]
