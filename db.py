"""SQLite storage for BDTI and vessel details."""
import sqlite3
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
            CREATE TABLE IF NOT EXISTS vessels (
                mmsi INTEGER PRIMARY KEY,
                name TEXT,
                lat REAL,
                lon REAL,
                sog REAL,
                cog REAL,
                nav_status INTEGER,
                vessel_type TEXT,
                flag TEXT,
                imo INTEGER,
                destination TEXT,
                destination_port TEXT,
                eta TEXT,
                deadweight_tonnage REAL,
                length REAL,
                year_built INTEGER,
                owner_name TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS vessel_crossings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mmsi INTEGER NOT NULL,
                from_zone TEXT NOT NULL,
                to_zone TEXT NOT NULL,
                direction TEXT NOT NULL,
                vessel_name TEXT,
                vessel_type TEXT,
                crossed_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_crossings_mmsi ON vessel_crossings(mmsi)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_crossings_crossed_at ON vessel_crossings(crossed_at)")


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


def upsert_vessels(vessels: dict):
    """Store or update vessel details. vessels: {mmsi: {name, lat, lon, ...}}."""
    if not vessels:
        return
    with _conn() as conn:
        for mmsi, v in vessels.items():
            if mmsi is None:
                continue
            conn.execute(
                """
                INSERT INTO vessels (mmsi, name, lat, lon, sog, cog, nav_status, vessel_type,
                    flag, imo, destination, destination_port, eta, deadweight_tonnage,
                    length, year_built, owner_name, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(mmsi) DO UPDATE SET
                    name=excluded.name, lat=excluded.lat, lon=excluded.lon,
                    sog=excluded.sog, cog=excluded.cog, nav_status=excluded.nav_status,
                    vessel_type=excluded.vessel_type, flag=excluded.flag, imo=excluded.imo,
                    destination=excluded.destination, destination_port=excluded.destination_port,
                    eta=excluded.eta, deadweight_tonnage=excluded.deadweight_tonnage,
                    length=excluded.length, year_built=excluded.year_built,
                    owner_name=excluded.owner_name, updated_at=datetime('now')
                """,
                (
                    mmsi,
                    v.get("name") or "",
                    v.get("lat"),
                    v.get("lon"),
                    v.get("sog"),
                    v.get("cog"),
                    v.get("nav_status"),
                    v.get("vessel_type"),
                    v.get("flag"),
                    v.get("imo"),
                    v.get("destination"),
                    v.get("destination_port"),
                    v.get("eta"),
                    v.get("deadweight_tonnage"),
                    v.get("length"),
                    v.get("year_built"),
                    v.get("owner_name"),
                ),
            )


def insert_crossing(mmsi: int, from_zone: str, to_zone: str, direction: str, vessel: dict):
    """Record a tanker crossing the Strait of Hormuz."""
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO vessel_crossings (mmsi, from_zone, to_zone, direction, vessel_name, vessel_type)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                mmsi,
                from_zone,
                to_zone,
                direction,
                vessel.get("name"),
                vessel.get("vessel_type"),
            ),
        )
