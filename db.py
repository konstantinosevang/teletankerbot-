"""SQLite storage for BDTI (Baltic Dirty Tanker Index) history."""
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
            CREATE TABLE IF NOT EXISTS ais_silence_alerts (
                mmsi INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ais_ledger (
                mmsi INTEGER PRIMARY KEY,
                name TEXT,
                lat REAL,
                lon REAL,
                last_seen REAL
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


def add_silence_alert(mmsi: int) -> bool:
    """
    Returns True if alert already exists for this MMSI, else stores it and returns False.
    """
    with _conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM ais_silence_alerts WHERE mmsi = ?",
            (mmsi,),
        ).fetchone()

        if row:
            return True

        conn.execute(
            "INSERT INTO ais_silence_alerts (mmsi) VALUES (?)",
            (mmsi,),
        )
        return False


def remove_ledger(mmsi: int):
    """
    Remove vessel from ledger and clear its silence alert state.
    """
    with _conn() as conn:
        conn.execute("DELETE FROM ais_ledger WHERE mmsi = ?", (mmsi,))
        conn.execute("DELETE FROM ais_silence_alerts WHERE mmsi = ?", (mmsi,))