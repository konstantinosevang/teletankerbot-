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
