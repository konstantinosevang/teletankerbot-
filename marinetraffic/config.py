"""
Vessel tracking configuration.
Uses VesselAPI (REST) - more reliable than aisstream.io WebSocket.
"""
import os

from dotenv import load_dotenv

load_dotenv()

# --- VesselAPI (primary - REST polling) ---
VESSELAPI_URL = os.getenv("VESSELAPI_URL", "https://api.vesselapi.com/v1")
VESSELAPI_KEY = (
    os.getenv("VESSELAPI_API_KEY", "")
    or os.getenv("VesselAPI_API_KEY", "")
    or os.getenv("VESSELAPI_KEY", "")
)

# --- aisstream.io (legacy, kept for test_aisstream.py) ---
WS_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.getenv("AISSTREAM_API_KEY", "") or os.getenv("aisstream_api_key", "")

# --- Persian Gulf + Oman Gulf + Strait of Hormuz ---
# Bbox: [[lat_lo, lon_lo], [lat_hi, lon_hi]] - SW and NE corners
# Default: Persian Gulf (W) + Strait of Hormuz + Gulf of Oman (E)
# Override via HORMUZ_BBOX env: "22,47,31,65" (minLat,minLon,maxLat,maxLon)
def _parse_bbox():
    s = os.getenv("HORMUZ_BBOX")
    if not s:
        return None
    s = s.strip().lower()
    if s == "world":
        return [[22.0, 47.0], [31.0, 65.0]]
    try:
        parts = [float(x.strip()) for x in s.split(",")]
        return [[parts[0], parts[1]], [parts[2], parts[3]]] if len(parts) == 4 else None
    except (ValueError, AttributeError):
        return None

HORMUZ_BBOX = _parse_bbox() or [[22.0, 47.0], [31.0, 65.0]]

# Strait of Hormuz zone boundaries (longitude) for enter/exit detection
# Persian Gulf = west of strait; Oman Gulf = east of strait
STRAIT_LON_WEST = 55.5   # lon < this = Persian Gulf
STRAIT_LON_EAST = 58.5   # lon > this = Oman Gulf; between = Strait
# AIS vessel types 80-89 = tankers
HORMUZ_TANKER_TYPES = (80, 81, 82, 83, 84, 85, 86, 87, 88, 89)
# Stats report interval - how often we send snapshot to Telegram
STATS_INTERVAL_SEC = int(os.getenv("HORMUZ_STATS_INTERVAL_SEC", "1800"))  # 30 minutes
# Low speed (kn) = waiting; above = transiting
HORMUZ_WAITING_SPEED_KN = 2.0
# Max age of position (seconds) - ignore older positions when building snapshot
POSITION_MAX_AGE_SEC = 30 * 60  # 30 minutes (AIS updates can be sparse)
