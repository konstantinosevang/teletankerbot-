"""
VesselAPI REST client for vessel tracking.
Polls bounding-box endpoint instead of WebSocket - more reliable than aisstream.io.
"""
import asyncio
import logging
import time

import aiohttp

from .config import (
    HORMUZ_BBOX,
    POSITION_MAX_AGE_SEC,
    STRAIT_LON_EAST,
    STRAIT_LON_WEST,
    VESSELAPI_KEY,
    VESSELAPI_URL,
)

log = logging.getLogger(__name__)

# VesselAPI max span: |dLat| + |dLon| <= 4 degrees
MAX_SPAN = 4


def _tile_bbox(bbox: list, max_span: float = MAX_SPAN) -> list[tuple[float, float, float, float]]:
    """Split bbox into tiles where each tile has dLat + dLon <= max_span."""
    lat_lo, lon_lo = bbox[0]
    lat_hi, lon_hi = bbox[1]
    dlat = lat_hi - lat_lo
    dlon = lon_hi - lon_lo
    if dlat + dlon <= max_span:
        return [(lat_lo, lon_lo, lat_hi, lon_hi)]
    # Tile with 2+2 degree cells
    cell = 2.0
    tiles = []
    lat = lat_lo
    while lat < lat_hi:
        lon = lon_lo
        while lon < lon_hi:
            lat_end = min(lat + cell, lat_hi)
            lon_end = min(lon + cell, lon_hi)
            if (lat_end - lat) + (lon_end - lon) <= max_span:
                tiles.append((lat, lon, lat_end, lon_end))
            lon += cell
        lat += cell
    return tiles


def _is_tanker(vessel_type: str | None) -> bool:
    """Check if vessel type string indicates tanker."""
    if not vessel_type:
        return False
    t = vessel_type.lower()
    return "tanker" in t or "oil" in t or "chemical" in t or "lpg" in t or "lng" in t


async def fetch_vessels_bbox(
    session: aiohttp.ClientSession,
    lat_lo: float,
    lon_lo: float,
    lat_hi: float,
    lon_hi: float,
    limit: int = 50,
) -> list[dict]:
    """Fetch vessels from VesselAPI bounding-box endpoint."""
    url = f"{VESSELAPI_URL}/location/vessels/bounding-box"
    params = {
        "filter.latBottom": lat_lo,
        "filter.latTop": lat_hi,
        "filter.lonLeft": lon_lo,
        "filter.lonRight": lon_hi,
        "pagination.limit": limit,
    }
    headers = {"Authorization": f"Bearer {VESSELAPI_KEY}"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                text = await r.text()
                log.warning("VesselAPI bbox %s: status %s %s", (lat_lo, lon_lo, lat_hi, lon_hi), r.status, text[:200])
                return []
            data = await r.json()
            if "error" in data:
                log.warning("VesselAPI error: %s", data["error"])
                return []
            return data.get("vessels") or []
    except Exception as e:
        log.warning("VesselAPI request failed: %s", e)
        return []


def _vessel_to_internal(v: dict, existing: dict | None = None) -> dict:
    """Convert VesselAPI vessel to internal format. Preserve vessel_type from existing if bbox has none."""
    vessel_type = v.get("vessel_type") or (existing and existing.get("vessel_type"))
    return {
        "mmsi": v.get("mmsi"),
        "name": v.get("vessel_name") or (existing and existing.get("name", "")) or "",
        "lat": v.get("latitude"),
        "lon": v.get("longitude"),
        "sog": v.get("sog"),
        "cog": v.get("cog"),
        "nav_status": v.get("nav_status"),
        "vessel_type": vessel_type,
        "timestamp": time.time(),
    }


def _zone_from_lon(lon: float | None) -> str:
    """Return zone: persian_gulf, strait, or oman_gulf."""
    if lon is None:
        return "unknown"
    if lon < STRAIT_LON_WEST:
        return "persian_gulf"
    if lon > STRAIT_LON_EAST:
        return "oman_gulf"
    return "strait"


async def fetch_vessel_type(session: aiohttp.ClientSession, mmsi: int) -> dict | None:
    """Fetch vessel type + static details from /vessel/{mmsi}."""
    url = f"{VESSELAPI_URL}/vessel/{mmsi}"
    params = {"filter.idType": "mmsi"}
    headers = {"Authorization": f"Bearer {VESSELAPI_KEY}"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if "error" in data:
                return None
            v = data.get("vessel") or {}
            return {
                "vessel_type": v.get("vessel_type"),
                "imo": v.get("imo"),
                "flag": v.get("country_code") or v.get("country"),
                "length": v.get("length"),
                "deadweight_tonnage": v.get("deadweight_tonnage"),
                "year_built": v.get("year_built"),
                "owner_name": v.get("owner_name"),
            }
    except Exception:
        return None


async def fetch_vessel_eta(session: aiohttp.ClientSession, mmsi: int) -> dict | None:
    """Fetch ETA/destination from /vessel/{mmsi}/eta (tankers only)."""
    url = f"{VESSELAPI_URL}/vessel/{mmsi}/eta"
    params = {"filter.idType": "mmsi"}
    headers = {"Authorization": f"Bearer {VESSELAPI_KEY}"}
    try:
        async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            if "error" in data:
                return None
            eta = data.get("vesselEta") or {}
            return {
                "destination": eta.get("destination"),
                "destination_port": eta.get("destination_port"),
                "eta": eta.get("eta"),
                "draught": eta.get("draught"),
            }
    except Exception:
        return None


async def _enrich_vessel_full(session: aiohttp.ClientSession, vessels: dict, semaphore: asyncio.Semaphore):
    """Fetch type for all vessels; full details (ETA) for tankers only."""
    to_enrich = [mmsi for mmsi, v in vessels.items() if not v.get("vessel_type")]
    if not to_enrich:
        return

    async def fetch_one(mmsi: int):
        async with semaphore:
            details = await fetch_vessel_type(session, mmsi)
            if details and mmsi in vessels:
                vessels[mmsi].update({k: v for k, v in details.items() if v is not None})
                if _is_tanker(details.get("vessel_type")):
                    eta_data = await fetch_vessel_eta(session, mmsi)
                    if eta_data and mmsi in vessels:
                        vessels[mmsi].update({k: v for k, v in eta_data.items() if v is not None})
            await asyncio.sleep(0.2)  # Throttle

    await asyncio.gather(*[fetch_one(m) for m in to_enrich])
    enriched = sum(1 for m in to_enrich if vessels.get(m, {}).get("vessel_type"))
    tankers = sum(1 for m, v in vessels.items() if _is_tanker(v.get("vessel_type")))
    if enriched:
        log.info("VesselAPI: enriched %d vessels (%d tankers)", enriched, tankers)


def zone_from_lon(lon: float | None) -> str:
    """Return zone: persian_gulf, strait, or oman_gulf. Exported for feed."""
    return _zone_from_lon(lon)


def _detect_strait_transitions(vessels: dict, last_zones: dict, enter_exit_callback, crossings_list: list | None = None) -> None:
    """Detect vessels entering/exiting Strait of Hormuz, call callback for each."""
    for mmsi, v in vessels.items():
        lon = v.get("lon")
        zone = _zone_from_lon(lon)
        prev_zone = last_zones.get(mmsi)
        last_zones[mmsi] = zone

        if prev_zone is None or prev_zone == zone:
            continue

        # Transition detected
        direction = None
        if prev_zone == "persian_gulf" and zone == "strait":
            direction = "ENTER (Persian Gulf → Strait)"
        elif prev_zone == "strait" and zone == "oman_gulf":
            direction = "EXIT (Strait → Oman Gulf)"
        elif prev_zone == "oman_gulf" and zone == "strait":
            direction = "ENTER (Oman Gulf → Strait)"
        elif prev_zone == "strait" and zone == "persian_gulf":
            direction = "EXIT (Strait → Persian Gulf)"
        elif prev_zone == "persian_gulf" and zone == "oman_gulf":
            direction = "TRANSIT (Persian Gulf → Oman Gulf)"
        elif prev_zone == "oman_gulf" and zone == "persian_gulf":
            direction = "TRANSIT (Oman Gulf → Persian Gulf)"

        if direction and _is_tanker(v.get("vessel_type")):
            if crossings_list is not None:
                crossings_list.append((time.time(), mmsi))
            if enter_exit_callback:
                enter_exit_callback(mmsi, prev_zone, zone, direction, v)


async def run_poller(
    vessels: dict,
    poll_interval: float = 60.0,
    connected_callback=None,
    enter_exit_callback=None,
    crossings_list: list | None = None,
):
    """
    Poll VesselAPI bounding-box, update vessels dict, enrich with full details.
    Detects Strait of Hormuz enter/exit and calls enter_exit_callback.
    """
    if not VESSELAPI_KEY:
        log.warning("VESSELAPI_API_KEY not set - skipping vessel feed")
        return

    tiles = _tile_bbox(HORMUZ_BBOX)
    semaphore = asyncio.Semaphore(5)
    last_zones: dict[int, str] = {}
    log.info("VesselAPI: polling %d tiles every %.0fs (bbox: %s)", len(tiles), poll_interval, HORMUZ_BBOX)

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                all_vessels = []
                for lat_lo, lon_lo, lat_hi, lon_hi in tiles:
                    batch = await fetch_vessels_bbox(session, lat_lo, lon_lo, lat_hi, lon_hi)
                    all_vessels.extend(batch)
                    await asyncio.sleep(0.3)  # Small delay between tiles to avoid rate limit

                for v in all_vessels:
                    mmsi = v.get("mmsi")
                    if mmsi is not None:
                        existing = vessels.get(mmsi)
                        internal = _vessel_to_internal(v, existing)
                        vessels[mmsi] = internal

                if all_vessels:
                    log.info("VesselAPI: fetched %d vessels, %d unique", len(all_vessels), len(vessels))
                    await _enrich_vessel_full(session, vessels, semaphore)
                    _detect_strait_transitions(vessels, last_zones, enter_exit_callback, crossings_list)
                if connected_callback:
                    connected_callback(True)

        except asyncio.CancelledError:
            log.info("VesselAPI poller cancelled")
            if connected_callback:
                connected_callback(False)
            raise
        except Exception as e:
            log.error("VesselAPI poller error: %s", e)
            if connected_callback:
                connected_callback(False)

        await asyncio.sleep(poll_interval)


def get_tanker_snapshot(vessels: dict) -> list[dict]:
    """Build list of tankers from vessel state."""
    return get_vessel_snapshot(vessels, tankers_only=True)


def get_vessel_snapshot(vessels: dict, tankers_only: bool = False) -> list[dict]:
    """Build list of vessels, filtering by max age and optionally tanker type."""
    now = time.time()
    cutoff = now - POSITION_MAX_AGE_SEC
    result = []
    for mmsi, v in vessels.items():
        ts = v.get("timestamp", 0)
        if ts < cutoff:
            continue
        if tankers_only and not _is_tanker(v.get("vessel_type")):
            continue
        result.append({
            "mmsi": mmsi,
            "name": v.get("name", ""),
            "lat": v.get("lat"),
            "lon": v.get("lon"),
            "sog": v.get("sog"),
            "cog": v.get("cog"),
            "nav_status": v.get("nav_status"),
            "vessel_type": v.get("vessel_type"),
            "flag": v.get("flag"),
            "imo": v.get("imo"),
            "destination": v.get("destination"),
            "destination_port": v.get("destination_port"),
            "eta": v.get("eta"),
            "deadweight_tonnage": v.get("deadweight_tonnage"),
            "length": v.get("length"),
            "year_built": v.get("year_built"),
            "owner_name": v.get("owner_name"),
        })
    return result
