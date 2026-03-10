"""
aisstream.io WebSocket client.
Streams AIS PositionReport and ShipStaticData for Hormuz Strait.
Uses aiohttp WebSocket (alternative to websockets lib).
"""
import asyncio
import json
import logging
import time

import aiohttp

from .config import (
    API_KEY,
    HORMUZ_BBOX,
    HORMUZ_TANKER_TYPES,
    POSITION_MAX_AGE_SEC,
    WS_URL,
)

log = logging.getLogger(__name__)


def _is_tanker(ship_type: int | None) -> bool:
    """AIS types 80-89 = tankers."""
    if ship_type is None:
        return False
    return 80 <= ship_type <= 89


async def run_stream(vessels: dict, reconnect_delay: float = 5.0, connected_callback=None):
    """
    Connect to aisstream.io, subscribe to Hormuz bbox, process messages.
    Updates `vessels` dict in place: mmsi -> {mmsi, name, lat, lon, sog, cog, nav_status, vessel_type, timestamp}.
    Reconnects on disconnect.
    """
    if not API_KEY:
        log.warning("AISSTREAM_API_KEY not set - skipping AIS stream")
        return

    log.info("aisstream: connecting to %s (bbox: %s)", WS_URL, HORMUZ_BBOX)

    # Try both: APIKey (Python docs) and Apikey (reported to work)
    subscription = {
        "APIKey": API_KEY,
        "BoundingBoxes": [HORMUZ_BBOX],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    sub_json = json.dumps(subscription)

    # Long connect timeout - aisstream can be slow to respond
    connect_timeout = aiohttp.ClientTimeout(total=60, connect=45)

    while True:
        try:
            log.info("aisstream: attempting connection...")
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, timeout=connect_timeout) as ws:
                    await ws.send_str(sub_json)
                    log.info("aisstream.io connected, subscribed to Hormuz")
                    if connected_callback:
                        connected_callback(True)

                    msg_count = 0
                    total_msgs = 0
                    last_log = 0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                if "error" in data:
                                    log.error("aisstream error: %s", data["error"])
                                    continue

                                total_msgs += 1
                                msg_type = data.get("MessageType")
                                if total_msgs == 1:
                                    log.info("aisstream: first message received (type: %s)", msg_type)
                                # Log every 30s if we're receiving messages but no positions yet
                                now_ts = time.time()
                                if total_msgs > 0 and msg_count == 0 and (now_ts - last_log) >= 30:
                                    log.info("aisstream: %d msgs received, 0 PositionReports so far (last type: %s)", total_msgs, msg_type)
                                    last_log = now_ts
                                meta = data.get("MetaData") or data.get("Metadata") or {}
                                inner = data.get("Message") or {}

                                if msg_type == "ShipStaticData":
                                    s = inner.get("ShipStaticData")
                                    if s:
                                        mmsi = s.get("UserID")
                                        if mmsi is not None:
                                            ship_type = s.get("Type")
                                            name = (s.get("Name") or meta.get("ShipName") or "").strip()
                                            if mmsi not in vessels:
                                                vessels[mmsi] = {}
                                            vessels[mmsi]["vessel_type"] = ship_type
                                            if name:
                                                vessels[mmsi]["name"] = name

                                elif msg_type == "PositionReport":
                                    pr = inner.get("PositionReport")
                                    if pr:
                                        mmsi = pr.get("UserID")
                                        lat = pr.get("Latitude")
                                        lon = pr.get("Longitude")
                                        if mmsi is not None and lat is not None and lon is not None:
                                            if -90 <= lat <= 90 and -180 <= lon <= 180:
                                                now = time.time()
                                                if mmsi not in vessels:
                                                    vessels[mmsi] = {}
                                                vessels[mmsi].update({
                                                    "mmsi": mmsi,
                                                    "lat": lat,
                                                    "lon": lon,
                                                    "sog": pr.get("Sog"),
                                                    "cog": pr.get("Cog"),
                                                    "nav_status": pr.get("NavigationalStatus"),
                                                    "name": meta.get("ShipName") or vessels[mmsi].get("name", ""),
                                                    "timestamp": now,
                                                })
                                                msg_count += 1
                                                if msg_count == 1:
                                                    log.info("aisstream: first PositionReport received")
                                                elif msg_count % 100 == 0:
                                                    log.info("aisstream: %d position reports, %d vessels", msg_count, len(vessels))
                            except (json.JSONDecodeError, KeyError) as e:
                                log.debug("Parse error: %s", e)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            log.error("aisstream WebSocket error: %s", ws.exception())
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break

        except asyncio.CancelledError:
            log.info("aisstream: stream cancelled")
            if connected_callback:
                connected_callback(False)
            raise
        except asyncio.TimeoutError as e:
            log.error("aisstream error: connection timeout - %s", e)
            if connected_callback:
                connected_callback(False)
        except aiohttp.ClientError as e:
            log.error("aisstream error: %s", e)
            if connected_callback:
                connected_callback(False)
        except Exception as e:
            log.error("aisstream error: %s", e)
            if connected_callback:
                connected_callback(False)
        log.info("aisstream: reconnecting in %s s...", reconnect_delay)
        await asyncio.sleep(reconnect_delay)


def get_tanker_snapshot(vessels: dict) -> list[dict]:
    """
    Build list of tankers from vessel state, filtering by type and max age.
    Returns list of {mmsi, name, lat, lon, sog, ...} for tankers only.
    """
    return get_vessel_snapshot(vessels, tankers_only=True)


def get_vessel_snapshot(vessels: dict, tankers_only: bool = False) -> list[dict]:
    """
    Build list of vessels from state, filtering by max age.
    If tankers_only: only vessels with ShipStaticData type 80-89.
    Otherwise: all vessels with position (includes those without type yet).
    """
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
        })
    return result
