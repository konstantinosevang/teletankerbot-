"""
Minimal tanker tracking for Strait of Hormuz.
- Live ledger of tankers (MMSI → zone, position, speed)
- Zone transitions → crossing alerts
- Heartbeat every 10 min
- AIS silence (> 20 min)
"""
import asyncio
import html
import json
import logging
import os
import ssl
import sys
from datetime import datetime, timezone

import aiohttp
import websockets
from aiohttp import web
from dotenv import load_dotenv

from db import (
    add_silence_alert,
    cleanup_position_updates,
    get_crossings,
    get_ledger,
    get_moving_vessels,
    get_position_updates,
    get_rate_events,
    get_stationary_vessels,
    init_db,
    insert_crossing,
    insert_position_update,
    load_vessel_cache,
    remove_ledger,
    remove_silence_alert,
    upsert_ledger,
    upsert_vessel,
)

# Logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
load_dotenv()

# --- Zones (lon-based) ---
# Persian Gulf: lon < 56.0
# Strait of Hormuz: 56.0 ≤ lon ≤ 56.5
# Gulf of Oman: lon > 56.5
# Lat filter: 24–30.5°N (full Persian Gulf + Strait)
ZONE_PG = "PG"
ZONE_STRAIT = "STRAIT"
ZONE_OMAN = "OMAN"
LON_PG_MAX = 56.0
LON_STRAIT_MAX = 56.5
LAT_MIN, LAT_MAX = 24.0, 30.5

# Gate for crossing detection (Oman ↔ Persian Gulf)
GATE_LON = 56.25

# AIS subscription bbox: Persian Gulf + Strait + Gulf of Oman (vessels anchor in both gulfs)
# Extends to 60°E to capture vessels stationed in Gulf of Oman waiting to enter
PERSIAN_GULF_BBOX = [[[24.0, 48.0], [30.5, 60.0]]]

# AIS ship type 80-89 = tankers
TANKER_TYPES = range(80, 90)

# Stationary = speed < 0.5 knots
STATIONARY_SPEED_KNOTS = 0.5

# Thresholds
AIS_SILENCE_MINUTES = 20
HEARTBEAT_INTERVAL = 20*60
RATE_WINDOW_HOURS = 24

WS_URL = "wss://stream.aisstream.io/v0/stream"
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# --- Tanker Ledger: MMSI → {lat, lon, zone, speed, last_seen, name, is_stationary} ---
# Loaded from DB on startup, persisted on each update
ledger = {}
startup_time = None  # Set when stream starts, used to suppress "new tanker" flood on startup
ais_connected = False  # True once we've received first AIS message
# vessel_info for name/type (from static data)
vessel_info = {}
# Message counters
msg_count = {"position": 0, "static": 0, "other": 0}
last_msg_time = {"t": None}


def in_tracking_region(lat, lon):
    """True if position is in tracking region (Gulf + Strait + Oman 24-30.5°N, 48-60°E)."""
    return LAT_MIN <= lat <= LAT_MAX and 48 <= lon <= 60


def get_zone(lon):
    """Return zone: PG, STRAIT, or OMAN."""
    if lon < LON_PG_MAX:
        return ZONE_PG
    if lon <= LON_STRAIT_MAX:
        return ZONE_STRAIT
    return ZONE_OMAN


def is_tanker(ship_type):
    if ship_type is None:
        return False
    try:
        return int(ship_type) in TANKER_TYPES
    except (TypeError, ValueError):
        return False


def _parse_ship_type(val):
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _msg_timestamp(meta):
    t = (meta or {}).get("time_utc")
    if not t:
        return None
    try:
        parts = str(t).strip().split()
        if len(parts) < 2:
            return None
        ts_str = f"{parts[0]} {parts[1]}"
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(ts_str, fmt)
                return parsed.replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
        return None
    except (ValueError, TypeError, IndexError):
        return None


def _process_tanker_position(mmsi, lat, lon, sog, name, ship_type, msg_ts):
    """Update ledger, persist to DB, detect zone change, emit alerts."""
    if not in_tracking_region(lat, lon):
        if mmsi in ledger:
            del ledger[mmsi]
            remove_ledger(mmsi)
        return

    # Include tankers (80-89) and unknown type (static data may arrive later)
    if ship_type is not None and not is_tanker(ship_type):
        return

    now_ts = msg_ts or datetime.now(timezone.utc).timestamp()
    new_zone = get_zone(lon)
    is_stationary = sog is None or sog < STATIONARY_SPEED_KNOTS
    prev = ledger.get(mmsi)
    prev_zone = prev["zone"] if prev else None

    entry = {
        "lat": lat,
        "lon": lon,
        "zone": new_zone,
        "speed": sog,
        "last_seen": now_ts,
        "name": name or "",
        "is_stationary": is_stationary,
    }
    ledger[mmsi] = entry

    # Persist to DB
    upsert_ledger(mmsi, lat, lon, new_zone, sog, now_ts, name or "", ship_type, is_stationary)
    insert_position_update(mmsi, lat, lon, new_zone, sog, now_ts, is_stationary)

    # Zone change → crossing event (only for confirmed tankers)
    if prev_zone is not None and prev_zone != new_zone:
        if is_tanker(ship_type):
            if new_zone == ZONE_PG and prev_zone in (ZONE_OMAN, ZONE_STRAIT):
                _emit_enter(mmsi, lat, lon, name)
            elif new_zone == ZONE_OMAN and prev_zone in (ZONE_PG, ZONE_STRAIT):
                _emit_exit(mmsi, lat, lon, name)
    elif prev_zone is None and is_tanker(ship_type):
        if startup_time and (now_ts - startup_time) > 300:
            _emit_new_tanker(mmsi, lat, lon, name)


def _emit_enter(mmsi, lat, lon, name):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    nm = (name or "").strip() or "-"
    log.info("TANKER ENTER | MMSI:%s %s | %.4f, %.4f", mmsi, nm, lat, lon)
    tg = f"🛢 <b>Tanker entered Gulf</b>\n{html.escape(nm)}\nMMSI: {mmsi}\n📍 {lat:.4f}, {lon:.4f}\n🕐 {ts}"
    asyncio.create_task(send_telegram(tg))
    insert_crossing(mmsi, "enter", nm, lat, lon, datetime.now(timezone.utc).timestamp())


def _emit_exit(mmsi, lat, lon, name):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    nm = (name or "").strip() or "-"
    log.info("TANKER EXIT | MMSI:%s %s | %.4f, %.4f", mmsi, nm, lat, lon)
    tg = f"🛢 <b>Tanker exited Gulf</b>\n{html.escape(nm)}\nMMSI: {mmsi}\n📍 {lat:.4f}, {lon:.4f}\n🕐 {ts}"
    asyncio.create_task(send_telegram(tg))
    insert_crossing(mmsi, "exit", nm, lat, lon, datetime.now(timezone.utc).timestamp())


def _emit_new_tanker(mmsi, lat, lon, name):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    nm = (name or "").strip() or "-"
    log.info("NEW TANKER | MMSI:%s %s | %.4f, %.4f", mmsi, nm, lat, lon)
    tg = f"🆕 <b>New tanker detected</b>\n{html.escape(nm)}\nMMSI: {mmsi}\n📍 {lat:.4f}, {lon:.4f}\n🕐 {ts}"
    asyncio.create_task(send_telegram(tg))


def count_by_zone():
    """Return (pg, strait, oman) counts from ledger."""
    pg = sum(1 for v in ledger.values() if v["zone"] == ZONE_PG)
    strait = sum(1 for v in ledger.values() if v["zone"] == ZONE_STRAIT)
    oman = sum(1 for v in ledger.values() if v["zone"] == ZONE_OMAN)
    return pg, strait, oman


def last_ais_age_minutes():
    """Minutes since last AIS message."""
    t = last_msg_time["t"]
    if not t:
        return None
    return (datetime.now(timezone.utc) - t).total_seconds() / 60


def _get_proxy():
    return os.getenv("proxy_url") or os.getenv("ws_proxy") or os.getenv("https_proxy") or os.getenv("WSS_PROXY") or os.getenv("HTTPS_PROXY")


async def send_telegram(text: str) -> bool:
    # Log to terminal what we send
    log.info("Telegram:\n%s", text)
    token = os.getenv("telegram_bot_token")
    chat_id = os.getenv("telegram_chat_id")
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                return r.status == 200
    except Exception:
        return False


async def stream_ais():
    api_key = os.getenv("aisstream_api_key")
    if not api_key:
        raise ValueError("Missing aisstream_api_key in .env")

    proxy = _get_proxy() or None
    attempt = 0
    while True:
        attempt += 1
        try:
            log.info("AISStream connect attempt #%d...", attempt)
            async with websockets.connect(WS_URL, ssl=ssl_context, open_timeout=120, proxy=proxy) as ws:
                log.info("WebSocket connected, sending subscription...")
                subscribe = {
                    "APIkey": api_key,
                    "BoundingBoxes": PERSIAN_GULF_BBOX,
                    "FilterMessageTypes": [
                        "PositionReport",
                        "ShipStaticData",
                        "ExtendedClassBPositionReport",
                        "StandardClassBPositionReport",
                        "StaticDataReport",
                    ],
                }
                await ws.send(json.dumps(subscribe))
                log.info("AISStream subscribed, receiving messages...")
                global startup_time, ais_connected
                startup_time = datetime.now(timezone.utc).timestamp()
                first_msg = True

                async for msg in ws:
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue
                    if "error" in data:
                        err = data["error"]
                        log.error("AISStream error: %s", err)
                        if "concurrent" in str(err).lower():
                            log.warning("Concurrent connection - waiting 60s")
                            await asyncio.sleep(60)
                        else:
                            await asyncio.sleep(10)
                        break
                    if first_msg:
                        log.info("First AIS message received - stream active")
                        first_msg = False
                        ais_connected = True

                    msg_type = data.get("MessageType")
                    meta = data.get("MetaData") or data.get("Metadata") or {}
                    inner = data.get("Message") or {}
                    msg_ts = _msg_timestamp(meta) or datetime.now(timezone.utc).timestamp()

                    if msg_type == "ShipStaticData":
                        msg_count["static"] += 1
                        last_msg_time["t"] = datetime.now(timezone.utc)
                        s = inner.get("ShipStaticData")
                        if s:
                            mmsi = s.get("UserID")
                            if mmsi is not None:
                                st = _parse_ship_type(s.get("Type"))
                                nm = s.get("Name") or meta.get("ShipName", "")
                                vessel_info[mmsi] = {"type": st, "name": nm}
                                upsert_vessel(mmsi, st, nm)

                    elif msg_type == "StaticDataReport":
                        msg_count["static"] += 1
                        last_msg_time["t"] = datetime.now(timezone.utc)
                        r = inner.get("StaticDataReport")
                        if r:
                            mmsi = r.get("UserID")
                            if mmsi is not None:
                                if mmsi not in vessel_info:
                                    vessel_info[mmsi] = {"type": None, "name": ""}
                                ra, rb = r.get("ReportA") or {}, r.get("ReportB") or {}
                                if ra.get("Valid") and ra.get("Name"):
                                    vessel_info[mmsi]["name"] = ra.get("Name", "") or vessel_info[mmsi]["name"]
                                if rb.get("Valid") and rb.get("ShipType") is not None:
                                    vessel_info[mmsi]["type"] = _parse_ship_type(rb.get("ShipType"))
                                upsert_vessel(mmsi, vessel_info[mmsi]["type"], vessel_info[mmsi]["name"])

                    elif msg_type == "ExtendedClassBPositionReport":
                        msg_count["position"] += 1
                        last_msg_time["t"] = datetime.now(timezone.utc)
                        r = inner.get("ExtendedClassBPositionReport")
                        if r:
                            mmsi, lat, lon = r.get("UserID"), r.get("Latitude"), r.get("Longitude")
                            if mmsi is not None and lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
                                remove_silence_alert(mmsi)
                                info = vessel_info.get(mmsi, {})
                                info["type"] = _parse_ship_type(r.get("Type") or info.get("type"))
                                info["name"] = r.get("Name") or info.get("name", "")
                                vessel_info[mmsi] = info
                                _process_tanker_position(mmsi, lat, lon, r.get("Sog"), info.get("name"), info.get("type"), msg_ts)

                    elif msg_type == "StandardClassBPositionReport":
                        msg_count["position"] += 1
                        last_msg_time["t"] = datetime.now(timezone.utc)
                        r = inner.get("StandardClassBPositionReport")
                        if r:
                            mmsi, lat, lon = r.get("UserID"), r.get("Latitude"), r.get("Longitude")
                            if mmsi is not None and lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
                                remove_silence_alert(mmsi)
                                info = vessel_info.get(mmsi, {})
                                _process_tanker_position(mmsi, lat, lon, r.get("Sog"), info.get("name"), info.get("type"), msg_ts)

                    elif msg_type == "PositionReport":
                        msg_count["position"] += 1
                        last_msg_time["t"] = datetime.now(timezone.utc)
                        r = inner.get("PositionReport")
                        if r:
                            mmsi, lat, lon = r.get("UserID"), r.get("Latitude"), r.get("Longitude")
                            if mmsi is not None and lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
                                remove_silence_alert(mmsi)
                                info = vessel_info.get(mmsi, {})
                                _process_tanker_position(mmsi, lat, lon, r.get("Sog"), info.get("name"), info.get("type"), msg_ts)

                    else:
                        msg_count["other"] += 1
                        last_msg_time["t"] = datetime.now(timezone.utc)

        except (TimeoutError, OSError) as e:
            log.warning("Connection lost: %s. Reconnecting in 10s...", e)
            await asyncio.sleep(10)


async def ais_silence_check():
    """Alert when tankers in tracking region stop transmitting > 20 min."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - AIS_SILENCE_MINUTES * 60
        for mmsi, v in list(ledger.items()):
            if v["last_seen"] >= cutoff:
                continue
            if add_silence_alert(mmsi):
                continue
            nm = (v.get("name") or "").strip() or "-"
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            tg = (
                f"🔇 <b>AIS silence</b>\n{html.escape(nm)}\nMMSI: {mmsi}\n"
                f"📍 Last: {v['lat']:.4f}, {v['lon']:.4f}\n"
                f"⏱ No signal {AIS_SILENCE_MINUTES}+ min\n🕐 {ts}"
            )
            await send_telegram(tg)
            del ledger[mmsi]
            remove_ledger(mmsi)


def _heartbeat_message():
    """Build heartbeat/status message."""
    pg, strait, oman = count_by_zone()
    stationary = len(get_stationary_vessels())
    moving = len(get_moving_vessels())
    enters, exits = get_rate_events(RATE_WINDOW_HOURS)
    age = last_ais_age_minutes()
    age_str = f"{age:.0f} min ago" if age is not None else "never"
    total_msgs = msg_count.get("position", 0) + msg_count.get("static", 0)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return (
        f"💓 <b>Heartbeat</b>\n"
        f"Vessels: {len(ledger)} (moving: {moving} | stationary: {stationary})\n"
        f"Persian Gulf: {pg} | Hormuz: {strait} | Oman: {oman}\n"
        f"Enters (24h): {len(enters)} | Exits (24h): {len(exits)}\n"
        f"AIS msgs: {total_msgs} | Last update: {age_str}\n{ts}"
    )


async def heartbeat():
    """Send first heartbeat once AIS is connected, then every interval."""
    # Wait for AIS connection before first message (avoid "0 vessels" spam)
    while not ais_connected:
        await asyncio.sleep(5)
    await send_telegram(_heartbeat_message())
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        await send_telegram(_heartbeat_message())


# --- HTTP ---
async def health(request):
    return web.Response(text="ok")


async def stats(request):
    pg, strait, oman = count_by_zone()
    stationary = len(get_stationary_vessels())
    moving = len(get_moving_vessels())
    enters, exits = get_rate_events(RATE_WINDOW_HOURS)
    return web.json_response({
        "vessels_tracked": len(ledger),
        "moving": moving,
        "stationary": stationary,
        "persian_gulf": pg,
        "hormuz": strait,
        "oman_gulf": oman,
        "enters_24h": len(enters),
        "exits_24h": len(exits),
        "msg_count": dict(msg_count),
        "last_msg_utc": last_msg_time["t"].isoformat() if last_msg_time["t"] else None,
    })


async def crossings_api(request):
    limit = min(int(request.query.get("limit", 100)), 500)
    rows = get_crossings(limit)
    for r in rows:
        r["ts"] = datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat()
    return web.json_response({"crossings": rows})


async def ledger_api(request):
    """Current vessel ledger (from DB)."""
    rows = [{"mmsi": m, **v} for m, v in ledger.items()]
    for r in rows:
        r["last_seen"] = datetime.fromtimestamp(r["last_seen"], tz=timezone.utc).isoformat()
    return web.json_response({"ledger": rows, "count": len(rows)})


async def stationary_api(request):
    """Vessels currently stationary (anchored/moored)."""
    rows = get_stationary_vessels()
    for r in rows:
        r["last_seen"] = datetime.fromtimestamp(r["last_seen"], tz=timezone.utc).isoformat()
    return web.json_response({"stationary": rows, "count": len(rows)})


async def moving_api(request):
    """Vessels currently moving."""
    rows = get_moving_vessels()
    for r in rows:
        r["last_seen"] = datetime.fromtimestamp(r["last_seen"], tz=timezone.utc).isoformat()
    return web.json_response({"moving": rows, "count": len(rows)})


async def activity_api(request):
    """Position history for activity/traffic analysis. ?mmsi=123&hours=24"""
    mmsi_raw = request.query.get("mmsi")
    try:
        mmsi = int(mmsi_raw) if mmsi_raw else None
    except ValueError:
        mmsi = None
    hours = float(request.query.get("hours", 24))
    limit = min(int(request.query.get("limit", 2000)), 10000)
    rows = get_position_updates(mmsi=mmsi, hours=hours, limit=limit)
    for r in rows:
        r["ts"] = datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat()
    return web.json_response({"activity": rows, "count": len(rows)})


def _log_startup(port):
    import subprocess
    log.info("=== Startup ===")
    log.info("Port: %d", port)
    try:
        r = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                parts = line.split()
                log.warning("Port %d in use by PID %s", port, parts[-1] if parts else "?")
                break
        else:
            log.info("Port %d free", port)
    except Exception:
        pass
    log.info("==============")


async def _cleanup_task():
    """Cleanup old position_updates every hour."""
    while True:
        await asyncio.sleep(3600)
        cleanup_position_updates()


async def start_background_tasks(app):
    init_db()
    vessel_info.update(load_vessel_cache())
    ledger.update(get_ledger())  # Load persisted ledger
    asyncio.create_task(stream_ais())
    asyncio.create_task(heartbeat())
    asyncio.create_task(ais_silence_check())
    asyncio.create_task(_cleanup_task())


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    _log_startup(port)
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/stats", stats)
    app.router.add_get("/crossings", crossings_api)
    app.router.add_get("/ledger", ledger_api)
    app.router.add_get("/stationary", stationary_api)
    app.router.add_get("/moving", moving_api)
    app.router.add_get("/activity", activity_api)
    app.on_startup.append(start_background_tasks)
    web.run_app(app, host="0.0.0.0", port=port)
