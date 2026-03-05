"""
Track tankers entering/exiting the Strait of Hormuz.
Uses AISStream API - subscribes to PositionReport and ShipStaticData.
Sends Telegram notifications for each crossing.
Runs as web service (for Render free tier) with minimal HTTP server for health checks.
"""
import asyncio
import html
import json
import os
import ssl
import sys
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Strait of Hormuz bounding box (slightly expanded for coverage)
STRAIT_OF_HORMUZ_BBOX = [[[25.5, 55.0], [27.0, 57.5]]]

# Gate longitude: crossing west = into Persian Gulf, east = to Gulf of Oman
GATE_LON = 56.25

# AIS ship type 80-89 = tankers
TANKER_TYPES = range(80, 90)

WS_URL = "wss://stream.aisstream.io/v0/stream"

# SSL workaround for corporate proxies/firewalls (see vessel-tracking example)
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Last activity time (crossing or "no activity" msg) - used for 10-min heartbeat
last_activity_time = {"t": None}

# Shared vessel data for in-strait count (updated by stream_tanker_crossings)
vessel_info = {}  # MMSI -> {type, name}
last_pos = {}     # MMSI -> (lat, lon)

BBOX_LAT_MIN, BBOX_LAT_MAX = 25.5, 27.0
BBOX_LON_MIN, BBOX_LON_MAX = 55.0, 57.5


def in_bbox(lat, lon):
    return BBOX_LAT_MIN <= lat <= BBOX_LAT_MAX and BBOX_LON_MIN <= lon <= BBOX_LON_MAX


def is_tanker(ship_type):
    return ship_type is not None and ship_type in TANKER_TYPES


def count_vessels_in_strait():
    """Count all vessels in strait bbox."""
    return sum(1 for mmsi, pos in last_pos.items() if in_bbox(pos[0], pos[1]))


def count_tankers_in_strait():
    """Count tankers in strait bbox (anchored, moored, or moving)."""
    return sum(
        1 for mmsi, pos in last_pos.items()
        if in_bbox(pos[0], pos[1]) and is_tanker(vessel_info.get(mmsi, {}).get("type"))
    )


def _get_proxy():
    return (
        os.getenv("proxy_url")
        or os.getenv("ws_proxy")
        or os.getenv("https_proxy")
        or os.getenv("WSS_PROXY")
        or os.getenv("HTTPS_PROXY")
    )


async def send_telegram(text: str) -> bool:
    """Send message to Telegram. Returns True on success."""
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


async def stream_tanker_crossings():
    api_key = os.getenv("aisstream_api_key")
    if not api_key:
        raise ValueError("Missing aisstream_api_key in .env")

    proxy = _get_proxy() or None
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ssl=ssl_context,
                open_timeout=120,
                proxy=proxy,
            ) as ws:
                subscribe = {
                    "APIKey": api_key,
                    "BoundingBoxes": STRAIT_OF_HORMUZ_BBOX,
                    "FilterMessageTypes": [
                        "PositionReport",
                        "ShipStaticData",
                        "ExtendedClassBPositionReport",
                        "StandardClassBPositionReport",
                        "StaticDataReport",
                    ],
                }
                await ws.send(json.dumps(subscribe))

                async for msg in ws:
                    data = json.loads(msg)
                    msg_type = data.get("MessageType")
                    meta = data.get("MetaData", {})

                    if msg_type == "ShipStaticData":
                        static = data["Message"]["ShipStaticData"]
                        mmsi = static["UserID"]
                        vessel_info[mmsi] = {
                            "type": static.get("Type"),
                            "name": static.get("Name") or meta.get("ShipName", ""),
                        }

                    elif msg_type == "StaticDataReport":
                        report = data["Message"]["StaticDataReport"]
                        mmsi = report["UserID"]
                        if mmsi not in vessel_info:
                            vessel_info[mmsi] = {"type": None, "name": ""}
                        report_a = report.get("ReportA") or {}
                        report_b = report.get("ReportB") or {}
                        if report_a.get("Valid") and report_a.get("Name"):
                            vessel_info[mmsi]["name"] = report_a.get("Name", "") or vessel_info[mmsi]["name"]
                        if report_b.get("Valid") and report_b.get("ShipType") is not None:
                            vessel_info[mmsi]["type"] = report_b.get("ShipType")

                    elif msg_type == "ExtendedClassBPositionReport":
                        report = data["Message"]["ExtendedClassBPositionReport"]
                        mmsi = report["UserID"]
                        lat, lon = report["Latitude"], report["Longitude"]
                        prev = last_pos.get(mmsi)
                        last_pos[mmsi] = (lat, lon)
                        if mmsi not in vessel_info:
                            vessel_info[mmsi] = {"type": None, "name": ""}
                        vessel_info[mmsi]["type"] = report.get("Type", vessel_info[mmsi].get("type"))
                        vessel_info[mmsi]["name"] = report.get("Name", "") or vessel_info[mmsi].get("name", "")
                        if prev is not None:
                            prev_lon = prev[1]
                            crossed_west = prev_lon > GATE_LON and lon < GATE_LON
                            crossed_east = prev_lon < GATE_LON and lon > GATE_LON
                            if crossed_west or crossed_east:
                                if is_tanker(vessel_info[mmsi].get("type")):
                                    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                                    direction = "ENTER (-> Persian Gulf)" if crossed_west else "EXIT (-> Gulf of Oman)"
                                    name = (vessel_info[mmsi].get("name") or "").strip() or "-"
                                    tg_text = f"🛢 <b>Tanker {direction.split()[0]}</b>\n{html.escape(name)}\nMMSI: {mmsi}\n📍 {lat:.4f}, {lon:.4f}\n🕐 {ts}"
                                    last_activity_time["t"] = datetime.now(timezone.utc)
                                    asyncio.create_task(send_telegram(tg_text))

                    elif msg_type == "StandardClassBPositionReport":
                        report = data["Message"]["StandardClassBPositionReport"]
                        mmsi = report["UserID"]
                        lat, lon = report["Latitude"], report["Longitude"]
                        last_pos[mmsi] = (lat, lon)

                    elif msg_type == "PositionReport":
                        report = data["Message"]["PositionReport"]
                        mmsi = report["UserID"]
                        lat, lon = report["Latitude"], report["Longitude"]

                        prev = last_pos.get(mmsi)
                        last_pos[mmsi] = (lat, lon)

                        if prev is not None:
                            prev_lon = prev[1]
                            crossed_west = prev_lon > GATE_LON and lon < GATE_LON
                            crossed_east = prev_lon < GATE_LON and lon > GATE_LON

                            if crossed_west or crossed_east:
                                info = vessel_info.get(mmsi, {})
                                if is_tanker(info.get("type")):
                                    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                                    direction = "ENTER (-> Persian Gulf)" if crossed_west else "EXIT (-> Gulf of Oman)"
                                    name = (info.get("name") or "").strip() or "-"
                                    msg = f"[{ts}] TANKER {direction} | MMSI:{mmsi} {name} | lat:{lat:.4f} lon:{lon:.4f}"
                                    print(msg)
                                    tg_text = f"🛢 <b>Tanker {direction.split()[0]}</b>\n{html.escape(name)}\nMMSI: {mmsi}\n📍 {lat:.4f}, {lon:.4f}\n🕐 {ts}"
                                    last_activity_time["t"] = datetime.now(timezone.utc)
                                    asyncio.create_task(send_telegram(tg_text))
        except (TimeoutError, OSError) as e:
            print(f"Connection lost: {e}. Reconnecting in 10s...", file=sys.stderr)
            await asyncio.sleep(10)


async def no_activity_heartbeat():
    """Send 'no activity' when app goes live, then every 10 minutes if no tanker crossings."""
    while True:
        last = last_activity_time["t"]
        elapsed = (datetime.now(timezone.utc) - last).total_seconds() if last else 9999
        if last is None or elapsed >= 600:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            total = count_vessels_in_strait()
            tankers = count_tankers_in_strait()
            await send_telegram(
                f"🕐 <b>No activity</b>\nStrait of Hormuz – no tanker crossings\n"
                f"🚢 <b>{total} vessels</b> in strait ({tankers} tankers)\n{ts}"
            )
            last_activity_time["t"] = datetime.now(timezone.utc)
        await asyncio.sleep(600)  # 10 minutes


async def health(request):
    """Health check for Render."""
    return web.Response(text="ok")


async def stats(request):
    """Debug: live vessel counts."""
    total = count_vessels_in_strait()
    tankers = count_tankers_in_strait()
    return web.json_response({"vessels_in_strait": total, "tankers_in_strait": tankers})


async def start_background_tasks(app):
    """Start AIS stream and no-activity heartbeat in background."""
    asyncio.create_task(stream_tanker_crossings())
    asyncio.create_task(no_activity_heartbeat())


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/stats", stats)
    app.on_startup.append(start_background_tasks)
    web.run_app(app, host="0.0.0.0", port=port)
