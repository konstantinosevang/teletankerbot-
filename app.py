"""
Track tankers entering/exiting the Strait of Hormuz.
Uses AISStream API - subscribes to PositionReport and ShipStaticData.
Sends Telegram notifications for each crossing.
"""
import asyncio
import html
import json
import os
import ssl
import sys
from datetime import datetime, timezone

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Strait of Hormuz bounding box
STRAIT_OF_HORMUZ_BBOX = [[[25.7, 55.3], [26.9, 57.3]]]

# Gate longitude: crossing west = into Persian Gulf, east = to Gulf of Oman
GATE_LON = 56.25

# AIS ship type 80-89 = tankers
TANKER_TYPES = range(80, 90)

WS_URL = "wss://stream.aisstream.io/v0/stream"

# SSL workaround for corporate proxies/firewalls (see vessel-tracking example)
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


def is_tanker(ship_type):
    return ship_type is not None and ship_type in TANKER_TYPES


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

    vessel_info = {}  # MMSI -> {type, name}
    last_pos = {}     # MMSI -> (lat, lon)

    proxy = _get_proxy() or None
    max_retries = 3
    for attempt in range(max_retries):
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
                    "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
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
                                    asyncio.create_task(send_telegram(tg_text))
            return
        except (TimeoutError, OSError) as e:
            if attempt < max_retries - 1:
                wait = 5 * (attempt + 1)
                print(f"Connection attempt {attempt + 1} failed: {e}. Retrying in {wait}s...", file=sys.stderr)
                await asyncio.sleep(wait)
            else:
                print("\nConnection failed. Troubleshooting:", file=sys.stderr)
                print("  - Corporate firewall may block WebSockets. Try from home/mobile hotspot.", file=sys.stderr)
                print("  - If behind a proxy, add to .env: proxy_url=http://proxy:port", file=sys.stderr)
                print("  - For SOCKS5: proxy_url=socks5://proxy:1080 (requires: pip install python-socks[asyncio])", file=sys.stderr)
                raise


if __name__ == "__main__":
    asyncio.run(stream_tanker_crossings())
