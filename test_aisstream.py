"""
Standalone script to test aisstream.io connection.
Run: python test_aisstream.py
"""
import asyncio
import json
import os

import aiohttp
from dotenv import load_dotenv

from marinetraffic.config import HORMUZ_BBOX

load_dotenv()

API_KEY = os.getenv("AISSTREAM_API_KEY", "")
WS_URL = "wss://stream.aisstream.io/v0/stream"


async def check_reachability():
    """Quick HTTPS check to see if aisstream.io is reachable (firewall/proxy test)."""
    print("Checking if aisstream.io is reachable (HTTPS)...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://aisstream.io", timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                print(f"  OK - got HTTP {r.status}\n")
                return True
    except Exception as e:
        print(f"  FAILED: {e}")
        print("  -> Possible firewall/proxy blocking outbound connections.\n")
        return False


async def test_connect():
    print("=== aisstream.io connection test ===\n")
    print(f"API key set: {'Yes' if API_KEY else 'No'}")
    if not API_KEY:
        print("ERROR: Set AISSTREAM_API_KEY in .env")
        return

    print(f"API key (first 8 chars): {API_KEY[:8]}...")
    print(f"URL: {WS_URL}")
    print(f"Bbox: {HORMUZ_BBOX}\n")

    if not await check_reachability():
        print("Skipping WebSocket test - fix connectivity first.")
        return

    subscription = {
        "APIKey": API_KEY,
        "BoundingBoxes": [HORMUZ_BBOX],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    # Shorter timeout so we fail faster if WebSocket is blocked (corporate firewalls often block wss://)
    timeout = aiohttp.ClientTimeout(total=30, connect=20)

    print("Connecting to WebSocket (max 20 sec)...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL, timeout=timeout) as ws:
                print("WebSocket connected! Sending subscription...")
                await ws.send_str(json.dumps(subscription))
                print("Subscription sent. Waiting for messages (max 30 sec)...\n")

                async def collect_messages():
                    count = 0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if "error" in data:
                                print(f"ERROR from server: {data['error']}")
                                return count
                            msg_type = data.get("MessageType", "?")
                            count += 1
                            if count <= 5:
                                print(f"  [{count}] {msg_type}")
                            elif count == 6:
                                print(f"  ... (more messages)")
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"WebSocket error: {ws.exception()}")
                            return count
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            print("WebSocket closed")
                            return count
                        if count >= 20:
                            print(f"\nReceived {count} messages - connection OK!")
                            return count
                    return count

                try:
                    count = await asyncio.wait_for(collect_messages(), timeout=30)
                    if count == 0:
                        print("No messages in 30 sec - Persian Gulf may have limited AIS coverage.")
                        print("Connection + subscription OK; stream can be sparse in this region.")
                    elif count < 20:
                        print(f"\nReceived {count} messages - connection OK!")
                except asyncio.TimeoutError:
                    print("No messages in 30 sec - Persian Gulf may have limited AIS coverage.")
                    print("Connection + subscription OK; stream can be sparse in this region.")
    except asyncio.CancelledError:
        raise  # Don't swallow - let event loop handle (e.g. Ctrl+C)
    except asyncio.TimeoutError as e:
        print(f"TIMEOUT: {e}")
        print("  -> WebSocket handshake timed out. Corporate firewalls often block wss://.")
    except aiohttp.ClientError as e:
        print(f"CLIENT ERROR: {e}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

    print("\n=== test done ===")


if __name__ == "__main__":
    asyncio.run(test_connect())
