"""
Tanker activity feed: Persian Gulf + Oman Gulf via VesselAPI REST.
Tanker snapshots + Strait of Hormuz enter/exit alerts.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone

from .config import HORMUZ_WAITING_SPEED_KN, STATS_INTERVAL_SEC
from .vesselapi_client import get_tanker_snapshot, run_poller, zone_from_lon

from db import insert_crossing, upsert_vessels

log = logging.getLogger(__name__)

CROSSINGS_RETENTION_SEC = 48 * 3600  # Keep crossings for 48h, prune older


def format_enter_exit_message(mmsi: int, direction: str, v: dict) -> str:
    """Format tanker Strait of Hormuz enter/exit alert for Telegram."""
    name = v.get("name", "") or f"MMSI {mmsi}"
    vtype = v.get("vessel_type", "")
    flag = v.get("flag", "")
    dest = v.get("destination", "")
    eta = v.get("eta", "")
    sog = v.get("sog")
    sog_str = f"{sog:.1f} kn" if sog is not None else "—"
    lines = [
        f"🛢 <b>Tanker – Strait of Hormuz {direction}</b>\n",
        f"📛 {name}\n",
        f"🆔 MMSI: {mmsi}\n",
    ]
    if vtype:
        lines.append(f"📦 Type: {vtype}\n")
    if flag:
        lines.append(f"🚩 Flag: {flag}\n")
    if dest:
        lines.append(f"🎯 Dest: {dest}\n")
    if eta:
        lines.append(f"📅 ETA: {eta}\n")
    lines.append(f"⚡ SOG: {sog_str}\n")
    lines.append(f"\n🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    return "".join(lines)


def _classify_vessel(sog: float | None) -> str:
    """Low speed = waiting, high = transiting."""
    if sog is None:
        return "unknown"
    return "waiting" if sog < HORMUZ_WAITING_SPEED_KN else "transiting"


def format_snapshot_message(
    tankers: list[dict],
    crossings_24h: int,
    stream_connected: bool = False,
) -> str:
    """Format tanker activity snapshot: counts by zone + crossings in last 24h."""
    persian_gulf = sum(1 for v in tankers if zone_from_lon(v.get("lon")) == "persian_gulf")
    strait = sum(1 for v in tankers if zone_from_lon(v.get("lon")) == "strait")
    oman_gulf = sum(1 for v in tankers if zone_from_lon(v.get("lon")) == "oman_gulf")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"🛢 <b>Tanker Activity – Persian Gulf + Oman Gulf</b>\n",
        f"🇮🇷 Persian Gulf: {persian_gulf}\n",
        f"⛵ Strait of Hormuz: {strait}\n",
        f"🇴🇲 Oman Gulf: {oman_gulf}\n",
        f"📊 Total: {len(tankers)}\n",
        f"🔄 Crossed (24h): {crossings_24h}\n",
    ]
    if len(tankers) == 0 and crossings_24h == 0:
        status = "🟢" if stream_connected else "🟡"
        lines.append(f"{status} API: {'connected' if stream_connected else 'connecting...'}\n")
    lines.append(f"\n🕐 {ts}")
    return "".join(lines)


async def run_feed(send_telegram_fn):
    """
    Start VesselAPI poller, send vessel snapshot + Strait enter/exit alerts to Telegram.
    """
    vessels: dict = {}
    connected = False

    def set_connected(ok: bool):
        nonlocal connected
        connected = ok

    crossings: list[tuple[float, int]] = []  # (timestamp, mmsi) for crossings

    def on_enter_exit(mmsi: int, prev_zone: str, zone: str, direction: str, v: dict):
        try:
            insert_crossing(mmsi, prev_zone, zone, direction, v)
        except Exception as e:
            log.warning("Failed to store crossing: %s", e)
        msg = format_enter_exit_message(mmsi, direction, v)
        asyncio.create_task(send_telegram_fn(msg))
        log.info("Tanker Strait %s: %s (MMSI %s)", direction, v.get("name", mmsi), mmsi)

    # Poll every 60s; first poll starts immediately
    asyncio.create_task(
        run_poller(
            vessels,
            poll_interval=60,
            connected_callback=set_connected,
            enter_exit_callback=on_enter_exit,
            crossings_list=crossings,
        )
    )

    # Wait for first poll (expanded bbox + enrichment can take ~40–50s)
    for _ in range(55):  # up to 55 seconds
        await asyncio.sleep(1)
        if len(vessels) > 0:
            log.info("First poll complete: %d vessels", len(vessels))
            break
    connected = True
    log.info("Tanker activity feed started (VesselAPI, Strait enter/exit alerts)")

    last_send_ts = 0.0
    while True:
        try:
            tankers = get_tanker_snapshot(vessels)
            now = time.time()
            # Prune old crossings
            cutoff = now - CROSSINGS_RETENTION_SEC
            while crossings and crossings[0][0] < cutoff:
                crossings.pop(0)
            crossings_24h = sum(1 for t, _ in crossings if now - t < 24 * 3600)

            if (now - last_send_ts) >= STATS_INTERVAL_SEC:
                if not connected and len(tankers) == 0 and crossings_24h == 0:
                    log.info("Tanker feed: waiting for first poll before snapshot")
                else:
                    try:
                        upsert_vessels(vessels)
                    except Exception as e:
                        log.warning("Failed to store vessels: %s", e)
                    msg = format_snapshot_message(tankers, crossings_24h, connected)
                    await send_telegram_fn(msg)
                    last_send_ts = now
                    pg = sum(1 for v in tankers if zone_from_lon(v.get("lon")) == "persian_gulf")
                    st = sum(1 for v in tankers if zone_from_lon(v.get("lon")) == "strait")
                    og = sum(1 for v in tankers if zone_from_lon(v.get("lon")) == "oman_gulf")
                    log.info("Tanker snapshot sent: %d total (PG:%d Strait:%d OG:%d), %d crossed 24h", len(tankers), pg, st, og, crossings_24h)
        except Exception as e:
            log.error("Hormuz feed error: %s", e)
        await asyncio.sleep(60)  # Check every minute for next scheduled send
