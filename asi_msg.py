"""
AIS silence alerts - notify when vessels stop transmitting for 20+ minutes.
Use with AIS stream: pass ledger dict and send_telegram function.
"""
import asyncio
import html
from datetime import datetime, timezone

AIS_SILENCE_MINUTES = 20
CHECK_INTERVAL_SEC = 20 * 60  # Run check every 20 min


def format_ais_silence_message(mmsi: int, name: str, lat: float, lon: float, silence_minutes: int = 20) -> str:
    """Format the AIS silence Telegram message."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    nm = (name or "").strip() or "-"
    return (
        f"🔇 <b>AIS silence</b>\n"
        f"{html.escape(nm)}\n"
        f"MMSI: {mmsi}\n"
        f"📍 Last: {lat:.4f}, {lon:.4f}\n"
        f"⏱ No signal {silence_minutes}+ min\n"
        f"🕐 {ts}"
    )


async def ais_silence_check(
    ledger: dict,
    send_telegram_fn,
    add_silence_alert_fn,
    remove_ledger_fn,
    interval_sec: int = CHECK_INTERVAL_SEC,
    silence_minutes: int = AIS_SILENCE_MINUTES,
):
    """
    Background task: alert when tankers in ledger stop transmitting > silence_minutes.
    ledger: dict mmsi -> {lat, lon, last_seen, name, ...}
    send_telegram_fn: async fn(text) -> bool
    add_silence_alert_fn: fn(mmsi) -> bool (True = already alerted, skip)
    remove_ledger_fn: fn(mmsi) - remove from ledger and DB
    """
    while True:
        await asyncio.sleep(interval_sec)
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - silence_minutes * 60
        for mmsi, v in list(ledger.items()):
            if v.get("last_seen", 0) >= cutoff:
                continue
            if add_silence_alert_fn(mmsi):
                continue
            nm = (v.get("name") or "").strip() or "-"
            lat = v.get("lat", 0)
            lon = v.get("lon", 0)
            msg = format_ais_silence_message(mmsi, nm, lat, lon, silence_minutes)
            await send_telegram_fn(msg)
            del ledger[mmsi]
            remove_ledger_fn(mmsi)


# --- Integration example (when AIS stream is added) ---
# from asi_msg import ais_silence_check, format_ais_silence_message
# from db import add_silence_alert, remove_ledger
# asyncio.create_task(ais_silence_check(
#     ledger, send_telegram, add_silence_alert, remove_ledger
# ))