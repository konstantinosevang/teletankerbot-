"""
Teletankerbot - fetches BDTI, monitors Trump Truth Social.
"""
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

import aiohttp

from dotenv import load_dotenv
from db import init_db, insert_bdti
from truth_monitor import check_trump_posts
from props import (
    TICKER_URL,
    BDTI_INDEX_NAME,
    BDTI_SCHEDULE,
    TRUMP_CHECK_INTERVAL_SEC,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


async def send_telegram(text: str) -> bool:
    """Send message via Telegram Bot API."""
    token = os.getenv("telegram_bot_token")
    chat_id = os.getenv("telegram_chat_id")
    if not token or not chat_id:
        log.warning("Missing telegram_bot_token or telegram_chat_id")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                status = r.status
                response_text = await r.text()
                log.info("Telegram API response: status=%s, body=%s", status, response_text)
                if status != 200:
                    log.error("Telegram send failed with status %s: %s", status, response_text)
                return status == 200
    except Exception as e:
        log.error("Telegram send exception: %s", e)
        return False


async def fetch_ticker() -> list[dict]:
    """Fetch ticker from Baltic Exchange API."""
    async with aiohttp.ClientSession() as session:
        async with session.get(TICKER_URL, timeout=aiohttp.ClientTimeout(total=15)) as r:
            r.raise_for_status()
            return await r.json()


def extract_bdti(data: list[dict]) -> dict | None:
    """Extract BDTI from ticker response."""
    for item in data:
        if item.get("indexName") == BDTI_INDEX_NAME:
            curr = item.get("current") or {}
            prev = item.get("previous") or {}
            return {
                "value": curr.get("value"),
                "previous": prev.get("value"),
                "index_date": curr.get("indexDate"),
            }
    return None


def _format_bdti_message(value: float, previous: float | None, index_date: str) -> str:
    date_str = index_date.split("T")[0] if index_date else ""
    if previous is not None:
        change = value - previous
        pct = (change / previous) * 100
        sign = "+" if change >= 0 else ""
        change_line = f"Change: {sign}{change:.0f} ({sign}{pct:.2f}%) today"
    else:
        change_line = "Change: —"
    return (
        f"🚢 <b>BDTI Update</b>\n\n"
        f"Index: {value:.0f}\n"
        f"{change_line}\n\n"
        f"Date: {date_str}"
    )


async def fetch_store_and_send():
    """Fetch ticker, store BDTI, send Telegram."""
    try:
        data = await fetch_ticker()
        bdti = extract_bdti(data)
        if not bdti or bdti.get("value") is None:
            log.warning("BDTI not found in ticker response")
            return
        value = float(bdti["value"])
        previous = float(bdti["previous"]) if bdti.get("previous") is not None else None
        index_date = bdti.get("index_date") or ""
        insert_bdti(value, previous, index_date)
        log.info("BDTI stored: %.2f (prev: %s) @ %s", value, previous, index_date)
        msg = _format_bdti_message(value, previous, index_date)
        await send_telegram(msg)
    except Exception as e:
        log.error("Fetch/store/send failed: %s", e)


def _seconds_until_next_schedule() -> float:
    """Seconds until next scheduled time (UTC)."""
    now = datetime.now(timezone.utc)
    candidates = []
    for h, m in BDTI_SCHEDULE:
        target = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        candidates.append((target - now).total_seconds())
    return min(candidates)


async def run_scheduled_loop():
    init_db()
    log.info("BDTI schedule (UTC): %s", [f"{h:02d}:{m:02d}" for h, m in BDTI_SCHEDULE])

    log.info("Startup: fetching BDTI and sending...")
    await fetch_store_and_send()

    async def trump_loop():
        while True:
            await check_trump_posts(send_telegram)
            await asyncio.sleep(TRUMP_CHECK_INTERVAL_SEC)

    asyncio.create_task(trump_loop())
    log.info("Trump Truth Social monitoring started (every %s s)", TRUMP_CHECK_INTERVAL_SEC)

    while True:
        delay = _seconds_until_next_schedule()
        log.info("Next BDTI run in %.0f s (at %s UTC)", delay, datetime.now(timezone.utc).strftime("%H:%M"))
        await asyncio.sleep(delay)
        await fetch_store_and_send()


if __name__ == "__main__":
    asyncio.run(run_scheduled_loop())
