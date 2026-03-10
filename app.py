"""
Teletankerbot - sends various message types via Telegram.
Logic lives in separate modules; app.py provides send_telegram and registry.
"""
import asyncio
import logging
import os

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# --- Message types this app sends (implemented in other modules) ---
MESSAGE_TYPES = {
    "BDTI",
    "AIS_SILENCE",
    # Add more as you implement them
}


async def send_telegram(text: str) -> bool:
    """Send message via Telegram Bot API."""
    log.info("Telegram:\n%s", text)
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
                return r.status == 200
    except Exception as e:
        log.error("Telegram send failed: %s", e)
        return False


async def main():
    log.info("Registered message types: %s", sorted(MESSAGE_TYPES))
    # Wire up your message sources here - e.g.:
    # asyncio.create_task(run_bdti_loop())
    # asyncio.create_task(ais_silence_check(ledger, send_telegram, ...))
    await asyncio.Future()  # run forever (add your tasks above)


if __name__ == "__main__":
    asyncio.run(main())
