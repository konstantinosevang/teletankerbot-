"""
Central configuration for teletankerbot.
All props, URLs, schedules, and message-related constants.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- Paths ---
ROOT = Path(__file__).resolve().parent
LAST_TRUMP_POST_FILE = ROOT / "last_trump_post.json"

# --- Telegram (from env) ---
TELEGRAM_BOT_TOKEN = os.getenv("telegram_bot_token")
TELEGRAM_CHAT_ID = os.getenv("telegram_chat_id")

# --- Baltic Exchange ---
TICKER_URL = "https://blacksun-api.balticexchange.com/api/ticker"
BDTI_INDEX_NAME = "BDTI"
# Scheduled times (hour, minute) UTC - fetch and send at these times
BDTI_SCHEDULE = [(8, 0), (17, 59), (18, 0), (18, 1), (18, 5)]

# --- Trump Truth Social ---
TRUMP_USERNAME = "realDonaldTrump"
TRUMP_KEYWORDS = [
    "iran", "hormuz", "oil", "strait", "war", "attack", "blockade",
    "navy", "escort", "energy", "brent", "crude", "sanction",
]
TRUMP_CHECK_INTERVAL_SEC = 180  # 3 minutes
