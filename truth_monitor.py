# truth_monitor.py
import os
from dotenv import load_dotenv

# Load .env BEFORE importing truthbrush
load_dotenv()

# Force-map your custom env vars to the names truthbrush expects
if os.getenv("truth_access_token") and not os.getenv("TRUTHSOCIAL_TOKEN"):
    os.environ["TRUTHSOCIAL_TOKEN"] = os.getenv("truth_access_token")

if os.getenv("truth_username") and not os.getenv("TRUTHSOCIAL_USERNAME"):
    os.environ["TRUTHSOCIAL_USERNAME"] = os.getenv("truth_username")

if os.getenv("truth_password") and not os.getenv("TRUTHSOCIAL_PASSWORD"):
    os.environ["TRUTHSOCIAL_PASSWORD"] = os.getenv("truth_password")

import asyncio
import logging
import json
import re
from html import unescape
from itertools import islice
import truthbrush as tb

LAST_POST_FILE = "last_trump_post.json"
TRUMP_KEYWORDS = [
    "iran", "hormuz", "oil", "strait", "war", "attack", "blockade",
    "navy", "escort", "energy", "brent", "crude", "sanction"
]

def load_last_post():
    if os.path.exists(LAST_POST_FILE):
        try:
            with open(LAST_POST_FILE, "r") as f:
                data = json.load(f)
                return data.get("timestamp"), data.get("post_id")
        except Exception as e:
            logging.error(f"Failed to load last post: {e}")
            return None, None
    return None, None

def save_last_post(timestamp: str, post_id: int | str):
    try:
        with open(LAST_POST_FILE, "w") as f:
            json.dump({"timestamp": timestamp, "post_id": str(post_id)}, f)
    except Exception as e:
        logging.error(f"Failed to save last post: {e}")

def clean_truth_html(raw: str) -> str:
    raw = raw.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    raw = raw.replace("</p>", "\n").replace("<p>", "")
    raw = re.sub(r"<a [^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", r"\2 (\1)", raw, flags=re.I | re.S)
    raw = re.sub(r"<[^>]+>", "", raw)
    return unescape(raw).strip()

def _fetch_statuses():
    logging.info(
        "Truth env: username=%s password=%s token=%s",
        bool(os.getenv("TRUTHSOCIAL_USERNAME")),
        bool(os.getenv("TRUTHSOCIAL_PASSWORD")),
        bool(os.getenv("TRUTHSOCIAL_TOKEN")),
    )

    api = tb.Api()
    statuses_iter = api.pull_statuses("realDonaldTrump")
    return list(islice(statuses_iter, 3))

async def check_trump_posts(send_telegram_func):
    try:
        statuses = await asyncio.to_thread(_fetch_statuses)

        if not statuses:
            logging.warning("No statuses fetched from Truth Social")
            return

        last_time, last_id = load_last_post()

        for status in statuses:
            post_time = status.get("created_at")
            post_id = status.get("id")
            text = (status.get("plain_content") or status.get("content") or "").strip()

            if not text or not post_id or not post_time:
                continue

            is_new = True
            if last_id is not None and int(post_id) <= int(last_id):
                is_new = False
            elif last_time is not None and post_time <= last_time:
                is_new = False

            if is_new:
                if any(kw in text.lower() for kw in TRUMP_KEYWORDS):
                    clean_text = clean_truth_html(text)
                    message = (
                        f"🚨 <b>TRUMP TRUTH - POTENTIAL OIL/WAR IMPACT</b>\n\n"
                        f"{clean_text}\n\n"
                        f"🕒 {post_time}\n"
                        f"🔗 https://truthsocial.com/@realDonaldTrump/posts/{post_id}"
                    )
                    await send_telegram_func(message)
                    logging.info(f"Sent Trump alert for post {post_id}")

                save_last_post(post_time, post_id)
                break

    except Exception as e:
        logging.error(f"Trump Truth Social check failed: {type(e).__name__}: {e}")