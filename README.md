# TeleTankerBot

Fetches the Baltic Exchange ticker at set times, stores **BDTI** (Baltic Dirty Tanker Index), sends Telegram updates.

## Schedule (UTC)

Fetch and send at: **08:00**, **17:59**, **18:00**, **18:01**, **18:05**

## Setup

1. Create a Telegram bot via [@BotFather](https://t.me/BotFather)
2. Get your chat ID (message the bot, then use `getUpdates` API)
3. Copy `.env.example` to `.env` and set `telegram_bot_token`, `telegram_chat_id`

## Run

```bash
pip install -r requirements.txt
python app.py
```

## Test GUI

For manual testing without waiting for schedules:

```bash
python gui.py
```

Buttons: **BDTI** (fetch & send), **Trump** (check now), **Hormuz** (send snapshot). Log output appears in the window.

## Data

- **BDTI**: https://blacksun-api.balticexchange.com/api/ticker → stored in `teletanker.db`
- **Hormuz**: aisstream.io WebSocket → tanker snapshot in Strait (waiting/transiting)

## Deployment (custom)

Run on any machine that stays on (VPS, Raspberry Pi, etc.):

```bash
python app.py
```

Or use cron to run at schedule times (Linux) – create a small script that fetches once, then use cron entries for 08:00, 17:59, 18:00, 18:01, 18:05 UTC.
