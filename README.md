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

## Data

- **API**: https://blacksun-api.balticexchange.com/api/ticker
- **BDTI** stored in `teletanker.db`
