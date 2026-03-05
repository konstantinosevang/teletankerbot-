# TeleTankerBot

Tracks tankers entering and exiting the Strait of Hormuz via AIS data. Sends Telegram notifications for each crossing.

## Setup

1. **Clone and install**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your keys
   ```

   Required variables:
   - `aisstream_api_key` — from [aisstream.io](https://aisstream.io/authenticate)
   - `telegram_bot_token` — from [@BotFather](https://t.me/BotFather)
   - `telegram_chat_id` — your chat ID (message the bot, then get from `getUpdates` API)

3. **Run locally**
   ```bash
   python app.py
   ```

## Deploy to Render

1. Push this repo to GitHub.
2. In [Render Dashboard](https://dashboard.render.com), click **New** → **Background Worker**.
3. Connect your GitHub repo.
4. Render will detect `render.yaml` — or set manually:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python app.py`
5. Add environment variables in Render: `aisstream_api_key`, `telegram_bot_token`, `telegram_chat_id`.
