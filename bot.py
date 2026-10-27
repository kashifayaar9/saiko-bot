import os
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

# ==============================
# SAIKO PRO TRADING BOT
# WITH AUTO-RETRY & RESULT TRACKING
# ==============================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

TZ = ZoneInfo("Asia/Karachi")
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"

SUBSCRIBERS_FILE = os.path.join(BASE_DIR, "saiko_subscribers.json")

# Pakistan Trading Sessions
SESSIONS = [
    ("SESSION 1", 12, 0),
    ("SESSION 2", 14, 0),
    ("SESSION 3", 18, 0),
    ("SESSION 4", 20, 0),
]

SESSION_DURATION_MINUTES = 60
REMINDER_MINUTES = 15
MAX_SIGNALS_PER_SESSION = 8

LIVE_PAIRS = [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "AUD/JPY",
    "EUR/GBP",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

subscribers = set()
offset = 0
running_session = False
active_session = ""
session_end = None
session_signals = 0
pair_index = 0

sent_signal_keys = set()
sent_reminders = set()
started_sessions = set()
daily_signal_count = 0
daily_date = datetime.now(TZ).date().isoformat()

state_lock = threading.Lock()


# ==============================
# DATABASE / SUBSCRIBERS
# ==============================

def load_subscribers():
    global subscribers
    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, list):
            subscribers = {int(item) for item in data}
    except (OSError, ValueError, TypeError):
        subscribers = set()


def save_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as file:
            json.dump(sorted(subscribers), file)
    except OSError as exc:
        logging.error("Subscriber save failed: %s", exc)


def add_subscriber(chat_id):
    subscribers.add(int(chat_id))
    save_subscribers()


# ==============================
# TELEGRAM HELPERS WITH AUTO-RETRY
# ==============================

def telegram(method, payload=None):
    if not BOT_TOKEN:
        return None

    url = f"{TELEGRAM_URL}/{method}"
    retries = 3

    for attempt in range(retries):
        try:
            response = requests.post(
                url,
                json=payload or {},
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            if not result.get("ok"):
                logging.error("Telegram API error: %s", result)
                return None
            return result.get("result")
        except Exception as exc:
            logging.warning("Telegram request failed (Attempt %d/%d): %s", attempt + 1, retries, exc)
            if attempt < retries - 1:
                time.sleep(3)  # Thoda wait karke dobara try karega
            else:
                logging.error("Telegram request failed completely after retries.")
    return None


def send_message(chat_id, text):
    return telegram(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
    )


def broadcast(text):
    for chat_id in list(subscribers):
        send_message(chat_id, text)
        time.sleep(0.1)


# ==============================
# TWELVE DATA WITH AUTO-RETRY
# ==============================

def get_candles(symbol, interval, outputsize=8):
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("Twelve Data API key missing.")

    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVE_DATA_API_KEY,
        "timezone": "UTC",
        "order": "ASC",
    }

    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(TWELVE_DATA_URL, params=params, timeout=25)
            response.raise_for_status()
            data = response.json()

            if "values" not in data:
                raise RuntimeError(data.get("message", "Candles data unavailable."))

            candles = []
            for item in data["values"]:
                try:
                    candles.append({
                        "datetime": item["datetime"],
                        "open": float(item["open"]),
                        "high": float(item["high"]),
                        "low": float(item["low"]),
                        "close": float(item["close"]),
                    })
                except (KeyError, ValueError, TypeError):
                    continue

            candles.sort(key=lambda c: c["datetime"])
            return candles
        except Exception as exc:
            logging.warning("Twelve Data fetch failed for %s (Attempt %d/%d): %s", symbol, attempt + 1, retries, exc)
            if attempt < retries - 1:
                time.sleep(3)
            else:
                raise exc


def get_current_price(symbol):
    try:
        candles = get_candles(symbol, "1min", 2)
        if candles:
            return candles[-1]["close"]
    except Exception:
        pass
    return None


def candle_pressure(candles):
    if len(candles) < 4:
        return "NEUTRAL"

    recent = candles[-3:]
    net_move = recent[-1]["close"] - recent[0]["open"]
    total_range = sum(max(c["high"] - c["low"], 0) for c in recent)

    if total_range <= 0:
        return "NEUTRAL"

    strength = abs(net_move) / total_range
    bullish = sum(c["close"] > c["open"] for c in recent)
    bearish = sum(c["close"] < c["open"] for c in recent)

    if net_move > 0 and bullish >= 2 and strength >= 0.18:
        return "BUY"
    if net_move < 0 and bearish >= 2 and strength >= 0.18:
        return "SELL"

    return "NEUTRAL"


def analyse_pair(symbol):
    try:
        candles_15m = get_candles(symbol, "15min", 8)
        candles_5m = get_candles(symbol, "5min", 8)
        candles_1m = get_candles(symbol, "1min", 8)
    except Exception as exc:
        logging.warning("API error for %s: %s", symbol, exc)
        return None

    if min(len(candles_15m), len(candles_5m), len(candles_1m)) < 4:
        return None

    closed_15m = candles_15m[:-1]
    closed_5m = candles_5m[:-1]
    closed_1m = candles_1m[:-1]

    if not closed_1m:
        return None

    latest_1m = closed_1m[-1]
    pressure_15m = candle_pressure(closed_15m)
    pressure_5m = candle_pressure(closed_5m)

    if pressure_15m != pressure_5m:
        return None

    if pressure_15m == "BUY":
        if latest_1m["close"] <= latest_1m["open"]:
            return None
        direction = "CALL / UP 🟢"
    elif pressure_15m == "SELL":
        if latest_1m["close"] >= latest_1m["open"]:
            return None
        direction = "PUT / DOWN 🔴"
    else:
        return None

    return {
        "symbol": symbol,
        "direction": direction,
        "price": latest_1m["close"],
        "pressure_15m": pressure_15m,
        "pressure_5m": pressure_5m,
    }


# ==============================
# TRADE RESULT TRACKER THREAD
# ==============================

def track_trade_result(symbol, direction, entry_price, duration_minutes=1):
    time.sleep(duration_minutes * 60)
    exit_price = get_current_price(symbol)

    if exit_price is None:
        return

    is_call = "UP" in direction
    if is_call:
        won = exit_price > entry_price
    else:
        won = exit_price < entry_price

    result_text = "✅ WIN (ITM)" if won else "❌ LOSS (OTM)"

    result_msg = (
        f"🎯 TRADE RESULT: {symbol}\n"
        f"DIRECTION: {direction}\n\n"
        f"📥 Entry Price: {entry_price:.5f}\n"
        f"📤 Exit Price: {exit_price:.5f}\n"
        f"RESULT: {result_text}"
    )
    broadcast(result_msg)


# ==============================
# SESSION & SIGNAL MANAGEMENT
# ==============================

def start_session(name, start, end):
    global running_session, active_session, session_end, session_signals
    with state_lock:
        if running_session:
            return
        running_session = True
        active_session = name
        session_end = end
        session_signals = 0

    broadcast(
        f"🔥 SAIKO PRO TRADING BOT 🔥\n\n"
        f"🟢 {name} STARTED\n"
        f"⏰ Time: {start.strftime('%I:%M %p')} PKT\n"
        f"📊 Market: LIVE FOREX ONLY\n\n"
        f"Multi-timeframe live analysis active."
    )


def finish_session():
    global running_session, active_session, session_end, session_signals
    if not running_session:
        return
    name = active_session
    count = session_signals

    broadcast(
        f"🔴 {name} CLOSED 🔴\n\n"
        f"📊 Signals sent in session: {count}\n"
        f"Session finished successfully."
    )
    with state_lock:
        running_session = False
        active_session = ""
        session_end = None
        session_signals = 0


def scan_for_signal():
    global session_signals, daily_signal_count, pair_index

    if not running_session or session_end is None:
        return
    if datetime.now(TZ) >= session_end or session_signals >= MAX_SIGNALS_PER_SESSION:
        finish_session()
        return

    symbol = LIVE_PAIRS[pair_index % len(LIVE_PAIRS)]
    pair_index += 1

    result = analyse_pair(symbol)
    if not result:
        return

    today_str = datetime.now(TZ).date().isoformat()
    signal_key = (today_str, symbol, result["price"], result["direction"])
    if signal_key in sent_signal_keys:
        return

    sent_signal_keys.add(signal_key)
    session_signals += 1
    daily_signal_count += 1

    trade_time = (datetime.now(TZ) + timedelta(minutes=1)).replace(second=0, microsecond=0)
    entry_price = result["price"]

    message = (
        f"🔥 SAIKO TRADING BOT 🔥\n\n"
        f"PAIR: {symbol} (LIVE)\n"
        f"DIRECTION: {result['direction']}\n"
        f"TRADE TIME: {trade_time.strftime('%I:%M %p')} PKT\n\n"
        f"📥 Trade Execute (Entry): {entry_price:.5f}\n"
        f"📊 15m/5m Pressure: {result['pressure_15m']}\n\n"
        f"⚠️ Live Forex analysis signal. Result will update in 1 minute."
    )
    broadcast(message)

    threading.Thread(
        target=track_trade_result,
        args=(symbol, result["direction"], entry_price, 1),
        daemon=True,
    ).start()


# ==============================
# TELEGRAM COMMANDS & POLLING
# ==============================

def handle_update(update):
    message = update.get("message")
    if not message or not message.get("text"):
        return
    chat_id = message.get("chat", {}).get("id")
    if chat_id is None:
        return

    text = message["text"].strip()
    command = text.split()[0].split("@")[0].lower()

    if command == "/start":
        add_subscriber(chat_id)
        send_message(
            chat_id,
            "🤖 Welcome to SAIKO PRO TRADING BOT\n\n"
            "⏰ Daily Sessions (PKT):\n"
            "1️⃣ 12:00 PM - 1:00 PM\n"
            "2️⃣ 2:00 PM - 3:00 PM\n"
            "3️⃣ 6:00 PM - 7:00 PM\n"
            "4️⃣ 8:00 PM - 9:00 PM\n\n"
            "Commands:\n"
            "/session - Check active status\n"
            "/status - Bot health check"
        )
    elif command in ["/session", "/status"]:
        add_subscriber(chat_id)
        status_text = (
            f"🔴 Active Session: {active_session if running_session else 'None'}\n"
            f"📊 Signals Sent Today: {daily_signal_count}\n"
            f"Market: LIVE FOREX ONLY"
        )
        send_message(chat_id, status_text)


def polling_loop():
    global offset
    while True:
        try:
            updates = telegram("getUpdates", {"offset": offset, "timeout": 20, "allowed_updates": ["message"]})
            if updates:
                for update in updates:
                    offset = update["update_id"] + 1
                    handle_update(update)
            else:
                time.sleep(3)
        except Exception as exc:
            logging.error("Polling loop error: %s", exc)
            time.sleep(5)


def scheduler_loop():
    global daily_signal_count, daily_date
    while True:
        now = datetime.now(TZ)
        today = now.date().isoformat()

        if today != daily_date:
            daily_date = today
            daily_signal_count = 0
            sent_signal_keys.clear()

        if now.weekday() < 5:
            for name, hour, minute in SESSIONS:
                start = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                end = start + timedelta(minutes=SESSION_DURATION_MINUTES)
                start_key = f"{today}-{name}-start"

                if start <= now < end and start_key not in started_sessions and not running_session:
                    started_sessions.add(start_key)
                    start_session(name, start, end)

            if running_session:
                if session_end and now >= session_end:
                    finish_session()
                else:
                    scan_for_signal()
        else:
            if running_session:
                finish_session()

        time.sleep(30)


def main():
    if not BOT_TOKEN or not TWELVE_DATA_API_KEY:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN or TWELVE_DATA_API_KEY in .env")

    me = telegram("getMe")
    if not me:
        raise SystemExit("Telegram connection failed. Check your token.")

    logging.info("Bot connected as @%s", me.get("username"))
    load_subscribers()

    threading.Thread(target=polling_loop, daemon=True).start()
    scheduler_loop()


if __name__ == "__main__":
    main()
