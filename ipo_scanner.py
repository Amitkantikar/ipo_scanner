# ipo_scanner.py

import os
import logging
import pandas as pd
import yfinance as yf
import requests
from io import BytesIO
from datetime import datetime
from time import sleep

# ------------------------------------------------
# LOGGING
# ------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("IPO")

# ------------------------------------------------
# CONFIG
# ------------------------------------------------
MIN_DAYS = 120               # Real IPO = less than 120 candles in MAX history
THRESHOLD = 0.04             # 4% near ATH
BATCH_SIZE = 40              # Batch size for yfinance calls
CACHE_FILE = "ipo_history.pkl"   # Cache stored inside GitHub Actions repo workspace

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# ------------------------------------------------
# Telegram Sender
# ------------------------------------------------
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram credentials missing.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}

    for attempt in range(3):
        try:
            r = requests.post(url, data=payload, timeout=10)
            if r.status_code == 200:
                return
            log.warning(f"Telegram HTTP {r.status_code}, retrying...")
            sleep(1 + attempt)
        except Exception as e:
            log.error(f"Telegram send failed: {e}")
            sleep(1 + attempt)


# ------------------------------------------------
# Load Mainboard NSE Symbols
# ------------------------------------------------
def get_all_mainboard_symbols():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    r = requests.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))
    df.columns = df.columns.str.strip()
    syms = df[df["SERIES"] == "EQ"]["SYMBOL"].tolist()
    log.info(f"Loaded {len(syms)} NSE symbols")
    return syms


# ------------------------------------------------
# Load Cache if exists
# ------------------------------------------------
def load_cache():
    if not os.path.exists(CACHE_FILE):
        log.info("Cache not found. First run will be slow (fetching full MAX history).")
        return {}

    try:
        cache = pd.read_pickle(CACHE_FILE)
        log.info(f"Loaded history cache for {len(cache)} tickers")
        return cache
    except Exception as e:
        log.error(f"Cache load failed: {e}")
        return {}


# ------------------------------------------------
# Save Cache
# ------------------------------------------------
def save_cache(cache):
    pd.to_pickle(cache, CACHE_FILE)
    log.info("Saved updated history cache.")


# ------------------------------------------------
# Batch download MAX history
# ------------------------------------------------
def download_max_history(symbols):
    log.info(f"Downloading MAX history for {len(symbols)} tickers...")

    data = yf.download(
        [s + ".NS" for s in symbols],
        period="max",
        interval="1d",
        group_by="ticker",
        threads=False,
        progress=False
    )

    hist_map = {}
    for sym in symbols:
        key = sym + ".NS"
        try:
            hist_map[sym] = data[key].dropna()
        except:
            hist_map[sym] = pd.DataFrame()

    return hist_map


# ------------------------------------------------
# Update only last candle daily (fast)
# ------------------------------------------------
def update_last_candle(cache, symbols):
    for sym in symbols:
        try:
            latest = yf.Ticker(sym + ".NS").history(period="1d")
            if not latest.empty:
                cache[sym] = pd.concat([cache[sym], latest]).drop_duplicates()
        except:
            pass
    save_cache(cache)
    return cache


# ------------------------------------------------
# IPO detection using FULL MAX history (Correct Method)
# ------------------------------------------------
def detect_real_ipos(cache, symbols):
    ipo_list = []

    for sym in symbols:
        hist = cache.get(sym)

        if hist is None or hist.empty:
            continue

        # Real IPO if full MAX history < MIN_DAYS
        if len(hist) < MIN_DAYS:
            ipo_list.append(sym)

    log.info(f"Real IPOs detected: {ipo_list}")
    return ipo_list


# ------------------------------------------------
# ATH Logic
# ------------------------------------------------
def process_near_ath(ipo_list, cache):
    for sym in ipo_list:
        hist = cache.get(sym)
        if hist is None or hist.empty:
            continue

        total = len(hist)
        ath = hist["High"].max()
        ath_idx = hist["High"].idxmax()
        ath_pos = hist.index.get_loc(ath_idx)

        # ATH must be at least 3 candles old
        if ath_pos > total - 4:
            continue

        current = hist["Close"].iloc[-1]

        if current >= ath * (1 - THRESHOLD):
            diff = round(((ath - current) / ath) * 100, 2)

            msg = (
                f"🚨 *IPO Near All-Time High!*\n"
                f"*Symbol:* {sym}\n"
                f"*ATH:* {ath:.2f}\n"
                f"*CMP:* {current:.2f}\n"
                f"*Distance:* {diff}%"
            )

            log.info(f"ALERT → {sym} (diff {diff}%)")
            send_telegram(msg)


# ------------------------------------------------
# MAIN WORKFLOW
# ------------------------------------------------
if __name__ == "__main__":
    log.info("🔍 Starting IPO Near ATH Scanner...")

    # Load all listed symbols
    symbols = get_all_mainboard_symbols()

    # Load cache if available
    cache = load_cache()

    # Step 1: Download missing MAX data once
    missing = [s for s in symbols if s not in cache]
    if missing:
        log.info(f"{len(missing)} tickers missing in cache. Downloading MAX history...")
        new_histories = download_max_history(missing)
        cache.update(new_histories)
        save_cache(cache)

    # Step 2: Update today's candles (fast)
    log.info("Refreshing last daily candle...")
    cache = update_last_candle(cache, symbols)

    # Step 3: Detect REAL IPOs
    ipo_list = detect_real_ipos(cache, symbols)

    # Step 4: Check IPOs near ATH
    process_near_ath(ipo_list, cache)

    log.info("✔ Scan completed successfully.")
