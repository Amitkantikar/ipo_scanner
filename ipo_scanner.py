# ipo_scanner.py

import os
import logging
import pandas as pd
import yfinance as yf
import requests
from io import BytesIO
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
MIN_DAYS = 120
THRESHOLD = 0.04
BATCH_SIZE = 40

# Path where GitHub Actions will restore/save cache file
CACHE_FILE = "ipo_history.pkl"

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
# Load NSE symbols
# ------------------------------------------------
def get_all_mainboard_symbols():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    r = requests.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))
    df.columns = df.columns.str.strip()
    symbols = df[df["SERIES"] == "EQ"]["SYMBOL"].tolist()
    return symbols


# ------------------------------------------------
# Cache load/save
# ------------------------------------------------
def load_cache():
    if not os.path.exists(CACHE_FILE):
        log.info("No cache found — full history will be downloaded.")
        return {}

    try:
        cache = pd.read_pickle(CACHE_FILE)
        log.info(f"Loaded cache with {len(cache)} symbols")
        return cache
    except Exception as e:
        log.error(f"Cache load failed: {e}")
        return {}


def save_cache(cache):
    pd.to_pickle(cache, CACHE_FILE)
    log.info("Cache updated")


# ------------------------------------------------
# MAX history fetch
# ------------------------------------------------
def download_max_history(symbols):
    data = yf.download(
        [s + ".NS" for s in symbols],
        period="max",
        interval="1d",
        group_by="ticker",
        threads=False,
        progress=False
    )

    out = {}
    for sym in symbols:
        key = sym + ".NS"
        try:
            out[sym] = data[key].dropna()
        except:
            out[sym] = pd.DataFrame()

    return out


# ------------------------------------------------
# Daily candle update (fast)
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
# Detect real IPOs using full MAX history
# ------------------------------------------------
def detect_real_ipos(cache, symbols):
    ipo_list = []
    for sym in symbols:
        hist = cache.get(sym)
        if hist is None or hist.empty:
            continue
        if len(hist) < MIN_DAYS:
            ipo_list.append(sym)
    return ipo_list


# ------------------------------------------------
# ATH logic for IPOs
# ------------------------------------------------
def process_near_ath(ipo_list, cache):
    for sym in ipo_list:
        hist = cache.get(sym)
        if hist is None or hist.empty:
            continue

        total = len(hist)
        ath = hist["High"].max()
        ath_time = hist["High"].idxmax()
        ath_pos = hist.index.get_loc(ath_time)

        if ath_pos > total - 4:
            continue

        current = hist["Close"].iloc[-1]

        if current >= ath * (1 - THRESHOLD):
            diff = round(((ath - current) / ath) * 100, 2)
            msg = (
                f"🚨 *IPO Near ATH!*\n"
                f"*Symbol:* {sym}\n"
                f"*ATH:* {ath:.2f}\n"
                f"*CMP:* {current:.2f}\n"
                f"*Distance:* {diff}%"
            )
            log.info(f"Alert: {sym}")
            send_telegram(msg)


# ------------------------------------------------
# MAIN
# ------------------------------------------------
if __name__ == "__main__":
    log.info("🔍 Starting IPO Near ATH Scanner")

    symbols = get_all_mainboard_symbols()

    cache = load_cache()

    # Download MAX history only once
    missing = [s for s in symbols if s not in cache]
    if missing:
        log.info(f"{len(missing)} symbols missing in cache — downloading MAX history.")
        new = download_max_history(missing)
        cache.update(new)
        save_cache(cache)

    # Refresh last daily candle
    cache = update_last_candle(cache, symbols)

    ipo_list = detect_real_ipos(cache, symbols)

    process_near_ath(ipo_list, cache)

    log.info("✔ Scan complete")
