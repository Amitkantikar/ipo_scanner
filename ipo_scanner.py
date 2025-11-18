# ipo_scanner_fast.py

import os
import logging
import pandas as pd
import yfinance as yf
import requests
from io import BytesIO
from datetime import datetime, timedelta

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

# Cache file (stored inside GitHub Actions workspace)
CACHE_FILE = "ipo_history.pkl"

# Telegram config
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")


# ------------------------------------------------
# Telegram Sender
# ------------------------------------------------
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Missing Telegram credentials")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        if r.status_code != 200:
            log.error(f"Telegram API error {r.status_code}")
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


# ------------------------------------------------
# Fetch NSE Symbols
# ------------------------------------------------
def get_all_mainboard_symbols():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    r = requests.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))
    df.columns = df.columns.str.strip()
    symbols = df[df["SERIES"] == "EQ"]["SYMBOL"].tolist()
    log.info(f"Loaded {len(symbols)} NSE symbols")
    return symbols


# ------------------------------------------------
# Detect IPOs (batch 90d)
# ------------------------------------------------
def fetch_batch(symbols):
    tickers = [s + ".NS" for s in symbols]
    return yf.download(
        tickers,
        period="90d",
        interval="1d",
        group_by="ticker",
        threads=False,
        progress=False
    )


def detect_ipos():
    symbols = get_all_mainboard_symbols()
    ipos = []

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        data = fetch_batch(batch)

        if data.empty:
            continue

        # Get available tickers
        if isinstance(data.columns, pd.MultiIndex):
            available = list(data.columns.get_level_values(0).unique())
        else:
            available = [batch[0] + ".NS"]

        for sym in batch:
            key = sym + ".NS"
            if key not in available:
                continue

            try:
                hist = data[key].dropna() if isinstance(data.columns, pd.MultiIndex) else data.dropna()
            except:
                continue

            if len(hist) < MIN_DAYS:
                ipos.append(sym)

    log.info(f"Detected IPO stocks: {len(ipos)}")
    return ipos


# ------------------------------------------------
# CACHE MANAGEMENT
# ------------------------------------------------
def load_cache():
    if not os.path.exists(CACHE_FILE):
        log.info("Cache not found. Full history will be downloaded.")
        return {}

    try:
        df_dict = pd.read_pickle(CACHE_FILE)
        log.info(f"Loaded cache with {len(df_dict)} tickers")
        return df_dict
    except Exception as e:
        log.error(f"Cache load failed: {e}")
        return {}


def save_cache(cache):
    pd.to_pickle(cache, CACHE_FILE)
    log.info("History cache updated.")


# ------------------------------------------------
# Fast history updater
# ------------------------------------------------
def update_history(cache, symbols):
    """Download ONLY missing/max history once, then only append new data"""

    missing = [s for s in symbols if s not in cache]

    if missing:
        log.info(f"Downloading MAX history for {len(missing)} new tickers")
        for i in range(0, len(missing), BATCH_SIZE):
            batch = missing[i:i + BATCH_SIZE]
            data = yf.download(
                [s + ".NS" for s in batch],
                period="max",
                interval="1d",
                group_by="ticker",
                threads=False,
                progress=False
            )

            for sym in batch:
                key = sym + ".NS"
                try:
                    cache[sym] = data[key].dropna()
                except:
                    cache[sym] = pd.DataFrame()

        save_cache(cache)

    # Update all symbols with ONLY the latest candle (super fast)
    log.info("Updating last daily candles for all cached tickers")

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
# MAIN WORKFLOW
# ------------------------------------------------
if __name__ == "__main__":
    log.info("🔍 Starting IPO Near ATH scan")

    ipos = detect_ipos()
    if not ipos:
        log.info("No IPO stocks found")
        exit()

    # Load cache (no max downloads if exists)
    history_cache = load_cache()

    # Update only missing & last candles
    history_cache = update_history(history_cache, ipos)

    # Process detection
    for sym in ipos:
        hist = history_cache.get(sym)
        if hist is None or hist.empty:
            continue

        total = len(hist)
        ath = hist["High"].max()
        ath_index = hist["High"].idxmax()
        ath_pos = hist.index.get_loc(ath_index)

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
                f"*Diff:* {diff}%"
            )

            log.info(f"ALERT → {sym} (diff {diff}%)")
            send_telegram(msg)

    log.info("✔ Scan complete")
