# ipo_scanner.py
import os
import logging
import pickle
import requests
import pandas as pd
import yfinance as yf
from io import BytesIO
from time import sleep

# --------------------------
# CONFIG
# --------------------------
MIN_DAYS = 120
THRESHOLD = 0.04       # 4% from ATH
BATCH_SIZE = 40
CACHE_FILE = "ipo_cache.pkl"   # store only IPO symbols + their full history

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("IPO")


# --------------------------
# Telegram
# --------------------------
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("BOT_TOKEN or CHAT_ID missing")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}

    try:
        requests.post(url, data=payload, timeout=10)
    except Exception:
        log.exception("Failed to send Telegram")


# --------------------------
# Load NSE symbols
# --------------------------
def get_all_mainboard_symbols():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    r = requests.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))
    df.columns = df.columns.str.strip()
    return df[df["SERIES"] == "EQ"]["SYMBOL"].tolist()


# --------------------------
# Fast IPO Detection (Your 90-day logic)
# --------------------------
def detect_recent_ipos():
    symbols = get_all_mainboard_symbols()
    ipo_list = []

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        tickers = [s + ".NS" for s in batch]

        # 90-day fetch is fast
        data = yf.download(tickers, period="90d", interval="1d",
                           group_by="ticker", threads=False, progress=False)

        if data.empty:
            continue

        if isinstance(data.columns, pd.MultiIndex):
            available = set(data.columns.get_level_values(0).unique())
        else:
            available = {batch[0] + ".NS"}

        for sym in batch:
            key = sym + ".NS"
            if key not in available:
                continue

            df = data[key] if isinstance(data.columns, pd.MultiIndex) else data
            hist = df.dropna()

            # IPO logic (your original intention)
            if len(hist) < MIN_DAYS:
                ipo_list.append(sym)

    return sorted(list(set(ipo_list)))


# --------------------------
# Cache Load/Save
# --------------------------
def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {"ipo_symbols": [], "histories": {}}
    try:
        return pickle.load(open(CACHE_FILE, "rb"))
    except:
        return {"ipo_symbols": [], "histories": {}}


def save_cache(cache):
    pickle.dump(cache, open(CACHE_FILE, "wb"))


# --------------------------
# Fetch MAX history ONCE for IPO symbols only
# --------------------------
def fetch_max_for_ipos(ipos, cache):
    missing = [s for s in ipos if s not in cache["histories"]]

    if not missing:
        return cache

    log.info(f"Downloading MAX history for {len(missing)} new IPOs...")

    for i in range(0, len(missing), BATCH_SIZE):
        batch = missing[i:i + BATCH_SIZE]
        tickers = [s + ".NS" for s in batch]

        data = yf.download(tickers, period="max", interval="1d",
                           group_by="ticker", threads=False, progress=False)

        for sym in batch:
            key = sym + ".NS"
            try:
                hist = data[key].dropna()
            except:
                hist = yf.Ticker(key).history(period="max")
            cache["histories"][sym] = hist

    return cache


# --------------------------
# Update latest 5-min or daily candle
# --------------------------
def update_latest_candle(cache):
    for sym in cache["ipo_symbols"]:
        ticker = sym + ".NS"

        try:
            # 5-minute candles (fast intraday update)
            df_5m = yf.download(ticker, period="1d", interval="5m",
                                group_by="ticker", threads=False, progress=False)
            new = df_5m.dropna()
        except:
            new = pd.DataFrame()

        if new.empty:
            # fallback
            new = yf.Ticker(ticker).history(period="1d")

        if not new.empty:
            old = cache["histories"].get(sym, pd.DataFrame())
            combined = pd.concat([old, new])
            combined = combined[~combined.index.duplicated(keep="last")]
            cache["histories"][sym] = combined

    return cache


# --------------------------
# ATH logic + alert
# --------------------------
def check_ath_and_alert(cache):
    for sym in cache["ipo_symbols"]:
        hist = cache["histories"].get(sym)
        if hist is None or hist.empty:
            continue

        # ATH calculations
        ath = hist["High"].max()
        ath_idx = hist["High"].idxmax()
        ath_pos = hist.index.get_loc(ath_idx)
        total = len(hist)

        # 3-candle rule
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
                f"*Diff:* {diff}%"
            )
            send_telegram(msg)
            log.info(f"Alert sent for {sym}")


# --------------------------
# MAIN WORKFLOW
# --------------------------
if __name__ == "__main__":
    log.info("Starting IPO scanner...")

    cache = load_cache()

    # Step 1 — Detect IPOs fast (your logic)
    ipos = detect_recent_ipos()
    log.info(f"Detected IPOs: {ipos}")

    cache["ipo_symbols"] = ipos

    # Step 2 — Fetch MAX history ONLY for these IPOs (first run)
    cache = fetch_max_for_ipos(ipos, cache)
    save_cache(cache)

    # Step 3 — Update with latest candle (fast)
    cache = update_latest_candle(cache)
    save_cache(cache)

    # Step 4 — Check ATH + alert
    check_ath_and_alert(cache)

    log.info("Scan completed")
