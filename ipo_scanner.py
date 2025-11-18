# ipo_scanner.py

import os
import pickle
import logging
from io import BytesIO
from time import sleep

import requests
import pandas as pd
import yfinance as yf

# --------------------------
# CONFIG
# --------------------------
MIN_DAYS = 90        # IPO = less than 90-day candles
THRESHOLD = 0.04
BATCH_SIZE = 40
CACHE_FILE = "ipo_cache.pkl"

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("IPO")


# --------------------------
# TELEGRAM
# --------------------------
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram credentials missing")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
    except:
        log.exception("Telegram send failed")


# --------------------------
# LOAD SYMBOLS
# --------------------------
def get_all_mainboard_symbols():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    r = requests.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))
    df.columns = df.columns.str.strip()
    return df[df["SERIES"] == "EQ"]["SYMBOL"].tolist()


# --------------------------
# LOAD / SAVE CACHE
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
# IPO DETECTION USING 90 DAYS ONLY (FAST)
# --------------------------
def detect_ipos_90d_only():
    symbols = get_all_mainboard_symbols()
    ipo_list = []

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        tickers = [s + ".NS" for s in batch]

        data = yf.download(
            tickers,
            period="90d",
            interval="1d",
            group_by="ticker",
            threads=False,
            progress=False
        )

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

            # 👉 CORE LOGIC YOU ASKED FOR
            # If stock has nearly full 90-day history, SKIP (not IPO)
            if len(hist) >= MIN_DAYS:
                continue

            # If very little / missing 90-day history → NEW IPO
            ipo_list.append(sym)

    return sorted(list(set(ipo_list)))


# --------------------------
# FETCH MAX HISTORY ONLY FOR IPOs
# --------------------------
def fetch_max_history(ipos, cache):
    missing = [s for s in ipos if s not in cache["histories"]]

    if not missing:
        return cache

    for i in range(0, len(missing), BATCH_SIZE):
        batch = missing[i : i + BATCH_SIZE]
        tickers = [s + ".NS" for s in batch]

        data = yf.download(
            tickers,
            period="max",
            interval="1d",
            group_by="ticker",
            threads=False,
            progress=False
        )

        for sym in batch:
            key = sym + ".NS"
            try:
                hist = data[key].dropna()
            except:
                hist = yf.Ticker(key).history(period="max")

            cache["histories"][sym] = hist

    return cache


# --------------------------
# UPDATE WITH LATEST 5-MIN / DAILY
# --------------------------
def update_latest_candles(cache):
    for sym in cache["ipo_symbols"]:
        ticker = sym + ".NS"

        try:
            df_5m = yf.download(
                ticker, period="1d", interval="5m",
                group_by="ticker", threads=False, progress=False
            )
            new = df_5m.dropna()
        except:
            new = pd.DataFrame()

        if new.empty:
            new = yf.Ticker(ticker).history(period="1d")

        if not new.empty:
            old = cache["histories"].get(sym, pd.DataFrame())
            merged = pd.concat([old, new])
            merged = merged[~merged.index.duplicated(keep="last")]
            cache["histories"][sym] = merged

    return cache


# --------------------------
# CHECK ATH + ALERT
# --------------------------
def check_ath(cache):
    for sym in cache["ipo_symbols"]:
        hist = cache["histories"].get(sym)
        if hist is None or hist.empty:
            continue

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
            log.info(f"Alert sent → {sym}")


# --------------------------
# MAIN
# --------------------------
if __name__ == "__main__":
    log.info("Running IPO scanner...")

    cache = load_cache()

    # Step 1: detect IPOs using 90-day history ONLY (VERY FAST)
    ipos = detect_ipos_90d_only()
    log.info(f"Found IPOs: {ipos}")

    cache["ipo_symbols"] = ipos

    # Step 2: fetch MAX history ONLY for IPOs
    cache = fetch_max_history(ipos, cache)
    save_cache(cache)

    # Step 3: intraday update
    cache = update_latest_candles(cache)
    save_cache(cache)

    # Step 4: ATH alert
    check_ath(cache)

    log.info("✔ Scan Complete")
