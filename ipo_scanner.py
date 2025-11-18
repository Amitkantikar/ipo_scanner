# ipo_scanner.py

import os
import logging
import warnings
from io import BytesIO
from time import sleep
import pickle

import requests
import pandas as pd
import yfinance as yf
import warnings


# ------------------------------------------------------------
# SUPPRESS FUTURE WARNINGS
# ------------------------------------------------------------
warnings.filterwarnings("ignore", category=FutureWarning)

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
MIN_DAYS = 120          # IPO detection threshold
THRESHOLD = 0.04        # 4% from ATH
BATCH_SIZE = 40         # batch size for yfinance
CACHE_FILE = "ipo_cache.pkl"

# Telegram
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("IPO-SCANNER")

# Persistent session for faster northbound requests
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


# ------------------------------------------------------------
# TELEGRAM
# ------------------------------------------------------------
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram credentials missing. Skipping message.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}

    for attempt in range(3):
        try:
            r = SESSION.post(url, data=payload, timeout=10)
            if r.status_code == 200:
                return
            log.warning(f"Telegram status {r.status_code}, retrying...")
            sleep(1 + attempt)
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")
            sleep(1 + attempt)


# ------------------------------------------------------------
# LOAD NSE SYMBOL LIST
# ------------------------------------------------------------
def get_all_mainboard_symbols():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    r = SESSION.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))
    df.columns = df.columns.str.strip()
    syms = df[df["SERIES"] == "EQ"]["SYMBOL"].tolist()
    log.info(f"Loaded {len(syms)} NSE EQ symbols")
    return syms


# ------------------------------------------------------------
# CACHE HELPERS
# ------------------------------------------------------------
def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {"ipo_symbols": [], "histories": {}}

    try:
        with open(CACHE_FILE, "rb") as f:
            cache = pickle.load(f)
        return cache
    except:
        return {"ipo_symbols": [], "histories": {}}


def save_cache(cache):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(cache, f)


# ------------------------------------------------------------
# IPO DETECTION USING 90-DAY HISTORY (FAST)
# ------------------------------------------------------------
def detect_ipos_90d():
    symbols = get_all_mainboard_symbols()
    ipo_list = []

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        tickers = [s + ".NS" for s in batch]

        try:
            data = yf.download(
                tickers,
                period="90d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False
            )
        except:
            continue

        if data.empty:
            continue

        if isinstance(data.columns, pd.MultiIndex):
            available = set(data.columns.get_level_values(0))
        else:
            available = {tickers[0]}  # single ticker mode

        for sym in batch:
            key = sym + ".NS"

            if key not in available:
                ipo_list.append(sym)
                continue

            df = data[key] if isinstance(data.columns, pd.MultiIndex) else data
            hist = df.dropna()

            if len(hist) < MIN_DAYS:
                ipo_list.append(sym)

    ipo_list = sorted(set(ipo_list))
    log.info(f"Detected {len(ipo_list)} IPO candidates: {ipo_list}")
    return ipo_list


# ------------------------------------------------------------
# FETCH MAX HISTORY ONLY FOR IPOs
# ------------------------------------------------------------
def fetch_max_histories(ipos, cache):
    missing = [s for s in ipos if s not in cache["histories"]]

    if not missing:
        return cache

    log.info(f"Downloading MAX history for {len(missing)} IPOs…")

    for i in range(0, len(missing), BATCH_SIZE):
        batch = missing[i : i + BATCH_SIZE]
        tickers = [s + ".NS" for s in batch]

        try:
            data = yf.download(
                tickers,
                period="max",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False
            )
        except:
            data = pd.DataFrame()

        for sym in batch:
            key = sym + ".NS"

            try:
                if isinstance(data.columns, pd.MultiIndex):
                    hist = data[key].dropna()
                else:
                    hist = data.dropna()
            except:
                hist = pd.DataFrame()

            cache["histories"][sym] = hist

    return cache


# ------------------------------------------------------------
# UPDATE LATEST CANDLES (5m -> fallback 1d)
# ------------------------------------------------------------
def update_latest_candles(cache):
    ipos = cache["ipo_symbols"]

    for sym in ipos:
        ticker = sym + ".NS"

        try:
            df_5m = yf.download(
                ticker,
                period="1d",
                interval="5m",
                auto_adjust=False,
                progress=False,
                threads=False
            )
            new_df = df_5m.dropna()
        except:
            new_df = pd.DataFrame()

        if new_df.empty:
            try:
                new_df = yf.Ticker(ticker).history(period="1d", auto_adjust=False)
            except:
                continue

        old = cache["histories"].get(sym, pd.DataFrame())

        combined = pd.concat([old, new_df])
        combined = combined[~combined.index.duplicated(keep="last")]
        cache["histories"][sym] = combined

    return cache


# ------------------------------------------------------------
# CHECK ATH + ALERT
# ------------------------------------------------------------
def check_ath(cache):
    ipos = cache["ipo_symbols"]

    for sym in ipos:
        hist = cache["histories"].get(sym)

        if hist is None or hist.empty:
            continue

        # Compute ATH
        ath = hist["High"].max()
        ath_idx = hist["High"].idxmax()

        # FIX: skip NaT errors
        if pd.isna(ath_idx):
            continue
        if ath_idx not in hist.index:
            continue

        ath_pos = hist.index.get_loc(ath_idx)
        total = len(hist)

        # ATH should be at least 3 candles old
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
                f"*Dist:* {diff}%"
            )

            log.info(f"ALERT -> {sym}")
            send_telegram(msg)


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
if __name__ == "__main__":
    log.info("Running IPO Scanner…")

    cache = load_cache()

    # Step 1: detect IPOs using 90-day window
    ipos = detect_ipos_90d()
    cache["ipo_symbols"] = ipos

    # Step 2: fetch MAX only for IPOs
    cache = fetch_max_histories(ipos, cache)
    save_cache(cache)

    # Step 3: update latest intraday
    cache = update_latest_candles(cache)
    save_cache(cache)

    # Step 4: check ATH + alerts
    check_ath(cache)

    log.info("Scan Complete ✔")
