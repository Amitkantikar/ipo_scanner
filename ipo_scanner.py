# ipo_scanner.py
import os
import requests
import pandas as pd
import yfinance as yf
from io import BytesIO

# --------------------------
# CONFIG
# --------------------------
MIN_DAYS = 60
THRESHOLD = 0.04  # 2% near ATH

# Read Telegram credentials from environment (GitHub Secrets)
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram credentials missing (BOT_TOKEN or CHAT_ID). Skipping send.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print("Telegram API returned", r.status_code, r.text[:200])
    except Exception as e:
        print("Failed to send telegram:", e)

# --------------------------
# Load NSE Symbols
# --------------------------
def get_all_mainboard_symbols():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    r = requests.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))
    df.columns = df.columns.str.strip()
    return df[df["SERIES"] == "EQ"]["SYMBOL"].tolist()

# --------------------------
# Batch OHLC for IPO detection
# --------------------------
def fetch_batch(symbols):
    tickers = [s + ".NS" for s in symbols]
    return yf.download(tickers, period="90d", interval="1d", group_by="ticker", threads=False)

def detect_ipos():
    symbols = get_all_mainboard_symbols()
    results = []
    batch_size = 40

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        data = fetch_batch(batch)
        # If single ticker, yfinance returns a single-level df — guard for that
        if isinstance(data.columns, pd.MultiIndex):
            available = list(data.columns.get_level_values(0).unique())
        else:
            # single ticker batch fallback
            available = [batch[0] + ".NS"] if batch else []

        for sym in batch:
            key = sym + ".NS"
            if key not in available:
                continue

            try:
                hist = data[key].dropna() if isinstance(data.columns, pd.MultiIndex) else data.dropna()
            except Exception:
                continue

            if len(hist) < MIN_DAYS:
                results.append(sym)

    return results

# --------------------------
# Get full ATH + ATH candle index
# --------------------------
def get_ath_and_index(symbol):
    hist = yf.Ticker(symbol + ".NS").history(period="max")
    if hist.empty:
        return None, None, 0
    ath = hist["High"].max()
    ath_index = hist["High"].idxmax()  # timestamp
    ath_pos = hist.index.get_loc(ath_index)  # integer position
    return ath, ath_pos, len(hist)

# --------------------------
# Get current price
# --------------------------
def get_current_price(symbol):
    data = yf.Ticker(symbol + ".NS").history(period="1d")
    if data.empty:
        return None
    return data["Close"].iloc[-1]

# --------------------------
# MAIN WORKFLOW
# --------------------------
if __name__ == "__main__":
    # quick validation
    if not BOT_TOKEN or not CHAT_ID:
        print("Warning: BOT_TOKEN or CHAT_ID not set in environment. Add them as GitHub Secrets.")
    print("🔎 Detecting IPO stocks…")
    ipos = detect_ipos()
    print("Found:", ipos)

    for sym in ipos:
        ath, ath_pos, total_candles = get_ath_and_index(sym)
        if not ath:
            continue

        # ATH must be at least 3 candles old
        if ath_pos > total_candles - 4:
            continue  # skip if ATH is too recent

        current = get_current_price(sym)
        if not current:
            continue

        # near ATH within THRESHOLD
        if current >= ath * (1 - THRESHOLD):
            diff = round(((ath - current) / ath) * 100, 2)

            msg = (
                f"🚨 *IPO Near All-Time High (3-candle rule passed)!*\n"
                f"*Symbol:* {sym}\n"
                f"*ATH:* {ath:.2f}\n"
                f"*CMP:* {current:.2f}\n"
                f"*Distance from ATH:* {diff}%"
            )

            print(f"Alert → {sym} (ATH {ath:.2f}, CMP {current:.2f}, diff {diff}%)")
            send_telegram(msg)

    print("\n✔ Scan complete!")
