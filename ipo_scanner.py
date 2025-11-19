#!/usr/bin/env python3
"""
Optimized IPO ATH scanner.

Features:
- Batch history download via yfinance.download (faster)
- Requests session with retries for NSE CSV and Telegram
- LRU cache for NSE CSV
- Clear ATH / candles-since-ATH logic
- CLI flags for min-listing-days, threshold, dry-run
- Logging instead of prints
- Defensive error handling
"""

from __future__ import annotations
import os
import argparse
import logging
from dataclasses import dataclass
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------
# Config / defaults
# -----------------------
DEFAULT_MIN_LISTING_DAYS = 120
DEFAULT_THRESHOLD = 0.05  # fraction (0.025 = within 2.5% of ATH)
YF_SUFFIX = ".NS"  # Yahoo ticker suffix for NSE

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------
# HTTP session with retries
# -----------------------
def make_session(total_retries: int = 3, backoff: float = 1.0) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = make_session()


# -----------------------
# Simple helpers / dataclasses
# -----------------------
@dataclass
class ATHInfo:
    ath: float
    ath_index: pd.Timestamp
    candles_since_ath: int
    total_candles: int


def escape_markdown(text: str) -> str:
    """
    Minimal escaping for Telegram Markdown (v2); adjust if using MarkdownV1.
    """
    # escape characters that might break Markdown v2
    for ch in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, "\\" + ch)
    return text


# -----------------------
# NSE Equity CSV
# -----------------------
@lru_cache(maxsize=1)
def fetch_equity_list_csv() -> pd.DataFrame:
    """
    Returns NSE EQUITY_L.csv as DataFrame. Cached in memory for lifetime of process.
    """
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    logger.debug("Fetching NSE CSV from %s", url)
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(pd.io.common.BytesIO(r.content))
    df.columns = df.columns.str.strip()
    return df


def get_recent_ipos(min_listing_days: int) -> List[str]:
    """
    Return list of SYMBOLs that have 'DATE OF LISTING' >= cutoff.
    Only include SERIES == 'EQ'.
    """
    df = fetch_equity_list_csv()
    if "DATE OF LISTING" not in df.columns or "SYMBOL" not in df.columns:
        raise RuntimeError("EQUITY_L.csv missing expected columns")

    df["DATE OF LISTING"] = pd.to_datetime(df["DATE OF LISTING"], errors="coerce")
    df = df.dropna(subset=["DATE OF LISTING"])
    df = df[df["SERIES"].str.strip().eq("EQ")]

    cutoff = datetime.now(timezone.utc) - timedelta(days=min_listing_days)
    recent = df[df["DATE OF LISTING"].dt.tz_localize(None) >= cutoff.replace(tzinfo=None)]
    symbols = recent["SYMBOL"].str.strip().unique().tolist()
    logger.info("Found %d recent EQ symbols listed within %d days", len(symbols), min_listing_days)
    return symbols


# -----------------------
# Fetch histories (batch)
# -----------------------
def batch_fetch_histories(symbols: List[str]) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Uses yfinance.download to fetch multiple tickers at once.
    Returns a mapping from symbol -> DataFrame or None if missing.
    """
    if not symbols:
        return {}

    yf_tickers = [s + YF_SUFFIX for s in symbols]
    logger.info("Downloading history for %d tickers (batch)...", len(yf_tickers))

    # yfinance returns:
    # - If multiple tickers: MultiIndex columns (ticker, field)
    # - If single ticker: regular columns
    raw = yf.download(tickers=yf_tickers, period="max", group_by="ticker", threads=True, progress=False)

    results: Dict[str, Optional[pd.DataFrame]] = {}
    # helper to extract single ticker frame
    def extract_frame(raw_df: pd.DataFrame, yf_ticker: str) -> Optional[pd.DataFrame]:
        # If multiindex columns present
        if isinstance(raw_df.columns, pd.MultiIndex):
            # Some tickers might be missing and not present in columns
            if yf_ticker in raw_df.columns.levels[0]:
                df = raw_df[yf_ticker].copy()
                df.index = pd.to_datetime(df.index)
                return df
            else:
                return None
        else:
            # Only one ticker was downloaded; assume it's the one requested
            df = raw_df.copy()
            df.index = pd.to_datetime(df.index)
            return df

    for sym in symbols:
        yf_tk = sym + YF_SUFFIX
        try:
            frame = extract_frame(raw, yf_tk)
            # If empty DataFrame or missing essential columns -> None
            if frame is None or frame.empty or "High" not in frame.columns or "Close" not in frame.columns:
                results[sym] = None
            else:
                # Ensure datetime index sorted ascending
                frame = frame.sort_index()
                results[sym] = frame
        except Exception as e:
            logger.warning("Error extracting history for %s: %s", sym, e)
            results[sym] = None

    return results


# -----------------------
# ATH computation
# -----------------------
def compute_ath_from_hist(hist: pd.DataFrame) -> Optional[ATHInfo]:
    """
    Given OHLC dataframe (with 'High' column), return ATHInfo or None.
    """
    if hist is None or hist.empty:
        return None
    if "High" not in hist.columns:
        return None

    try:
        ath = float(hist["High"].max())
        ath_idx = hist["High"].idxmax()
        total = len(hist)
        # locate position (0-based)
        ath_pos = hist.index.get_loc(ath_idx)
        candles_since_ath = total - ath_pos - 1
        return ATHInfo(ath=ath, ath_index=ath_idx, candles_since_ath=candles_since_ath, total_candles=total)
    except Exception as e:
        logger.debug("compute_ath failure: %s", e)
        return None


# -----------------------
# Telegram
# -----------------------
def send_telegram_message(text: str) -> bool:
    """
    Send message via Telegram Bot API. Returns True if OK.
    """
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("Telegram credentials missing. Set BOT_TOKEN and CHAT_ID env vars.")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = SESSION.post(url, data=payload, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error("Telegram send failed: %s", e)
        return False


# -----------------------
# Main scanning routine
# -----------------------
def scan_ipos(min_listing_days: int, threshold: float, dry_run: bool = False) -> List[Tuple[str, float]]:
    """
    Scan recent IPOs and alert if current price is within 'threshold' fraction of ATH.
    Returns list of (symbol, percent_distance) that triggered.
    """
    symbols = get_recent_ipos(min_listing_days)
    if not symbols:
        logger.info("No recent IPOs found. Exiting.")
        return []

    histories = batch_fetch_histories(symbols)
    alerts: List[Tuple[str, float]] = []

    for sym, hist in histories.items():
        logger.debug("Processing %s", sym)
        if hist is None:
            logger.info("No history for %s (skipping)", sym)
            continue

        ath_info = compute_ath_from_hist(hist)
        if not ath_info:
            logger.info("ATH unavailable for %s (skipping)", sym)
            continue

        # Ensure at least 3 candles since ATH (your rule)
        if ath_info.candles_since_ath < 3:
            logger.debug("ATH too recent for %s (candles_since_ath=%d). Skipping.", sym, ath_info.candles_since_ath)
            continue

        # Current market price (last Close)
        current = float(hist["Close"].iloc[-1])
        ath = ath_info.ath

        # distance from ATH in percent (positive if ATH > current)
        distance_pct = ((ath - current) / ath) * 100.0

        # If price is above ATH (distance < 0), treat as 0 distance (or skip)
        if distance_pct < 0:
            logger.debug("%s current price above ATH (%.2f > %.2f). Skipping.", sym, current, ath)
            continue

        if current >= ath * (1.0 - threshold):
            logger.info("ALERT %s: within %.2f%% of ATH", sym, distance_pct)
            alerts.append((sym, round(distance_pct, 2)))

            # prepare message
            listing_date = hist.index[0].date() if len(hist) else ""
            msg = (
                f"<b>🚨 IPO Near All-Time High!</b><br>"
                f"<b>Symbol:</b> {sym}<br>"
                f"<b>Listing Date:</b> {listing_date}<br>"
                f"<b>ATH:</b> {ath:.2f}<br>"
                f"<b>CMP:</b> {current:.2f}<br>"
                f"<b>Distance from ATH:</b> {distance_pct:.2f}%<br>"
            )


            if not dry_run:
                send_telegram_message(msg)
            else:
                logger.info("Dry run enabled — not sending Telegram for %s", sym)

    return alerts


# -----------------------
# CLI entrypoint
# -----------------------
def parse_args():
    p = argparse.ArgumentParser(description="IPO ATH scanner")
    p.add_argument("--min-days", type=int, default=DEFAULT_MIN_LISTING_DAYS, help="minimum listing age in days")
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="fractional threshold (e.g. 0.025 for 2.5%)")
    p.add_argument("--dry-run", action="store_true", help="do not send Telegram messages, only log")
    p.add_argument("--debug", action="store_true", help="enable debug logging")
    return p.parse_args()


def main():
    args = parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.debug("Starting scan with args: %s", args)

    # quick env validation
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("BOT_TOKEN or CHAT_ID missing — running in limited mode (no Telegram).")

    try:
        alerts = scan_ipos(min_listing_days=args.min_days, threshold=args.threshold, dry_run=args.dry_run)
        logger.info("Scan complete. Alerts found: %d", len(alerts))
        for sym, pct in alerts:
            logger.info(" -> %s: %s%%", sym, pct)
    except Exception as e:
        logger.exception("Unhandled exception during scan: %s", e)


if __name__ == "__main__":
    main()
