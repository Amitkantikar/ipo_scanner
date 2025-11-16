import pandas as pd
import requests
from datetime import datetime, timedelta
from io import BytesIO

def fetch_recent_non_sme(days=60):
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

    r = requests.get(url, timeout=20)
    df = pd.read_csv(BytesIO(r.content))

    # clean column names
    df.columns = [c.strip() for c in df.columns]

    # parse listing date
    df["DATE OF LISTING"] = pd.to_datetime(df["DATE OF LISTING"], errors="coerce")

    # filter only mainboard EQ (non-SME)
    df = df[df["SERIES"] == "EQ"]

    # filter recently listed IPOs
    cutoff = datetime.now() - timedelta(days=days)
    df = df[df["DATE OF LISTING"] >= cutoff]

    # select required columns
    df = df[["SYMBOL", "NAME OF COMPANY", "DATE OF LISTING"]]

    # sort newest first
    df = df.sort_values("DATE OF LISTING", ascending=False)

    df.to_csv("ipo_output.csv", index=False)
    print(df)

if __name__ == "__main__":
    fetch_recent_non_sme(60)
