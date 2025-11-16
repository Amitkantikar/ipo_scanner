import requests
import pandas as pd

def fetch_recent_non_sme():
    url = "https://api.chittorgarh.com/api/latest-ipos"
    headers = {"User-Agent": "Mozilla/5.0"}

    r = requests.get(url, headers=headers, timeout=10)
    data = r.json().get("data", [])

    df = pd.DataFrame(data)
    df = df[df["is_sme"] == False]

    df = df[[
        "company_name",
        "nse_symbol",
        "listing_date",
        "issue_price"
    ]]

    df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")
    df = df.sort_values("listing_date", ascending=False)

    df.to_csv("ipo_output.csv", index=False)
    print(df)

if __name__ == "__main__":
    fetch_recent_non_sme()
