import requests
import pandas as pd

def fetch_recent_non_sme():
    url = "https://groww.in/v1/api/ipo/v2/all"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, headers=headers, timeout=10)

    data = r.json()

    listed = data.get("listedIpos", [])

    df = pd.DataFrame(listed)

    # Filter mainboard (non-SME)
    df = df[df["isSmeIpo"] == False]

    df = df[[
        "companyName",
        "symbol",
        "listingDate",
        "issuePrice",
        "isSmeIpo"
    ]]

    df["listingDate"] = pd.to_datetime(df["listingDate"], errors="coerce")
    df = df.sort_values("listingDate", ascending=False)

    df.to_csv("ipo_output.csv", index=False)
    print(df)


if __name__ == "__main__":
    fetch_recent_non_sme()
