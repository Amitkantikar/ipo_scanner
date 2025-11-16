import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

def fetch_recent_non_sme():
    sitemap_url = "https://groww.in/sitemap/ipo_sitemap.xml"
    headers = {"User-Agent": "Mozilla/5.0"}

    r = requests.get(sitemap_url, headers=headers)
    soup = BeautifulSoup(r.text, "xml")

    urls = [loc.text for loc in soup.find_all("loc")]

    results = []

    for url in urls:
        page = requests.get(url, headers=headers)
        if page.status_code != 200:
            continue
        
        html = BeautifulSoup(page.text, "html.parser")

        # Extract embedded JSON-LD (IPO data)
        script = html.find("script", {"type": "application/ld+json"})
        if not script:
            continue

        try:
            data = eval(script.text)  # safe because groww uses JS objects
        except:
            continue

        # Extract fields
        company = data.get("name")
        listing_date = data.get("datePublished")
        symbol = data.get("tickerSymbol", "")
        is_sme = "SME" in company.upper()

        # Only non-SME
        if not is_sme:
            results.append([company, symbol, listing_date, is_sme])

    df = pd.DataFrame(results, columns=["company_name", "symbol", "listing_date", "is_sme"])
    df["listing_date"] = pd.to_datetime(df["listing_date"], errors="coerce")
    df = df.sort_values("listing_date", ascending=False)

    df.to_csv("ipo_output.csv", index=False)
    print(df)


if __name__ == "__main__":
    fetch_recent_non_sme()
