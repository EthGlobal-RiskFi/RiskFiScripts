import requests
import pandas as pd
import time
import os

BASE_URL = "https://api.coingecko.com/api/v3"
COINS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]
OUTPUT_FILE = "crypto_onchain_data.csv"

def fetch_market_data(coin_id, days="90"):
    """
    Fetch market chart data for a given coin.
    - days=1 → minute data
    - days=2–90 → hourly data
    - days>90 → daily data
    """
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    r = requests.get(url, params=params)

    if r.status_code != 200:
        print(f"Error fetching {coin_id}: {r.text}")
        return None

    data = r.json()
    prices = data.get("prices", [])
    market_caps = data.get("market_caps", [])
    total_volumes = data.get("total_volumes", [])

    df = pd.DataFrame(prices, columns=["timestamp", "price"])
    df["market_cap"] = [mc[1] for mc in market_caps]
    df["total_volume"] = [tv[1] for tv in total_volumes]
    df["coin"] = coin_id

    return df

def main():
    all_data = []

    for coin in COINS:
        print(f"Fetching data for {coin}...")
        df = fetch_market_data(coin, days="90")
        if df is not None:
            all_data.append(df)
        time.sleep(2)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df["timestamp"] = pd.to_datetime(final_df["timestamp"], unit="ms")

        if os.path.exists(OUTPUT_FILE):
            final_df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)
        else:
            final_df.to_csv(OUTPUT_FILE, index=False)

        print(f"Data saved to {OUTPUT_FILE}")
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
