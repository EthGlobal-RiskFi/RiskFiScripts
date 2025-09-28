import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Ensure UTF-8 output in console
sys.stdout.reconfigure(encoding='utf-8')

# --- Configuration ---
API_KEY = ""
SUBGRAPH_ID = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
GRAPH_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}"

# Tokens to fetch (symbol -> ERC20 contract address)
TOKENS = {
    "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    "DOGE": "0xbA2aE424d960c26247Dd6c32edC70B295c744C43",
    "MATIC": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0",
    "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "UNI": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    "LINK": "0x514910771af9ca656af840dff83e8264ecf986ca",
    "AAVE": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
    "SUSHI": "0x6B3595068778DD592e39A122f4f5a5Cf09C90fE2",
    "COMP": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
    "TRUMP": "0xCaB66bc319C96A2921Caed30E990b20F2322357e",  # placeholder
    "DADDY": "0x42377e11CC93a515a6D78BCA77481b8DdEBC6Dfc",  # placeholder
    "SHIB": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE",
    "SOL": "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39"
}

# GraphQL query
QUERY = """
query($token: String!, $start: Int!, $end: Int!, $batch_size: Int!) {
  tokenDayDatas(
    where: { token: $token, date_gte: $start, date_lte: $end }
    first: $batch_size
    orderBy: date
    orderDirection: asc
  ) {
    date
    token { symbol id }
    priceUSD
    totalValueLockedUSD
    volumeUSD
  }
}
"""

def fetch_month_data(token_symbol: str, token_address: str, month_start: datetime, month_end: datetime):
    """Fetch all tokenDayDatas for a token for a specific month"""
    print(f"[START] {token_symbol} - {month_start.strftime('%Y-%m')}")
    results = []
    start_ts = int(month_start.replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(month_end.replace(tzinfo=timezone.utc).timestamp())
    batch_size = 1000

    while start_ts <= end_ts:
        try:
            resp = requests.post(
                GRAPH_URL,
                json={
                    "query": QUERY,
                    "variables": {
                        "token": token_address.lower(),
                        "start": start_ts,
                        "end": end_ts,
                        "batch_size": batch_size
                    }
                }
            )
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                raise Exception(data["errors"])
            entries = data.get("data", {}).get("tokenDayDatas", [])
            for e in entries:
                results.append({
                    "token": token_symbol,
                    "timestamp": e["date"],
                    "datetime": datetime.utcfromtimestamp(e["date"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "priceUSD": e.get("priceUSD"),
                    "totalValueLockedUSD": e.get("totalValueLockedUSD"),
                    "volumeUSD": e.get("volumeUSD")
                })
        except Exception as ex:
            print(f"[WARN] {token_symbol} - {month_start.strftime('%Y-%m')} window: {ex}")
        start_ts = end_ts + 1

    print(f"[DONE] {token_symbol} - {month_start.strftime('%Y-%m')} -> {len(results)} records")
    return results

def main():
    now = datetime.now(timezone.utc)
    one_year_ago = now - timedelta(days=365)

    # Build month ranges for the last 12 months
    months = []
    cur = one_year_ago.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur < now:
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        months.append((cur, min(next_month - timedelta(seconds=1), now)))
        cur = next_month

    all_data = []

    # Use ThreadPoolExecutor for each token-month combination
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for token_symbol, token_address in TOKENS.items():
            for month_start, month_end in months:
                futures.append(executor.submit(fetch_month_data, token_symbol, token_address, month_start, month_end))

        for fut in as_completed(futures):
            month_data = fut.result()
            if month_data:
                all_data.extend(month_data)

    # Convert to DataFrame and save CSV
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv("token_last12months_updated.csv", index=False, encoding="utf-8")
        print(f"✅ Saved token_last12months.csv with {len(df)} rows")
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
