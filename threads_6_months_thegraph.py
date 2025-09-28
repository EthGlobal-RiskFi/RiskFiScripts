import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Ensure console output is UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# Use the correct and active Graph endpoint (replace with your valid one)
API_KEY = ""
SUBGRAPH_ID = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
GRAPH_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}"

QUERY = """
query($start: BigInt!, $end: BigInt!, $batch_size: Int!) {
  swaps(where: { timestamp_gte: $start, timestamp_lt: $end }, first: $batch_size, orderBy: timestamp, orderDirection: asc) {
    id
    timestamp
    transaction { id }
    pool { id liquidity volumeUSD txCount }
    token0 { symbol id }
    token1 { symbol id }
    sender
    recipient
    origin
    amount0
    amount1
    amountUSD
    sqrtPriceX96
    tick
    logIndex
  }
}
"""

def fetch_month_data(month_start: datetime, month_end: datetime):
    print(f"[START] Month {month_start.strftime('%Y-%m')}")
    swaps_list = []
    window_seconds = 3600 * 6
    start_ts = int(month_start.replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(month_end.replace(tzinfo=timezone.utc).timestamp())

    while start_ts < end_ts:
        chunk_end = min(start_ts + window_seconds, end_ts)
        try:
            resp = requests.post(GRAPH_URL, json={
                "query": QUERY,
                "variables": {
                    "start": start_ts,
                    "end": chunk_end,
                    "batch_size": 1000
                }
            })
            resp.raise_for_status()
            j = resp.json()
            if "errors" in j:
                raise Exception(j["errors"])
            chunk = j["data"].get("swaps", [])
            swaps_list.extend(chunk)
        except Exception as e:
            print(f"[WARN] in month {month_start.strftime('%Y-%m')} window {datetime.fromtimestamp(start_ts, timezone.utc)}–{datetime.fromtimestamp(chunk_end, timezone.utc)}: {e}")
            # skip that chunk
        start_ts = chunk_end + 1

    df = pd.DataFrame(swaps_list)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True)
        # flatten transaction
        df["tx"] = df["transaction"].apply(lambda x: x.get("id") if x else None)
        df = df.drop(columns=["transaction"])
    print(f"[DONE] Month {month_start.strftime('%Y-%m')} -> {len(df)} records")
    return df

def main():
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    six_months_ago = now - timedelta(days=180)

    # Build month ranges
    months = []
    cur = six_months_ago.replace(day=1)
    while cur < now:
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        months.append((cur, min(next_month, now)))
        cur = next_month

    all_dfs = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fetch_month_data, start, end): (start, end) for start, end in months}
        for fut in as_completed(futures):
            df = fut.result()
            if not df.empty:
                all_dfs.append(df)

    if all_dfs:
        final = pd.concat(all_dfs, ignore_index=True)
        final.to_csv("swaps_last6months.csv", index=False, encoding="utf-8")
        print("✅ Saved swaps_last6months.csv")
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
