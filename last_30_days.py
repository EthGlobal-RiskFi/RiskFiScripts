import requests
import time
import csv
from datetime import datetime, UTC

# Replace with your Graph API key
API_KEY = "168ce640c99b820e2d260cd1b8e8be2f"
SUBGRAPH_ID = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
GRAPH_API = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}"

def fetch_swaps_between(start_ts, end_ts, batch_size=1000):
    query = """
    query($start: BigInt!, $end: BigInt!, $batch_size: Int!) {
      swaps(
        first: $batch_size,
        where: { timestamp_gte: $start, timestamp_lte: $end },
        orderBy: timestamp,
        orderDirection: asc
      ) {
        id
        transaction { id }
        timestamp
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
    variables = {"start": start_ts, "end": end_ts, "batch_size": batch_size}
    resp = requests.post(GRAPH_API, json={"query": query, "variables": variables})
    if resp.status_code != 200:
        raise Exception(f"HTTP Error {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    if "errors" in data:
        raise Exception(f"GraphQL Error: {data['errors']}")
    return data.get("data", {}).get("swaps", [])

def main():
    now = int(time.time())
    thirty_days_ago = now - 30 * 24 * 60 * 60   # 30 days back
    filename = "swaps_last30days21.csv"

    # CSV header
    with open(filename, "w", newline="", encoding="utf-8", errors="replace") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "tx", "timestamp", "datetime",
            "pool", "liquidity", "volumeUSD", "txCount",
            "token0", "token1", "cryptoPair",
            "sender", "recipient", "origin",
            "amount0", "amount1", "amountUSD",
            "tokenFlow", "liquidityMovement",
            "sqrtPriceX96", "tick", "logIndex"
        ])

    # Pull data in 6h windows
    window_seconds = 3600 * 6
    start = thirty_days_ago

    while start < now:
        end = min(start + window_seconds, now)
        print(f"Fetching swaps from {datetime.fromtimestamp(start, UTC)} to {datetime.fromtimestamp(end, UTC)} ...")
        try:
            swaps = fetch_swaps_between(start, end)
        except Exception as e:
            print(f"⚠️ Error fetching batch: {e}. Skipping this window.")
            swaps = []

        with open(filename, "a", newline="", encoding="utf-8", errors="replace") as f:
            writer = csv.writer(f)
            for s in swaps:
                crypto_pair = f"{s['token0']['symbol']}/{s['token1']['symbol']}"
                token_flow = float(s["amount0"]) + float(s["amount1"])
                liquidity_movement = float(s["pool"]["liquidity"]) if s["pool"]["liquidity"] else 0

                writer.writerow([
                    s["id"],
                    s["transaction"]["id"],
                    s["timestamp"],
                    datetime.fromtimestamp(int(s["timestamp"]), UTC).strftime("%Y-%m-%d %H:%M:%S"),
                    s["pool"]["id"],
                    s["pool"]["liquidity"],
                    s["pool"]["volumeUSD"],
                    s["pool"]["txCount"],
                    s["token0"]["symbol"],
                    s["token1"]["symbol"],
                    crypto_pair,
                    s["sender"],
                    s["recipient"],
                    s["origin"],
                    s["amount0"],
                    s["amount1"],
                    s["amountUSD"],
                    token_flow,
                    liquidity_movement,
                    s["sqrtPriceX96"],
                    s["tick"],
                    s["logIndex"]
                ])
        start = end + 1
        time.sleep(0.3)

    print(f"✅ Done. Data saved to {filename}")

if __name__ == "__main__":
    main()
