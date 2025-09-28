import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

sys.stdout.reconfigure(encoding='utf-8')

API_KEY = "168ce640c99b820e2d260cd1b8e8be2f"
SUBGRAPH_ID = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
GRAPH_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}"

TARGET_TOKENS = {
    "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    # "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    # "DOGE": "0xbA2aE424d960c26247Dd6c32edC70B295c744C43",
    # "MATIC": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0",
    # "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    # "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    # "UNI": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    # "LINK": "0x514910771af9ca656af840dff83e8264ecf986ca",
    # "AAVE": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
    # "SUSHI": "0x6B3595068778DD592e39A122f4f5a5Cf09C90fE2",
    # "COMP": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
    # "TRUMP": "0x1234567890abcdef1234567890abcdef12345678",  # placeholder
    # "DADDY": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",  # placeholder
    # "SHIB": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE",
    # "SOL": "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39"
}

TARGET_TOKEN_ADDRESSES = set(addr.lower() for addr in TARGET_TOKENS.values())

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

def is_target_swap(swap_data):
    """Check if swap involves any of our target tokens"""
    try:
        token0_id = swap_data.get("token0", {}).get("id", "").lower()
        token1_id = swap_data.get("token1", {}).get("id", "").lower()
        return token0_id in TARGET_TOKEN_ADDRESSES or token1_id in TARGET_TOKEN_ADDRESSES
    except:
        return False

def fetch_month_data(month_start: datetime, month_end: datetime):
    print(f"[START] Month {month_start.strftime('%Y-%m')}")
    swaps_list = []
    window_seconds = 3600 * 6  # 6 hours
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
            
            # Filter for target tokens only
            filtered_chunk = [swap for swap in chunk if is_target_swap(swap)]
            swaps_list.extend(filtered_chunk)
            
            if filtered_chunk:
                print(f"  Found {len(filtered_chunk)} target token swaps in window {datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d %H:%M')}")
                
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
        
        # Add readable token symbols for easier analysis
        df["token0_symbol"] = df["token0"].apply(lambda x: x.get("symbol") if x else None)
        df["token1_symbol"] = df["token1"].apply(lambda x: x.get("symbol") if x else None)
        df["token0_id"] = df["token0"].apply(lambda x: x.get("id") if x else None)
        df["token1_id"] = df["token1"].apply(lambda x: x.get("id") if x else None)
        
        # Add pool info
        df["pool_id"] = df["pool"].apply(lambda x: x.get("id") if x else None)
        df["pool_liquidity"] = df["pool"].apply(lambda x: x.get("liquidity") if x else None)
        df["pool_volumeUSD"] = df["pool"].apply(lambda x: x.get("volumeUSD") if x else None)
        df["pool_txCount"] = df["pool"].apply(lambda x: x.get("txCount") if x else None)
        
        # Clean up nested columns
        df = df.drop(columns=["token0", "token1", "pool"])
    
    print(f"[DONE] Month {month_start.strftime('%Y-%m')} -> {len(df)} target token records")
    return df

def main():
    print(f"Fetching swaps for {len(TARGET_TOKENS)} target tokens:")
    for symbol, address in TARGET_TOKENS.items():
        print(f"  {symbol}: {address}")
    print()
    
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
        
        # Sort by timestamp
        final = final.sort_values('timestamp').reset_index(drop=True)
        
        # Save the data
        final.to_csv("target_token_swaps_last6months11.csv", index=False, encoding="utf-8")
        
        print(f"\n✅ Saved {len(final)} target token swaps to 'target_token_swaps_last6months.csv'")
        
        # Print some statistics
        if not final.empty:
            print(f"\nData Summary:")
            print(f"  Total swaps: {len(final)}")
            print(f"  Date range: {final['timestamp'].min()} to {final['timestamp'].max()}")
            print(f"  Unique pools: {final['pool_id'].nunique()}")
            print(f"  Total volume USD: ${final['amountUSD'].astype(float).sum():,.2f}")
            
            print(f"\nToken pair frequency (top 10):")
            token_pairs = final.apply(lambda row: f"{row['token0_symbol']}-{row['token1_symbol']}", axis=1)
            print(token_pairs.value_counts().head(10))
    else:
        print("No target token data fetched.")

if __name__ == "__main__":
    main()