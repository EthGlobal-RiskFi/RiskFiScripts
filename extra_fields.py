import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import time
import sys

class TokenDataFetcher:
    def __init__(self):
        # CoinGecko API endpoints
        self.base_url = "https://api.coingecko.com/api/v3"
        
        # Token addresses and their CoinGecko IDs
        self.tokens = {
            "USDC": {
                "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "coingecko_id": "usd-coin"
            },
            "USDT": {
                "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                "coingecko_id": "tether"
            },
            "UNI": {
                "address": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
                "coingecko_id": "uniswap"
            },
            "LINK": {
                "address": "0x514910771af9ca656af840dff83e8264ecf986ca",
                "coingecko_id": "chainlink"
            },
            "AAVE": {
                "address": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
                "coingecko_id": "aave"
            },
            "SUSHI": {
                "address": "0x6B3595068778DD592e39A122f4f5a5Cf09C90fE2",
                "coingecko_id": "sushi"
            },
            "COMP": {
                "address": "0xc00e94Cb662C3520282E6f5717214004A7f26888",
                "coingecko_id": "compound-governance-token"
            },
            "SHIB": {
                "address": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE",
                "coingecko_id": "shiba-inu"
            },
            # Note: TRUMP, DADDY, and SOL may need manual ID lookup or might not be available
            "SOL": {
                "address": "0x2b591e99afe9f32eaa6214f7b7629768c40eeb39",
                "coingecko_id": "solana"  # This might be wrapped SOL
            }
        }
        
        # Rate limiting
        self.last_request_time = 0
        self.min_interval = 3.0  # Increased to 3 seconds between requests
        self.max_retries = 3
        self.retry_delay = 10  # Wait 10 seconds before retry

    def rate_limit(self):
        """Implement rate limiting to avoid hitting API limits"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            print(f"Rate limiting... waiting {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def get_token_data_by_id(self, coingecko_id, days=7):
        """Fetch historical data for a token using CoinGecko ID with retry logic"""
        
        url = f"{self.base_url}/coins/{coingecko_id}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': days,
            'interval': 'daily'
        }
        
        for attempt in range(self.max_retries):
            self.rate_limit()
            
            try:
                response = requests.get(url, params=params, timeout=30)
                if response.status_code == 429:
                    print(f"  Rate limited on attempt {attempt + 1}, waiting {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                print(f"  Error on attempt {attempt + 1} for {coingecko_id}: {e}")
                if attempt < self.max_retries - 1:
                    print(f"  Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"  Failed after {self.max_retries} attempts")
                    
        return None

    def get_token_data_alternative(self, token_symbol, days=7):
        """Alternative method to get basic token data (simplified)"""
        # For testing - create mock data if API fails
        print(f"  Generating sample data for {token_symbol} (API unavailable)")
        
        import random
        from datetime import datetime, timedelta
        
        sample_data = []
        base_price = {
            'WETH': 2500, 'WBTC': 45000, 'USDC': 1.0, 'USDT': 1.0,
            'UNI': 8.5, 'LINK': 15.0, 'AAVE': 180, 'SUSHI': 1.2,
            'COMP': 55, 'SHIB': 0.000015, 'DOGE': 0.08, 'MATIC': 0.85,
            'SOL': 100
        }.get(token_symbol, 10)
        
        for i in range(days):
            date = datetime.now() - timedelta(days=days-1-i)
            price = base_price * (0.95 + random.random() * 0.1)  # +/- 5% variation
            volume = random.randint(1000000, 100000000)
            market_cap = price * random.randint(1000000, 500000000)
            
            sample_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'token_symbol': token_symbol,
                'token_address': self.tokens[token_symbol]['address'],
                'priceUSD': price,
                'dailyVolumeUSD': volume,
                'totalLiquidityUSD': market_cap
            })
        
        return sample_data

    def get_current_token_info(self, coingecko_id):
        """Get current token information including volume"""
        self.rate_limit()
        
        url = f"{self.base_url}/coins/{coingecko_id}"
        params = {
            'localization': 'false',
            'tickers': 'false',
            'market_data': 'true',
            'community_data': 'false',
            'developer_data': 'false'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching current info for {coingecko_id}: {e}")
            return None

    def process_market_chart_data(self, market_data, token_symbol, token_address):
        """Process CoinGecko market chart data"""
        if not market_data or 'prices' not in market_data:
            return []
        
        processed_data = []
        prices = market_data.get('prices', [])
        market_caps = market_data.get('market_caps', [])
        volumes = market_data.get('total_volumes', [])
        
        for i, price_data in enumerate(prices):
            timestamp = price_data[0] / 1000  # Convert from milliseconds
            date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            price_usd = price_data[1]
            
            # Get corresponding volume and market cap
            volume_usd = volumes[i][1] if i < len(volumes) else 0
            market_cap = market_caps[i][1] if i < len(market_caps) else 0
            
            processed_data.append({
                'date': date_str,
                'token_symbol': token_symbol,
                'token_address': token_address,
                'priceUSD': price_usd,
                'dailyVolumeUSD': volume_usd,
                'totalLiquidityUSD': market_cap  # Using market cap as proxy for liquidity
            })
        
        return processed_data

    def fetch_all_token_data(self):
        """Fetch data for all tokens with fallback options"""
        print(f"Fetching data for {len(self.tokens)} tokens from the last 7 days...")
        print("Using CoinGecko API with extended rate limiting...")
        print("This may take several minutes due to API restrictions...\n")
        
        all_data = []
        successful_tokens = []
        failed_tokens = []
        
        for i, (token_symbol, token_info) in enumerate(self.tokens.items()):
            coingecko_id = token_info['coingecko_id']
            token_address = token_info['address']
            
            print(f"[{i+1}/{len(self.tokens)}] Fetching data for {token_symbol} ({coingecko_id})...")
            
            # Try to get real data from CoinGecko
            market_data = self.get_token_data_by_id(coingecko_id, days=7)
            
            if market_data:
                token_data = self.process_market_chart_data(
                    market_data, token_symbol, token_address
                )
                all_data.extend(token_data)
                successful_tokens.append(token_symbol)
                print(f"[SUCCESS] Successfully fetched {len(token_data)} data points for {token_symbol}")
            else:
                # Use alternative/sample data if API fails
                print(f"[WARNING] API failed for {token_symbol}, using sample data...")
                sample_data = self.get_token_data_alternative(token_symbol, days=7)
                all_data.extend(sample_data)
                failed_tokens.append(f"{token_symbol} (sample)")
                print(f"[SAMPLE] Generated {len(sample_data)} sample data points for {token_symbol}")
        
        print(f"\n=== FETCH SUMMARY ===")
        print(f"Successfully fetched from API: {len(successful_tokens)} tokens")
        print(f"Using sample data: {len(failed_tokens)} tokens")
        if successful_tokens:
            print(f"API success: {', '.join(successful_tokens)}")
        if failed_tokens:
            print(f"Sample data: {', '.join(failed_tokens)}")
        
        return pd.DataFrame(all_data)

    def save_to_csv(self, df, filename="token_data_7days.csv"):
        """Save DataFrame to CSV file"""
        if not df.empty:
            # Sort by token symbol and date for better organization
            df_sorted = df.sort_values(['token_symbol', 'date'])
            df_sorted.to_csv(filename, index=False)
            print(f"Data saved to {filename}")
            print(f"CSV contains {len(df_sorted)} rows and {len(df_sorted.columns)} columns")
        else:
            print("No data to save")

    def save_formatted_csv(self, df, filename="formatted_token_data.csv"):
        """Save data in a more readable CSV format"""
        if df.empty:
            print("No data to save")
            return
        
        # Create a formatted version
        formatted_data = []
        for _, row in df.iterrows():
            formatted_data.append({
                'Token': row['token_symbol'],
                'Date': row['date'],
                'Price_USD': f"${row['priceUSD']:.6f}",
                'Daily_Volume_USD': f"${row['dailyVolumeUSD']:,.0f}",
                'Market_Cap_USD': f"${row['totalLiquidityUSD']:,.0f}",
                'Contract_Address': row['token_address']
            })
        
        formatted_df = pd.DataFrame(formatted_data)
        formatted_df.to_csv(filename, index=False)
        print(f"Formatted data saved to {filename}")
        return formatted_df

    def display_summary(self, df):
        """Display a summary of the fetched data"""
        if df.empty:
            print("No data to display")
            return
        
        print(f"\n=== DATA SUMMARY ===")
        print(f"Total records: {len(df)}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"Tokens with data: {sorted(df['token_symbol'].unique())}")
        print(f"Records per token:")
        for token in sorted(df['token_symbol'].unique()):
            count = len(df[df['token_symbol'] == token])
            print(f"  {token}: {count} days")
        
        print(f"\nSample data (latest prices):")
        latest_data = df.loc[df.groupby('token_symbol')['date'].idxmax()]
        latest_data = latest_data.sort_values('priceUSD', ascending=False)
        for _, row in latest_data.head(10).iterrows():
            print(f"  {row['token_symbol']}: ${row['priceUSD']:.4f} (Volume: ${row['dailyVolumeUSD']:,.0f})")

def main():
    # Initialize the fetcher
    fetcher = TokenDataFetcher()
    
    # Fetch data
    print("Starting token data fetch from CoinGecko...")
    print("Due to API rate limits, this will include sample data for demonstration.")
    print("For production use, consider getting a CoinGecko Pro API key.\n")
    
    df = fetcher.fetch_all_token_data()
    
    # Display results
    fetcher.display_summary(df)
    
    # Save to CSV files
    if not df.empty:
        print(f"\n=== SAVING CSV FILES ===")
        
        # Save raw data
        fetcher.save_to_csv(df, "raw_token_data_7days.csv")
        
        # Save formatted data
        fetcher.save_formatted_csv(df, "formatted_token_data_7days.csv")
        
        # Save individual token CSV files
        print(f"\n=== SAVING INDIVIDUAL TOKEN CSV FILES ===")
        for token_symbol in df['token_symbol'].unique():
            token_df = df[df['token_symbol'] == token_symbol].copy()
            token_df = token_df.sort_values('date')
            filename = f"{token_symbol}_7days_data.csv"
            token_df.to_csv(filename, index=False)
            print(f"[SAVED] Saved {len(token_df)} records for {token_symbol} to {filename}")
        
        # Create a summary CSV with latest prices
        print(f"\n=== CREATING SUMMARY CSV ===")
        latest_data = df.loc[df.groupby('token_symbol')['date'].idxmax()]
        summary_df = latest_data[['token_symbol', 'date', 'priceUSD', 'dailyVolumeUSD', 'totalLiquidityUSD']].copy()
        summary_df.columns = ['Token', 'Latest_Date', 'Current_Price_USD', 'Latest_Volume_USD', 'Market_Cap_USD']
        summary_df = summary_df.sort_values('Current_Price_USD', ascending=False)
        summary_df.to_csv("token_summary_latest.csv", index=False)
        print(f"[SAVED] Token summary saved to token_summary_latest.csv")
        
        print(f"\n=== COMPLETE ===")
        print("Check your directory for the following CSV files:")
        print("1. raw_token_data_7days.csv - All data in raw format")
        print("2. formatted_token_data_7days.csv - Formatted with currency symbols")
        print("3. token_summary_latest.csv - Latest data for each token")
        print("4. Individual token files (e.g., WETH_7days_data.csv)")
        print("\nNOTE: Some data may be sample data due to API rate limits.")
        print("For real-time production data, consider upgrading to CoinGecko Pro API.")
        
    else:
        print("No data fetched - CSV files not created")
        print("This might be due to:")
        print("1. Network connectivity issues")
        print("2. CoinGecko API rate limits (very strict on free tier)")
        print("3. Invalid token IDs")
        
        # Create a sample CSV anyway for testing
        print("\nCreating sample data for testing...")
        sample_data = []
        for token in ['WETH', 'USDC', 'USDT']:
            for i in range(7):
                date = datetime.now() - timedelta(days=6-i)
                sample_data.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'token_symbol': token,
                    'token_address': fetcher.tokens[token]['address'],
                    'priceUSD': 1500 if token == 'WETH' else 1.0,
                    'dailyVolumeUSD': 50000000,
                    'totalLiquidityUSD': 1000000000
                })
        
        sample_df = pd.DataFrame(sample_data)
        sample_df.to_csv("sample_token_data.csv", index=False)
        print("Created sample_token_data.csv with test data")

if __name__ == "__main__":
    main()