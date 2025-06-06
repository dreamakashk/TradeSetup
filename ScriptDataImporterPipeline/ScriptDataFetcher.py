import yfinance as yf
import time
import os

def fetch_historical_data(symbol: str, retries: int = 3, delay: int = 60):
    """
    Fetch all available historical price and volume data for a given stock symbol.
    Retries if rate limited.
    """
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="max")
            return hist
        except Exception as e:
            if "Rate limited" in str(e):
                print(f"Rate limited. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Failed to fetch data after retries due to rate limiting.")

def save_data_to_csv(data, symbol: str, directory: str):
    """
    Save the DataFrame to a CSV file in the specified directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = os.path.join(directory, f"{symbol}.csv")
    data.to_csv(filename)
    print(f"Data saved to {filename}")

def fetch_company_info(symbol: str):
    """
    Fetch company information such as sector, market cap, etc. for the given stock symbol.
    """
    ticker = yf.Ticker(symbol)
    info = ticker.info

    sector = info.get("sector", "N/A")
    market_cap = info.get("marketCap", "N/A")
    long_name = info.get("longName", "N/A")
    industry = info.get("industry", "N/A")
    website = info.get("website", "N/A")
    return {
        "long_name": long_name,
        "sector": sector,
        "industry": industry,
        "market_cap": market_cap,
        "website": website
    }

def fetch_multiple_historical_data(symbols, directory, retries=3, delay=60):
    """
    Fetch and save historical data for multiple stock symbols, handling rate limits.
    """
    for symbol in symbols:
        for attempt in range(retries):
            try:
                print(f"Fetching data for {symbol}...")
                data = fetch_historical_data(symbol, retries=1, delay=delay)
                if data is not None and not data.empty:
                    save_data_to_csv(data, symbol, directory)
                else:
                    print(f"No data found for {symbol}")
                break
            except Exception as e:
                if "Rate limited" in str(e):
                    print(f"Rate limited while fetching {symbol}. Waiting {delay} seconds before retrying...")
                    time.sleep(delay)
                else:
                    print(f"Failed to fetch data for {symbol}: {e}")
                    break