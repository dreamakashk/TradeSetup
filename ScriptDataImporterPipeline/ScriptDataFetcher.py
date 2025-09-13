import yfinance as yf
import time
import os

def fetch_historical_data(symbol: str, exchange: str = "NS", retries: int = 3, delay: int = 60):
    """
    Fetch all available historical price and volume data for a given stock symbol.
    Retries if rate limited.
    """
    full_symbol = f"{symbol}.{exchange}" if not symbol.endswith(f".{exchange}") else symbol
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(full_symbol)
            hist = ticker.history(period="max")
            return hist
        except Exception as e:
            if "Rate limited" in str(e):
                print(f"Rate limited. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Failed to fetch data after retries due to rate limiting.")

def fetch_historical_data_from_date(symbol: str, start_date: str, exchange: str = "NS", end_date: str = None, retries: int = 3, delay: int = 60):
    """
    Fetch historical price and volume data for a given stock symbol from a specific start date.
    Dates should be in 'YYYY-MM-DD' format.
    If end_date is None, fetches up to the latest available date.
    Retries if rate limited.
    """
    full_symbol = f"{symbol}.{exchange}" if not symbol.endswith(f".{exchange}") else symbol
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(full_symbol)
            hist = ticker.history(start=start_date, end=end_date)
            return hist
        except Exception as e:
            if "Rate limited" in str(e):
                print(f"Rate limited. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Failed to fetch data after retries due to rate limiting.")

def fetch_company_info(symbol: str, exchange: str = "NS", retries: int = 3, delay: int = 60):
    """
    Fetch company information such as sector, market cap, etc. for the given stock symbol.
    Retries if rate limited.
    """
    full_symbol = f"{symbol}.{exchange}" if not symbol.endswith(f".{exchange}") else symbol
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(full_symbol)
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
        except Exception as e:
            if "Rate limited" in str(e):
                print(f"Rate limited. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Failed to fetch company info after retries due to rate limiting.")


def save_data_to_csv(data, symbol: str, directory: str):
    """
    Save the DataFrame to a CSV file in the specified directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = os.path.join(directory, f"{symbol}.csv")
    data.to_csv(filename)
    print(f"Data saved to {filename}")

