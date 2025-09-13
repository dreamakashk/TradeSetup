"""
ScriptDataFetcher - Stock Market Data Fetching Module

This module handles fetching stock market data from Yahoo Finance using the yfinance library.
It provides functions to retrieve historical price data, company information, and handle
rate limiting with automatic retry mechanisms.

Functions:
    - fetch_historical_data: Get all available historical data for a stock
    - fetch_historical_data_from_date: Get historical data from a specific date
    - fetch_company_info: Get company metadata (sector, industry, market cap)
    - save_data_to_csv: Save DataFrame to CSV file

Author: TradeSetup Team
Created: 2025-09-13
"""

import yfinance as yf  # Yahoo Finance API library for stock data
import time           # For sleep delays during rate limiting
import os            # For file and directory operations

def fetch_historical_data(symbol: str, exchange: str = "NS", start_date: str = None, retries: int = 3, delay: int = 60):
    """
    Fetch all available historical price and volume data for a given stock symbol.
    
    This function retrieves the complete historical dataset available for a stock,
    including OHLCV data, dividends, and stock splits. It handles rate limiting
    by automatically retrying with exponential backoff.
    
    Args:
        symbol (str): Stock symbol (e.g., 'RELIANCE' for Indian stocks)
        exchange (str): Stock exchange suffix (default: 'NS' for NSE)
        retries (int): Maximum number of retry attempts (default: 3)
        delay (int): Delay in seconds between retries (default: 60)
    
    Returns:
        pandas.DataFrame: Historical data with columns:
            - Open, High, Low, Close: Price data
            - Volume: Trading volume
            - Dividends: Dividend amounts
            - Stock Splits: Split ratios
    
    Raises:
        Exception: If data fetching fails after all retry attempts
    """
    # Construct full symbol with exchange suffix (e.g., RELIANCE.NS)
    full_symbol = f"{symbol}.{exchange}" if not symbol.endswith(f".{exchange}") else symbol
    
    # Attempt to fetch data with retry logic for rate limiting
    for attempt in range(retries):
        try:
            # Create yfinance Ticker object and fetch historical data
            ticker = yf.Ticker(full_symbol)
            if start_date:
                # Fetch data from specific start date
                hist = ticker.history(start=start_date)
            else:
                # Fetch maximum available history
                hist = ticker.history(period="max")
            return hist
        except Exception as e:
            # Handle rate limiting specifically
            if "Rate limited" in str(e):
                print(f"Rate limited. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                # Re-raise non-rate-limiting exceptions immediately
                raise
    
    # All retry attempts exhausted
    raise Exception("Failed to fetch data after retries due to rate limiting.")

def fetch_historical_data_from_date(symbol: str, start_date: str, exchange: str = "NS", end_date: str = None, retries: int = 3, delay: int = 60):
    """
    Fetch historical price and volume data for a stock symbol from a specific start date.
    
    This function is useful for updating existing datasets by fetching only new data
    from a specific date onwards. It supports date range filtering and handles
    rate limiting with automatic retries.
    
    Args:
        symbol (str): Stock symbol (e.g., 'TCS' for Indian stocks)
        start_date (str): Start date in 'YYYY-MM-DD' format
        exchange (str): Stock exchange suffix (default: 'NS' for NSE)
        end_date (str, optional): End date in 'YYYY-MM-DD' format. If None, 
                                 fetches up to the latest available date
        retries (int): Maximum number of retry attempts (default: 3)
        delay (int): Delay in seconds between retries (default: 60)
    
    Returns:
        pandas.DataFrame: Historical data for the specified date range with same
                         structure as fetch_historical_data()
    
    Raises:
        Exception: If data fetching fails after all retry attempts
    """
    # Construct full symbol with exchange suffix
    full_symbol = f"{symbol}.{exchange}" if not symbol.endswith(f".{exchange}") else symbol
    
    # Attempt to fetch data with retry logic for rate limiting
    for attempt in range(retries):
        try:
            # Create yfinance Ticker object and fetch data for date range
            ticker = yf.Ticker(full_symbol)
            hist = ticker.history(start=start_date, end=end_date)
            return hist
        except Exception as e:
            # Handle rate limiting with delay and retry
            if "Rate limited" in str(e):
                print(f"Rate limited. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                # Re-raise non-rate-limiting exceptions immediately
                raise
    
    # All retry attempts exhausted
    raise Exception("Failed to fetch data after retries due to rate limiting.")

def fetch_company_info(symbol: str, exchange: str = "NS", retries: int = 3, delay: int = 60):
    """
    Fetch company information and metadata for a given stock symbol.
    
    Retrieves fundamental company data including business sector, industry
    classification, market capitalization, and other corporate information.
    This is useful for stock screening and categorization.
    
    Args:
        symbol (str): Stock symbol (e.g., 'INFY' for Infosys)
        exchange (str): Stock exchange suffix (default: 'NS' for NSE)
        retries (int): Maximum number of retry attempts (default: 3)
        delay (int): Delay in seconds between retries (default: 60)
    
    Returns:
        dict: Company information containing:
            - long_name (str): Full company name
            - sector (str): Business sector (e.g., 'Technology')
            - industry (str): Specific industry (e.g., 'Software')
            - market_cap (int): Market capitalization in local currency
            - website (str): Company website URL
    
    Raises:
        Exception: If company info fetching fails after all retry attempts
    """
    # Construct full symbol with exchange suffix
    full_symbol = f"{symbol}.{exchange}" if not symbol.endswith(f".{exchange}") else symbol
    
    # Attempt to fetch company info with retry logic for rate limiting
    for attempt in range(retries):
        try:
            # Create yfinance Ticker object and fetch company information
            ticker = yf.Ticker(full_symbol)
            info = ticker.info

            # Extract key company information with fallback to "N/A"
            sector = info.get("sector", "N/A")
            market_cap = info.get("marketCap", "N/A")
            long_name = info.get("longName", "N/A")
            industry = info.get("industry", "N/A")
            website = info.get("website", "N/A")
            
            # Return structured company information dictionary
            return {
                "long_name": long_name,
                "sector": sector,
                "industry": industry,
                "market_cap": market_cap,
                "website": website
            }
        except Exception as e:
            # Handle rate limiting with delay and retry
            if "Rate limited" in str(e):
                print(f"Rate limited. Waiting {delay} seconds before retrying...")
                time.sleep(delay)
            else:
                # Re-raise non-rate-limiting exceptions immediately
                raise
    
    # All retry attempts exhausted
    raise Exception("Failed to fetch company info after retries due to rate limiting.")


def save_data_to_csv(data, symbol: str, directory: str):
    """
    Save stock data DataFrame to a CSV file in the specified directory.
    
    Creates the output directory if it doesn't exist and saves the data
    with the symbol name as the filename. The CSV includes the date index
    and all OHLCV columns.
    
    Args:
        data (pandas.DataFrame): Stock data to save (typically from yfinance)
        symbol (str): Stock symbol to use as filename (e.g., 'RELIANCE.NS')
        directory (str): Target directory path for saving the CSV file
    
    Returns:
        None
    
    Side Effects:
        - Creates directory if it doesn't exist
        - Writes CSV file to disk
        - Prints confirmation message with full file path
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Construct full file path with symbol as filename
    filename = os.path.join(directory, f"{symbol}.csv")
    
    # Save DataFrame to CSV file (includes index which contains dates)
    data.to_csv(filename)
    
    # Provide user feedback about successful save operation
    print(f"Data saved to {filename}")

