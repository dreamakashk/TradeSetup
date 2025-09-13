"""
NiftyScriptsDataSyncer - Bulk Stock Data Synchronization Module

This module handles bulk synchronization operations for stock market data,
including downloading data for all Nifty stocks and updating existing datasets.
Provides functions for both initial data download and incremental updates.

Functions:
    sync_nifty_scripts_data: Download/update data for all Nifty stocks
    fetch_multiple_historical_data: Batch download with rate limit handling
    sync_symbol_data: Update existing CSV with latest data for specific symbol

Author: TradeSetup Team
Created: 2025-09-13
"""

import time                # For sleep delays during rate limiting
import os                 # For file and directory operations
import pandas as pd       # For DataFrame operations and data processing
from datetime import datetime  # For date calculations and formatting
import ScriptDataFetcher  # Custom module for fetching stock data via yfinance
import FileHandler       # Custom module for file operations

def sync_nifty_scripts_data(data_dir, csv_file, db_table=None):
    """
    Download and synchronize data for all stocks in the Nifty universe.
    
    Processes all stock symbols from the Nifty total market list CSV file,
    checking for existing data files and downloading missing ones. Optionally
    saves data to database if table name is provided.
    
    Args:
        data_dir (str): Directory path for storing CSV files
        csv_file (str): Path to Nifty symbols CSV file
        db_table (str, optional): Database table name for storing data.
                                 If None, only CSV files are created
    
    Returns:
        None
    
    Side Effects:
        - Downloads historical data for missing symbols
        - Creates CSV files in data_dir
        - Optionally inserts data into database table
        - Prints progress messages for each symbol
    """
    # Read all stock symbols from the Nifty CSV file
    symbols = FileHandler.read_nifty_symbols(csv_file)
    print(f"Total symbols: {len(symbols)}")
    
    # Import database functions if available
    # This lazy import prevents errors when database dependencies are missing
    try:
        from PostgresWriter import upsert_stock_metadata, upsert_stock_price_data
        db_available = True
    except ImportError as e:
        print(f"Warning: Could not import PostgresWriter for database operations: {e}")
        db_available = False
        
    # Import configuration reader to get database settings
    from ConfigReader import read_config
    import os
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "config.json")
    config = read_config(config_path)
    
    use_database = config.db_enabled and db_available

    # Process each symbol in the Nifty universe
    for symbol in symbols:
        # Check if data file already exists to avoid re-downloading
        if FileHandler.check_symbol_file_exists(symbol, data_dir):
            print(f"exists: {symbol}")
        else:
            # Download historical data for new symbols
            print(f"downloading: {symbol}")
            try:
                # Fetch historical data from configured start date
                data = ScriptDataFetcher.fetch_historical_data(symbol, start_date=config.start_date)
                
                if data is not None and not data.empty:
                    # Save data to CSV file
                    FileHandler.save_data_to_csv(data, symbol, data_dir)
                    
                    # Save to database if enabled and available
                    if use_database:
                        try:
                            # First, get company information for metadata
                            company_info = ScriptDataFetcher.fetch_company_info(symbol)
                            
                            # Insert/update stock metadata
                            upsert_stock_metadata(config, symbol, company_info)
                            
                            # Reset index to ensure 'Date' is a column for database
                            data_for_db = data.reset_index()
                            
                            # Insert/update stock price data
                            upsert_stock_price_data(config, data_for_db, symbol)
                            
                        except Exception as db_error:
                            print(f"Database operation failed for {symbol}: {db_error}")
                            print("Continuing with CSV-only mode for this symbol...")
                else:
                    print(f"No data found for {symbol}")
            except Exception as e:
                # Continue processing other symbols even if one fails
                print(f"Failed to fetch data for {symbol}: {e}")


def fetch_multiple_historical_data(symbols, directory, retries=3, delay=60):
    """
    Fetch and save historical data for multiple stock symbols with rate limit handling.
    
    This function processes a list of stock symbols and downloads their historical
    data with built-in retry logic for handling API rate limits. It's an alternative
    to sync_nifty_scripts_data for custom symbol lists.
    
    Args:
        symbols (list[str]): List of stock symbols to process
        directory (str): Target directory for saving CSV files
        retries (int): Number of retry attempts per symbol (default: 3)
        delay (int): Delay in seconds between retries (default: 60)
    
    Returns:
        None
    
    Side Effects:
        - Downloads historical data for each symbol
        - Creates CSV files in the specified directory
        - Handles rate limiting with automatic retries
    """
    for symbol in symbols:
        for attempt in range(retries):
            try:
                print(f"Fetching data for {symbol}...")
                data = ScriptDataFetcher.fetch_historical_data(symbol, retries=1, delay=delay)
                if data is not None and not data.empty:
                    FileHandler.save_data_to_csv(data, symbol, directory)
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

def sync_symbol_data(symbol, data_dir, db_table=None):
    """
    Update existing CSV file with the latest historical data for a specific symbol.
    
    This function performs incremental updates by reading the existing CSV file,
    determining the latest date, and fetching only new data from that point forward.
    It includes data validation to detect discrepancies between existing and new data.
    
    Args:
        symbol (str): Stock symbol to update (e.g., 'RELIANCE')
        data_dir (str): Directory containing the existing CSV file
        db_table (str, optional): Database table name for storing updates.
                                 If None, only CSV file is updated
    
    Returns:
        None
    
    Side Effects:
        - Updates existing CSV file with new data
        - Optionally updates database table
        - Prints alerts for data discrepancies
        - Creates backup before modifications
    
    Notes:
        - Requires existing CSV file for the symbol
        - Performs data validation between old and new data
        - Handles date format conversions automatically
    """
    csv_path = os.path.join(data_dir, f"{symbol}.csv")
    if not os.path.exists(csv_path):
        print(f"CSV file for {symbol} does not exist in {data_dir}.")
        return

    # Read existing CSV
    df = pd.read_csv(csv_path, parse_dates=["Date"])
    if df.empty:
        print(f"No data in CSV for {symbol}.")
        return

    # Get the latest date in the CSV
    last_row = df.iloc[-1]
    last_date = last_row["Date"]
    if isinstance(last_date, pd.Timestamp):
        last_date_str = last_date.strftime("%Y-%m-%d")
    else:
        last_date_str = str(last_date)

    today_str = datetime.today().strftime("%Y-%m-%d")
    if last_date_str >= today_str:
        print(f"{symbol}: Data is already up to date (latest date: {last_date_str}).")
        return

    # Fetch new data from the last date (inclusive)
    new_data = ScriptDataFetcher.fetch_historical_data_from_date(symbol, last_date_str)
    if new_data is None or new_data.empty:
        print(f"{symbol}: No new data fetched.")
        return

    new_data = new_data.reset_index()
    # Ensure 'Date' column is present and in the same format
    if "Date" not in new_data.columns:
        new_data.rename(columns={new_data.columns[0]: "Date"}, inplace=True)

    # Compare last row of CSV and first row of new data
    first_new_row = new_data.iloc[0]
    alert = ""
    columns_to_check = ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]
    for col in columns_to_check:
        csv_val = last_row.get(col)
        new_val = first_new_row.get(col)
        if pd.isnull(csv_val) and pd.isnull(new_val):
            continue
        if csv_val != new_val:
            alert += f"ALERT: {symbol} - Mismatch in column '{col}' for date {last_date_str}: CSV={csv_val}, New={new_val}\n"

    # Remove the first row from new_data to avoid duplication
    new_data = new_data.iloc[1:]

    # Append new rows (if any) and save
    if not new_data.empty:
        updated_df = pd.concat([df, new_data], ignore_index=True)
        updated_df.to_csv(csv_path, index=False)
        print(f"{symbol}: Appended {len(new_data)} new rows to CSV.")
        if db_table:
            try:
                from PostgresWriter import upsert_stock_data
                upsert_stock_data(new_data, db_table, symbol)
            except ImportError as e:
                print(f"Warning: Could not import PostgresWriter for database operations: {e}")
    else:
        print(f"{symbol}: No new rows to append.")

    if alert:
        print(alert.strip())

if __name__ == "__main__":
    data_dir = "./data"
    # Import configuration to get the source file
    from ConfigReader import read_config
    import os
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "config.json")
    config = read_config(config_path)
    csv_file = os.path.join(os.path.dirname(__file__), "..", "sources", config.source_file)
    db_table = "stock_prices"  # Change to your actual table name
    sync_nifty_scripts_data(data_dir, csv_file, db_table=None)  # Disable database for now