import time
import os
import pandas as pd
from datetime import datetime
import ScriptDataFetcher  # Assuming ScriptDataFetcher.py is in the same directory or in PYTHONPATH
import FileHandler

def sync_nifty_scripts_data(data_dir, csv_file, db_table=None):
    symbols = FileHandler.read_nifty_symbols(csv_file)
    print(f"Total symbols: {len(symbols)}")
    from PostgresWriter import upsert_stock_data

    for symbol in symbols:
        if FileHandler.check_symbol_file_exists(symbol, data_dir):
            print(f"exists: {symbol}")
        else:
            print(f"downloading: {symbol}")
            try:
                data = ScriptDataFetcher.fetch_historical_data(symbol)
                if data is not None and not data.empty:
                    FileHandler.save_data_to_csv(data, symbol, data_dir)
                    if db_table:
                        # Reset index to ensure 'Date' is a column
                        data = data.reset_index()
                        upsert_stock_data(data, db_table, symbol)
                else:
                    print(f"No data found for {symbol}")
            except Exception as e:
                print(f"Failed to fetch data for {symbol}: {e}")


def fetch_multiple_historical_data(symbols, directory, retries=3, delay=60):
    """
    Fetch and save historical data for multiple stock symbols, handling rate limits.
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
    Update the CSV for the given symbol in data_dir with the latest historical data.
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
            from PostgresWriter import upsert_stock_data
            upsert_stock_data(new_data, db_table, symbol)
    else:
        print(f"{symbol}: No new rows to append.")

    if alert:
        print(alert.strip())

if __name__ == "__main__":
    data_dir = r"/home/shared/Src/VSWorkspace/TraderSetup/TradeSetup/data"
    # csv_file = os.path.join(os.path.dirname(__file__), "niftytotalmarket_list.csv")
    csv_file = r"/home/shared/Src/VSWorkspace/TraderSetup/TradeSetup/sources/niftytotalmarket_list.csv"  # Update with your actual path
    db_table = "stock_prices"  # Change to your actual table name
    sync_nifty_scripts_data(data_dir, csv_file, db_table=db_table)