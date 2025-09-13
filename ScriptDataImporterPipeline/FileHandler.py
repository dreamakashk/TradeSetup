"""
FileHandler - File Operations Module

This module handles file operations for the TradeSetup application, including
CSV file management, symbol list processing, and file existence checks.
Provides utilities for managing stock data files and Nifty symbol lists.

Functions:
    save_data_to_csv: Save DataFrame to CSV file
    read_nifty_symbols: Read stock symbols from Nifty CSV file
    check_symbol_file_exists: Check if symbol's CSV file exists

Author: TradeSetup Team
Created: 2025-09-13
"""

import csv  # For reading CSV files with stock symbols
import os   # For file and directory operations


def save_data_to_csv(data, symbol: str, directory: str):
    """
    Save stock data DataFrame to a CSV file in the specified directory.
    
    Creates the target directory if it doesn't exist and saves the DataFrame
    with the stock symbol as the filename. This function provides a consistent
    way to store stock data across the application.
    
    Args:
        data (pandas.DataFrame): Stock data to save
        symbol (str): Stock symbol for filename (e.g., 'RELIANCE.NS')
        directory (str): Target directory for saving the file
    
    Returns:
        None
    
    Side Effects:
        - Creates directory if it doesn't exist
        - Writes CSV file to disk
        - Prints confirmation message
    """
    # Ensure the target directory exists
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Create full file path using symbol as filename
    filename = os.path.join(directory, f"{symbol}.csv")
    
    # Save DataFrame to CSV (includes date index)
    data.to_csv(filename)
    
    # Provide user feedback
    print(f"Data saved to {filename}")

def read_nifty_symbols(csv_file):
    """
    Read stock symbols from a Nifty total market CSV file.
    
    Parses the official Nifty total market list CSV file downloaded from
    NSE India website and extracts all stock symbols. The CSV format
    is expected to have a 'Symbol' column containing stock symbols.
    
    Source: https://niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv
    
    Args:
        csv_file (str): Path to the Nifty symbols CSV file
    
    Returns:
        list[str]: List of stock symbols (e.g., ['RELIANCE', 'TCS', 'INFY'])
    
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        KeyError: If the 'Symbol' column is missing from the CSV
    """
    symbols = []
    
    # Read CSV file with UTF-8 encoding to handle special characters
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Extract symbol from each row and clean whitespace
        for row in reader:
            symbol = row["Symbol"].strip()
            symbols.append(symbol)
    
    return symbols

def check_symbol_file_exists(symbol, directory):
    """
    Check if a CSV file for the given stock symbol exists in the directory.
    
    This function is used to determine whether historical data for a specific
    stock symbol has already been downloaded, avoiding unnecessary re-downloads
    during bulk sync operations.
    
    Args:
        symbol (str): Stock symbol to check (e.g., 'RELIANCE')
        directory (str): Directory path to check for the CSV file
    
    Returns:
        bool: True if the CSV file exists, False otherwise
    
    Example:
        >>> check_symbol_file_exists('RELIANCE', './data')
        True  # if ./data/RELIANCE.csv exists
    """
    # Construct expected file path for the symbol
    file_path = os.path.join(directory, f"{symbol}.csv")
    
    # Check if file exists and return boolean result
    return os.path.exists(file_path)

# def rename_ns_csv_files_to_csv(data_dir):
#     """
#     Renames all files in the data directory from 'SYMBOL.NS.csv' to 'SYMBOL.csv'.
#     """
#     for filename in os.listdir(data_dir):
#         if filename.endswith(".NS.csv"):
#             old_path = os.path.join(data_dir, filename)
#             new_filename = filename.replace(".NS.csv", ".csv")
#             new_path = os.path.join(data_dir, new_filename)
#             os.rename(old_path, new_path)
#             print(f"Renamed: {old_path} -> {new_path}")

# def delete_ns_csv_files(data_dir):
#     """
#     Deletes all files in the data directory that end with '.NS.csv'.
#     """
#     for filename in os.listdir(data_dir):
#         if filename.endswith(".NS.csv"):
#             file_path = os.path.join(data_dir, filename)
#             os.remove(file_path)
#             print(f"Deleted: {file_path}")


# if __name__ == "__main__":
#     data_dir = r"C:\Users\akashkatare\Src\TradeSetup\data"
#     # rename_ns_csv_files_to_csv(data_dir)
#     delete_ns_csv_files(data_dir)
