"""
Runner - Legacy Test Script for TradeSetup Pipeline

This is a legacy test script that demonstrates basic usage of the ScriptDataFetcher
module. It fetches company information and historical data for a single stock symbol
(BSE.NS by default) and saves it to CSV.

Note: This script is superseded by main.py which provides a comprehensive CLI.
      It's kept for backward compatibility and simple testing purposes.

Author: TradeSetup Team
Created: 2025-09-13
"""

import os                      # For file path operations
import ScriptDataFetcher      # Custom module for stock data fetching
from ConfigReader import read_config  # Custom module for configuration

# Entry point for legacy test execution
if __name__ == "__main__":
    # Define target stock symbol (BSE Limited - Bombay Stock Exchange)
    symbol = "BSE.NS"  # Change to your desired stock symbol
    
    # Fetch and display company information
    info = ScriptDataFetcher.fetch_company_info(symbol)  # Fetch company info for BSE
    print(f"Company Info for {symbol}:")
    print(info)

    # Load configuration to get data directory path
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "config.json")
    config = read_config(config_path)
    print(f"Data will be saved to: {config.data_file_path}")

    # Fetch complete historical data for the symbol
    data = ScriptDataFetcher.fetch_historical_data(symbol)
    print(f"Fetched data shape: {data.shape}")
    print(data.head())
    
    # Save the fetched data to CSV in the configured directory
    ScriptDataFetcher.save_data_to_csv(data, symbol, config.data_file_path)