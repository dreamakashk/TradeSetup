#!/usr/bin/env python3
"""
TradeSetup - Stock Market Data Pipeline
Main entry point for the stock market data analysis pipeline

This module provides a command-line interface for fetching and processing
Indian stock market data using the yfinance library. It supports multiple
operation modes for different use cases.

Author: TradeSetup Team
Created: 2025-09-13
"""

import os
import sys
import argparse
from datetime import datetime

# Add the ScriptDataImporterPipeline directory to Python path for module imports
# This allows importing modules from the pipeline subdirectory
sys.path.append(os.path.join(os.path.dirname(__file__), 'ScriptDataImporterPipeline'))

import ScriptDataFetcher
from ConfigReader import read_config
from NiftyScriptsDataSyncer import sync_nifty_scripts_data, sync_symbol_data

def main():
    """
    Main entry point for the TradeSetup application
    
    Parses command line arguments and executes the appropriate data fetching
    operation based on the specified mode. Supports three main modes:
    1. single: Fetch data for a specific stock symbol
    2. sync-all: Download/update data for all Nifty stocks
    3. sync-symbol: Update existing CSV data for a specific symbol
    
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    # Setup command line argument parser with available options
    parser = argparse.ArgumentParser(description='TradeSetup - Stock Market Data Pipeline')
    parser.add_argument('--mode', choices=['single', 'sync-all', 'sync-symbol'], default='single',
                       help='Mode: single stock, sync all Nifty stocks, or sync specific symbol')
    parser.add_argument('--symbol', default='BSE.NS', help='Stock symbol to fetch (default: BSE.NS)')
    parser.add_argument('--config', default='configs/config.json', help='Configuration file path')
    
    # Parse command line arguments
    args = parser.parse_args()
    
    # Display application header with current operation details
    print("="*60)
    print("TradeSetup - Stock Market Data Pipeline")
    print("="*60)
    print(f"Mode: {args.mode}")
    print(f"Symbol: {args.symbol}")
    print(f"Timestamp: {datetime.now()}")
    print("="*60)
    
    # Load configuration from JSON file
    config_path = args.config
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found: {config_path}")
        return 1
        
    # Read configuration and display data directory
    config = read_config(config_path)
    print(f"Data directory: {config.data_file_path}")
    
    # Ensure data directory exists, create if necessary
    os.makedirs(config.data_file_path, exist_ok=True)
    
    try:
        # Execute the appropriate operation based on the selected mode
        if args.mode == 'single':
            # Single stock mode: Fetch complete data for one stock symbol
            print(f"\nFetching data for single symbol: {args.symbol}")
            
            # First, get company information (sector, industry, market cap, etc.)
            print("Fetching company information...")
            info = ScriptDataFetcher.fetch_company_info(args.symbol)
            print(f"Company: {info.get('long_name', 'Unknown')}")
            print(f"Sector: {info.get('sector', 'Unknown')}")
            print(f"Industry: {info.get('industry', 'Unknown')}")
            
            # Fetch all available historical price and volume data
            print("Fetching historical data...")
            data = ScriptDataFetcher.fetch_historical_data(args.symbol)
            print(f"Data shape: {data.shape}")
            print(f"Date range: {data.index.min()} to {data.index.max()}")
            
            # Save the fetched data to CSV file
            ScriptDataFetcher.save_data_to_csv(data, args.symbol, config.data_file_path)
            print(f"Data saved successfully!")
            
        elif args.mode == 'sync-all':
            # Sync all mode: Download/update data for all stocks in Nifty universe
            print("\nSyncing all Nifty stocks...")
            csv_file = os.path.join("sources", "niftytotalmarket_list.csv")
            if not os.path.exists(csv_file):
                print(f"Error: Nifty symbols file not found: {csv_file}")
                return 1
            # Process all symbols from the Nifty total market list
            # Database operations disabled (db_table=None) for CSV-only mode
            sync_nifty_scripts_data(config.data_file_path, csv_file, db_table=None)
            
        elif args.mode == 'sync-symbol':
            # Sync symbol mode: Update existing CSV with latest data for specific symbol
            print(f"\nSyncing data for symbol: {args.symbol}")
            # This mode appends new data to existing CSV files without full redownload
            sync_symbol_data(args.symbol, config.data_file_path, db_table=None)
            
    except Exception as e:
        # Handle any unexpected errors during execution
        print(f"Error: {e}")
        return 1
    
    print("\nOperation completed successfully!")
    return 0

# Entry point when script is run directly (not imported)
if __name__ == "__main__":
    # Execute main function and exit with the returned code
    exit_code = main()
    sys.exit(exit_code)