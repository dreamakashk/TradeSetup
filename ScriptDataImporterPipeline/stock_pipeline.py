#!/usr/bin/env python3
"""
Stock Pipeline Module - Extracted Stock Operations for TradeSetup

This module contains all stock data operations extracted from main.py to enable
proper import and usage by the unified pipeline system. It provides a clean
interface for stock data fetching, synchronization, and incremental updates.

Functions:
    process_single_symbol: Process a single stock symbol with complete data fetch
    sync_all_nifty_symbols: Sync all stocks from Nifty universe
    sync_single_symbol: Update a specific symbol's data incrementally
    cron_incremental_update: Batch incremental updates for all symbols

Author: TradeSetup Team
Created: 2025-09-13
"""

import os
import sys
from datetime import datetime

# Import existing modules from the pipeline
import ScriptDataFetcher
import FileHandler
from NiftyScriptsDataSyncer import (
    sync_nifty_scripts_data,
    sync_symbol_data,
    cron_incremental_update
)


def process_single_symbol(symbol: str, data_dir: str, config) -> bool:
    """
    Process a single stock symbol with complete data fetch.
    
    Fetches company information and all available historical data for the
    specified symbol, then saves it to CSV and optionally to database.
    
    Args:
        symbol (str): Stock symbol to fetch (e.g., 'RELIANCE.NS')
        data_dir (str): Directory path for storing CSV files
        config: Configuration object with all settings
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"Processing single symbol: {symbol}")
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # First, get company information (sector, industry, market cap, etc.)
        print("Fetching company information...")
        info = ScriptDataFetcher.fetch_company_info(symbol)
        print(f"Company: {info.get('long_name', 'Unknown')}")
        print(f"Sector: {info.get('sector', 'Unknown')}")
        print(f"Industry: {info.get('industry', 'Unknown')}")
        
        # Fetch all available historical price and volume data
        print("Fetching historical data...")
        data = ScriptDataFetcher.fetch_historical_data(symbol, start_date=config.start_date)
        
        if data is None or data.empty:
            print(f"No data found for {symbol}")
            return False
            
        print(f"Data shape: {data.shape}")
        print(f"Date range: {data.index.min()} to {data.index.max()}")
        
        # Save the fetched data to CSV file
        FileHandler.save_data_to_csv(data, symbol, data_dir)
        print(f"Data saved to CSV successfully!")
        
        # Save to database if enabled
        if config.db_enabled and config.db_update_enabled:
            try:
                from PostgresWriter import upsert_stock_metadata, upsert_stock_price_data
                
                print("Updating database...")
                
                # Insert/update stock metadata
                upsert_stock_metadata(config, symbol, info)
                
                # Reset index to ensure 'Date' is a column for database
                data_for_db = data.reset_index()
                
                # Insert/update stock price data
                upsert_stock_price_data(config, data_for_db, symbol)
                
                print("Database updated successfully!")
                
            except Exception as db_error:
                print(f"Database operation failed for {symbol}: {db_error}")
                print("Continuing with CSV-only mode...")
        
        return True
        
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return False


def sync_all_nifty_symbols(data_dir: str, csv_file: str, config) -> tuple:
    """
    Sync all stocks from the Nifty universe.
    
    Downloads/updates data for all stocks in the Nifty total market list,
    checking for existing data files and downloading missing ones.
    
    Args:
        data_dir (str): Directory path for storing CSV files
        csv_file (str): Path to Nifty symbols CSV file
        config: Configuration object with all settings
        
    Returns:
        tuple: (success_count, error_count) - Number of successful and failed operations
    """
    try:
        print(f"Syncing all Nifty symbols from: {csv_file}")
        print(f"Data directory: {data_dir}")
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        if not os.path.exists(csv_file):
            print(f"Error: Nifty symbols file not found: {csv_file}")
            return 0, 1
        
        # Read symbols and get counts
        symbols = FileHandler.read_nifty_symbols(csv_file)
        total_symbols = len(symbols)
        
        # Check existing files to calculate success/error counts
        success_count = 0
        error_count = 0
        
        for symbol in symbols:
            if FileHandler.check_symbol_file_exists(symbol, data_dir):
                success_count += 1
            else:
                # This will be handled by sync_nifty_scripts_data
                pass
        
        # Call the existing sync function
        sync_nifty_scripts_data(data_dir, csv_file, db_table=None)
        
        # Recount after sync to get final numbers
        new_success_count = 0
        new_error_count = 0
        
        for symbol in symbols:
            if FileHandler.check_symbol_file_exists(symbol, data_dir):
                new_success_count += 1
            else:
                new_error_count += 1
        
        print(f"Sync completed: {new_success_count} successful, {new_error_count} failed")
        return new_success_count, new_error_count
        
    except Exception as e:
        print(f"Error in sync_all_nifty_symbols: {e}")
        return 0, 1


def sync_single_symbol(symbol: str, data_dir: str, csv_file: str, config) -> bool:
    """
    Update a specific symbol's data incrementally.
    
    Updates existing CSV file with the latest historical data for the
    specified symbol, performing incremental updates from the last date.
    
    Args:
        symbol (str): Stock symbol to update
        data_dir (str): Directory containing the existing CSV file
        csv_file (str): Path to source file (for validation)
        config: Configuration object with all settings
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"Syncing single symbol: {symbol}")
        
        # Validate that symbol exists in source file
        if os.path.exists(csv_file):
            symbols = FileHandler.read_nifty_symbols(csv_file)
            if symbol not in symbols:
                print(f"Warning: {symbol} not found in source file {csv_file}")
                # Continue anyway in case it's a valid symbol not in our list
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Call the existing sync function for individual symbol
        sync_symbol_data(symbol, data_dir, db_table=None)
        
        # Verify the update was successful
        csv_path = os.path.join(data_dir, f"{symbol}.csv")
        if os.path.exists(csv_path):
            print(f"✓ Symbol {symbol} data updated successfully")
            return True
        else:
            print(f"❌ Failed to update {symbol} data")
            return False
            
    except Exception as e:
        print(f"Error syncing {symbol}: {e}")
        return False


# Re-export the existing cron_incremental_update function directly
# since it already has the correct signature and behavior
def cron_incremental_update_wrapper(data_dir: str, csv_file: str, config) -> tuple:
    """
    Wrapper for cron incremental update to maintain consistent interface.
    
    Args:
        data_dir (str): Directory path for CSV data files
        csv_file (str): Path to source CSV file containing stock symbols
        config: Configuration object with all settings
        
    Returns:
        tuple: (success_count, error_count) - Number of successful and failed updates
    """
    return cron_incremental_update(data_dir, csv_file, config)


# For backward compatibility, export with the expected name
def cron_incremental_update_pipeline(data_dir: str, csv_file: str, config) -> tuple:
    """
    Legacy wrapper for cron incremental update.
    
    Args:
        data_dir (str): Directory path for CSV data files
        csv_file (str): Path to source CSV file containing stock symbols
        config: Configuration object with all settings
        
    Returns:
        tuple: (success_count, error_count) - Number of successful and failed updates
    """
    return cron_incremental_update(data_dir, csv_file, config)


if __name__ == "__main__":
    # Test the module with a simple command-line interface
    import argparse
    from ConfigReader import read_config
    
    parser = argparse.ArgumentParser(description='Stock Pipeline Operations')
    parser.add_argument('--mode', choices=['single', 'sync-all', 'sync-symbol', 'cron-update'], 
                       required=True, help='Operation mode')
    parser.add_argument('--symbol', help='Stock symbol for single operations')
    parser.add_argument('--config', default='../configs/config.json', help='Configuration file')
    
    args = parser.parse_args()
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), '..', args.config)
    config = read_config(config_path)
    
    data_dir = config.data_file_path
    csv_file = os.path.join(os.path.dirname(__file__), '..', 'sources', config.source_file)
    
    if args.mode == 'single':
        if not args.symbol:
            print("Error: --symbol required for single mode")
            sys.exit(1)
        success = process_single_symbol(args.symbol, data_dir, config)
        sys.exit(0 if success else 1)
        
    elif args.mode == 'sync-all':
        success_count, error_count = sync_all_nifty_symbols(data_dir, csv_file, config)
        print(f"Results: {success_count} success, {error_count} errors")
        sys.exit(0 if error_count == 0 else 1)
        
    elif args.mode == 'sync-symbol':
        if not args.symbol:
            print("Error: --symbol required for sync-symbol mode")
            sys.exit(1)
        success = sync_single_symbol(args.symbol, data_dir, csv_file, config)
        sys.exit(0 if success else 1)
        
    elif args.mode == 'cron-update':
        success_count, error_count = cron_incremental_update(data_dir, csv_file, config)
        print(f"Results: {success_count} success, {error_count} errors")
        sys.exit(0 if error_count == 0 else 1)