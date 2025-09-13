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
import FileHandler
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
    parser.add_argument('--mode', choices=['single', 'sync-all', 'sync-symbol', 'cron-update'], default='single',
                       help='Mode: single stock, sync all Nifty stocks, sync specific symbol, or cron incremental update')
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
    
    # Initialize database schema if enabled
    if config.db_enabled:
        try:
            from DatabaseSetup import setup_database_schema, verify_schema
            print("Database enabled - verifying schema...")
            if not verify_schema(config):
                print("Schema verification failed - initializing database...")
                if setup_database_schema(config):
                    print("✅ Database schema initialized successfully")
                else:
                    print("❌ Database schema initialization failed - continuing in CSV-only mode")
                    config.db_enabled = False
            else:
                print("✅ Database schema verified")
        except Exception as db_error:
            print(f"Database setup error: {db_error}")
            print("Continuing in CSV-only mode...")
            config.db_enabled = False
    
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
            data = ScriptDataFetcher.fetch_historical_data(args.symbol, start_date=config.start_date)
            print(f"Data shape: {data.shape}")
            print(f"Date range: {data.index.min()} to {data.index.max()}")
            
            # Save the fetched data to CSV file using consistent FileHandler
            FileHandler.save_data_to_csv(data, args.symbol, config.data_file_path)
            print(f"Data saved successfully!")
            
        elif args.mode == 'sync-all':
            # Sync all mode: Download/update data for all stocks in Nifty universe
            print("\nSyncing all Nifty stocks...")
            csv_file = os.path.join("sources", config.source_file)
            if not os.path.exists(csv_file):
                print(f"Error: Nifty symbols file not found: {csv_file}")
                return 1
            # Process all symbols from the Nifty total market list
            # Database operations controlled by configuration
            sync_nifty_scripts_data(config.data_file_path, csv_file, db_table=None)
            
            # Display database status
            if config.db_enabled:
                print("Database integration: ENABLED")
            else:
                print("Database integration: DISABLED (CSV-only mode)")
            
        elif args.mode == 'sync-symbol':
            # Sync symbol mode: Update existing CSV with latest data for specific symbol
            print(f"\nSyncing data for symbol: {args.symbol}")
            # This mode appends new data to existing CSV files without full redownload
            sync_symbol_data(args.symbol, config.data_file_path, db_table=None)
            
            # Display database status
            if config.db_enabled:
                print("Database integration: ENABLED - data will be updated in database")
            else:
                print("Database integration: DISABLED - CSV-only mode")
                
        elif args.mode == 'cron-update':
            # Cron update mode: Incremental updates for all stocks (designed for automation)
            print("\nRunning incremental updates for all stocks (cron mode)...")
            csv_file = os.path.join("sources", config.source_file)
            if not os.path.exists(csv_file):
                print(f"Error: Source file not found: {csv_file}")
                return 1
                
            # Import the incremental update function
            from NiftyScriptsDataSyncer import cron_incremental_update
            
            # Run incremental updates for all stocks
            success_count, error_count = cron_incremental_update(config.data_file_path, csv_file, config)
            
            # Display execution summary
            print("\nCron update completed!")
            print(f"Successfully updated: {success_count} stocks")
            print(f"Errors encountered: {error_count} stocks")
            print(f"Source file: {config.source_file}")
            print(f"Database integration: {'ENABLED' if config.db_enabled else 'DISABLED'}")
            if config.db_enabled:
                print(f"Database updates: {'ENABLED' if config.db_update_enabled else 'DISABLED'}")
                print(f"Database logging: {'ENABLED' if config.db_logging_enabled else 'DISABLED'}")
            
            # Return non-zero exit code if there were errors (for cron monitoring)
            if error_count > 0:
                print(f"Warning: {error_count} stocks failed to update")
                return 1
            
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