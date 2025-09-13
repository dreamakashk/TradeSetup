#!/usr/bin/env python3
"""
Indicators Pipeline - Technical Indicators Calculation and Database Update

This is a standalone pipeline for calculating and updating technical indicators
in the stock_indicators_daily table. It can be run independently from the main
data fetching pipeline and supports incremental processing.

Usage Examples:
    # Process all stocks with missing indicators
    python indicators_pipeline.py --mode update-all

    # Process specific symbol
    python indicators_pipeline.py --mode single --symbol RELIANCE.NS

    # Reprocess all indicators (full recalculation)
    python indicators_pipeline.py --mode recalculate-all

    # Update indicators from specific date
    python indicators_pipeline.py --mode update-all --from-date 2025-01-01

Features:
- Incremental processing (calculates only missing indicators)
- Batch database operations for performance
- Robust error handling for production use
- Progress tracking and detailed logging
- Support for both CSV and database data sources

Author: TradeSetup Team
Created: 2025-09-13
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

# Add the ScriptDataImporterPipeline directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'ScriptDataImporterPipeline'))

from ConfigReader import read_config
from TechnicalIndicators import create_indicators_dataframe
from IndicatorsWriter import (
    upsert_indicators_data, 
    get_latest_indicators_date,
    get_latest_price_date,
    fetch_price_data_from_db,
    get_symbols_needing_indicators_update
)
import FileHandler


def load_price_data_from_csv(symbol: str, data_dir: str, from_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Load stock price data from CSV file.
    
    Args:
        symbol (str): Stock symbol (e.g., 'RELIANCE.NS')
        data_dir (str): Directory containing CSV files
        from_date (datetime, optional): Load data from this date onwards
    
    Returns:
        pd.DataFrame: Price data with Date index and OHLCV columns
    """
    csv_path = os.path.join(data_dir, f"{symbol}.csv")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Read CSV file
    df = pd.read_csv(csv_path, parse_dates=['Date'], index_col='Date')
    
    if df.empty:
        return df
    
    # Filter from specific date if provided
    if from_date:
        df = df[df.index >= pd.to_datetime(from_date)]
    
    # Ensure required columns exist
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in CSV: {missing_columns}")
    
    return df[required_columns]


def process_single_symbol(symbol: str, config, force_recalculate: bool = False, 
                         from_date: Optional[datetime] = None) -> bool:
    """
    Process indicators for a single symbol.
    
    Args:
        symbol (str): Stock symbol to process
        config (ConfigData): Configuration object
        force_recalculate (bool): If True, recalculate all indicators
        from_date (datetime, optional): Calculate indicators from this date
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        clean_symbol = symbol.replace('.NS', '').replace('.BO', '')
        print(f"Processing indicators for {symbol}...")
        
        # Determine what date range we need for incremental updates
        target_from_date = None  # The date from which we want to upsert new indicators
        warmup_from_date = None  # The date from which we need to fetch data for calculation
        
        # Determine data source and load price data
        if config.db_enabled and not force_recalculate:
            try:
                # Try to load from database first
                if from_date:
                    target_from_date = from_date
                else:
                    # Get latest indicators date and start from next day
                    latest_indicators_date = get_latest_indicators_date(config, symbol)
                    if latest_indicators_date and not force_recalculate:
                        target_from_date = latest_indicators_date + timedelta(days=1)
                        print(f"  Found existing indicators up to {latest_indicators_date.strftime('%Y-%m-%d')}")
                        print(f"  Target date for new indicators: {target_from_date.strftime('%Y-%m-%d')}")
                    else:
                        target_from_date = None
                        print(f"  No existing indicators found, calculating from beginning")
                
                # For incremental updates, fetch warmup window for accurate EMA calculations
                if target_from_date:
                    # Fetch extra 220 trading days (approximately 300 calendar days) for warmup
                    # This ensures EMA(200) has sufficient historical data
                    warmup_from_date = target_from_date - timedelta(days=300)
                    print(f"  Using warmup window from {warmup_from_date.strftime('%Y-%m-%d')} for accurate EMA calculations")
                else:
                    warmup_from_date = None
                
                price_data = fetch_price_data_from_db(config, symbol, warmup_from_date)
                data_source = "database"
                
            except Exception as db_error:
                print(f"  Warning: Failed to load from database: {db_error}")
                print(f"  Falling back to CSV data...")
                price_data = load_price_data_from_csv(symbol, config.data_file_path, from_date)
                data_source = "CSV"
        else:
            # Load from CSV
            price_data = load_price_data_from_csv(symbol, config.data_file_path, from_date)
            data_source = "CSV"
        
        if price_data.empty:
            print(f"  No price data available for {symbol}")
            return False
        
        print(f"  Loaded {len(price_data)} price records from {data_source}")
        print(f"  Date range: {price_data.index.min().strftime('%Y-%m-%d')} to {price_data.index.max().strftime('%Y-%m-%d')}")
        
        # Calculate indicators on full dataset (including warmup window)
        print(f"  Calculating technical indicators on full dataset...")
        indicators_df = create_indicators_dataframe(price_data, symbol)
        
        if indicators_df.empty:
            print(f"  No indicators calculated for {symbol}")
            return False
        
        print(f"  Calculated {len(indicators_df)} indicators records")
        
        # For incremental updates, filter to only new indicators that need to be upserted
        if target_from_date and data_source == "database":
            # Convert target_from_date to pandas datetime for comparison
            target_from_pd = pd.to_datetime(target_from_date)
            original_count = len(indicators_df)
            
            # Filter indicators_df to only include dates >= target_from_date
            indicators_df = indicators_df[pd.to_datetime(indicators_df['date']) >= target_from_pd]
            
            if indicators_df.empty:
                print(f"  No new indicators to upsert (all dates before {target_from_date.strftime('%Y-%m-%d')})")
                return True  # This is success - no new data to process
            
            print(f"  Filtered to {len(indicators_df)} new indicators records (from {original_count} total)")
            print(f"  New indicators date range: {pd.to_datetime(indicators_df['date']).min().strftime('%Y-%m-%d')} to {pd.to_datetime(indicators_df['date']).max().strftime('%Y-%m-%d')}")
        
        # Save to database if enabled
        if config.db_enabled:
            try:
                upsert_indicators_data(config, indicators_df, symbol)
                print(f"  ✓ Indicators saved to database for {symbol}")
            except Exception as db_error:
                print(f"  ❌ Failed to save indicators to database: {db_error}")
                return False
        else:
            print(f"  Database disabled - indicators calculated but not persisted")
        
        return True
        
    except FileNotFoundError as e:
        print(f"  ❌ File not found: {e}")
        return False
    except Exception as e:
        print(f"  ❌ Error processing {symbol}: {e}")
        return False


def update_all_symbols(config, force_recalculate: bool = False, 
                      from_date: Optional[datetime] = None) -> Tuple[int, int]:
    """
    Update indicators for all symbols that need processing.
    
    Args:
        config (ConfigData): Configuration object
        force_recalculate (bool): If True, recalculate all indicators
        from_date (datetime, optional): Calculate indicators from this date
    
    Returns:
        Tuple[int, int]: (success_count, error_count)
    """
    success_count = 0
    error_count = 0
    
    if config.db_enabled and not force_recalculate:
        # Get symbols from database that need indicators updates
        print("Checking database for symbols needing indicators updates...")
        symbols_data = get_symbols_needing_indicators_update(config)
        symbols = [f"{ticker}.NS" for ticker, _ in symbols_data]  # Add .NS suffix
        print(f"Found {len(symbols)} symbols needing indicators updates from database")
    else:
        # Get all symbols from source file
        print("Loading symbols from source file...")
        csv_file = os.path.join("sources", config.source_file)
        if not os.path.exists(csv_file):
            print(f"Error: Source file not found: {csv_file}")
            return 0, 1
        
        symbols = FileHandler.read_nifty_symbols(csv_file)
        symbols = [f"{symbol}.NS" for symbol in symbols]  # Add .NS suffix
        print(f"Loaded {len(symbols)} symbols from {config.source_file}")
    
    if not symbols:
        print("No symbols to process")
        return 0, 0
    
    print(f"\nProcessing {len(symbols)} symbols for indicators calculation...")
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{len(symbols)}] {symbol}")
        
        success = process_single_symbol(symbol, config, force_recalculate, from_date)
        if success:
            success_count += 1
        else:
            error_count += 1
    
    return success_count, error_count


def main():
    """
    Main entry point for the indicators pipeline.
    """
    parser = argparse.ArgumentParser(description='Technical Indicators Pipeline - Calculate and Update Stock Indicators')
    parser.add_argument('--mode', choices=['update-all', 'single', 'recalculate-all'], required=True,
                       help='Processing mode: update-all (incremental), single (one symbol), recalculate-all (full recalc)')
    parser.add_argument('--symbol', type=str, help='Stock symbol (required for single mode)')
    parser.add_argument('--from-date', type=str, help='Calculate indicators from this date (YYYY-MM-DD)')
    parser.add_argument('--config', default='configs/config.json', help='Configuration file path')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == 'single' and not args.symbol:
        parser.error("--symbol is required for single mode")
    
    # Parse from_date if provided
    from_date = None
    if args.from_date:
        try:
            from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
        except ValueError:
            parser.error("Invalid date format. Use YYYY-MM-DD")
    
    # Display header
    print("=" * 70)
    print("Technical Indicators Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode}")
    if args.symbol:
        print(f"Symbol: {args.symbol}")
    if from_date:
        print(f"From Date: {from_date.strftime('%Y-%m-%d')}")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 70)
    
    # Load configuration
    if not os.path.exists(args.config):
        print(f"Error: Configuration file not found: {args.config}")
        return 1
    
    config = read_config(args.config)
    print(f"Data directory: {config.data_file_path}")
    print(f"Database enabled: {'YES' if config.db_enabled else 'NO'}")
    if config.db_enabled:
        print(f"Database updates: {'YES' if config.db_update_enabled else 'NO'}")
        print(f"Database logging: {'YES' if config.db_logging_enabled else 'NO'}")
    
    # Initialize database schema if needed
    if config.db_enabled:
        try:
            from DatabaseSetup import setup_database_schema, verify_schema
            print("\nVerifying database schema...")
            if not verify_schema(config):
                print("Schema verification failed - initializing database...")
                if setup_database_schema(config):
                    print("✅ Database schema initialized successfully")
                else:
                    print("❌ Database schema initialization failed")
                    return 1
            else:
                print("✅ Database schema verified")
        except Exception as db_error:
            print(f"Database setup error: {db_error}")
            if args.mode in ['update-all', 'recalculate-all']:
                print("Cannot proceed without database for batch processing")
                return 1
    
    try:
        if args.mode == 'single':
            # Process single symbol
            print(f"\nProcessing single symbol: {args.symbol}")
            success = process_single_symbol(args.symbol, config, False, from_date)
            
            if success:
                print(f"\n✅ Successfully processed indicators for {args.symbol}")
                return 0
            else:
                print(f"\n❌ Failed to process indicators for {args.symbol}")
                return 1
                
        elif args.mode == 'update-all':
            # Incremental update for all symbols
            print(f"\nRunning incremental indicators update for all symbols...")
            success_count, error_count = update_all_symbols(config, False, from_date)
            
        elif args.mode == 'recalculate-all':
            # Full recalculation for all symbols
            print(f"\nRunning full indicators recalculation for all symbols...")
            success_count, error_count = update_all_symbols(config, True, from_date)
        
        # Display summary for batch modes
        if args.mode in ['update-all', 'recalculate-all']:
            print(f"\nIndicators pipeline completed!")
            print(f"Successfully processed: {success_count} symbols")
            print(f"Errors encountered: {error_count} symbols")
            print(f"Database integration: {'ENABLED' if config.db_enabled else 'DISABLED'}")
            
            # Return non-zero exit code if there were errors
            if error_count > 0:
                print(f"Warning: {error_count} symbols failed to process")
                return 1
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    print(f"\nOperation completed successfully!")
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)