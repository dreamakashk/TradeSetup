#!/usr/bin/env python3
"""
TradeSetup - Stock Market Data Pipeline
Main entry point for the stock market data analysis pipeline
"""

import os
import sys
import argparse
from datetime import datetime

# Add the ScriptDataImporterPipeline to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'ScriptDataImporterPipeline'))

import ScriptDataFetcher
from ConfigReader import read_config
from NiftyScriptsDataSyncer import sync_nifty_scripts_data, sync_symbol_data

def main():
    """Main entry point for the TradeSetup application"""
    parser = argparse.ArgumentParser(description='TradeSetup - Stock Market Data Pipeline')
    parser.add_argument('--mode', choices=['single', 'sync-all', 'sync-symbol'], default='single',
                       help='Mode: single stock, sync all Nifty stocks, or sync specific symbol')
    parser.add_argument('--symbol', default='BSE.NS', help='Stock symbol to fetch (default: BSE.NS)')
    parser.add_argument('--config', default='configs/config.json', help='Configuration file path')
    
    args = parser.parse_args()
    
    print("="*60)
    print("TradeSetup - Stock Market Data Pipeline")
    print("="*60)
    print(f"Mode: {args.mode}")
    print(f"Symbol: {args.symbol}")
    print(f"Timestamp: {datetime.now()}")
    print("="*60)
    
    # Load configuration
    config_path = args.config
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found: {config_path}")
        return 1
        
    config = read_config(config_path)
    print(f"Data directory: {config.data_file_path}")
    
    # Create data directory if it doesn't exist
    os.makedirs(config.data_file_path, exist_ok=True)
    
    try:
        if args.mode == 'single':
            print(f"\nFetching data for single symbol: {args.symbol}")
            
            # Fetch company info
            print("Fetching company information...")
            info = ScriptDataFetcher.fetch_company_info(args.symbol)
            print(f"Company: {info.get('long_name', 'Unknown')}")
            print(f"Sector: {info.get('sector', 'Unknown')}")
            print(f"Industry: {info.get('industry', 'Unknown')}")
            
            # Fetch historical data
            print("Fetching historical data...")
            data = ScriptDataFetcher.fetch_historical_data(args.symbol)
            print(f"Data shape: {data.shape}")
            print(f"Date range: {data.index.min()} to {data.index.max()}")
            
            # Save data
            ScriptDataFetcher.save_data_to_csv(data, args.symbol, config.data_file_path)
            print(f"Data saved successfully!")
            
        elif args.mode == 'sync-all':
            print("\nSyncing all Nifty stocks...")
            csv_file = os.path.join("sources", "niftytotalmarket_list.csv")
            if not os.path.exists(csv_file):
                print(f"Error: Nifty symbols file not found: {csv_file}")
                return 1
            sync_nifty_scripts_data(config.data_file_path, csv_file, db_table=None)
            
        elif args.mode == 'sync-symbol':
            print(f"\nSyncing data for symbol: {args.symbol}")
            sync_symbol_data(args.symbol, config.data_file_path, db_table=None)
            
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    print("\nOperation completed successfully!")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)