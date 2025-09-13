#!/usr/bin/env python3
"""
TradeSetup Pipeline - Unified Entry Point for Stock Market Data Pipeline

This is the unified entry point for both stock data fetching and technical
indicators calculation. It provides a consistent interface for all pipeline
operations with subcommands for different functionality.

Usage Examples:
    # Stock Data Operations
    python pipeline.py stock single --symbol RELIANCE.NS
    python pipeline.py stock sync-all
    python pipeline.py stock cron-update
    
    # Technical Indicators Operations  
    python pipeline.py indicators update-all
    python pipeline.py indicators single --symbol RELIANCE.NS
    python pipeline.py indicators recalculate-all
    
    # Configuration and Status
    python pipeline.py config show
    python pipeline.py status

Features:
- Unified command structure for all operations
- Consistent error handling and logging
- Optimized database connection management
- Configurable stock filtering and date ranges
- Simplified cron job setup

Author: TradeSetup Team
Created: 2025-09-13
"""

import os
import sys
import argparse
import json
from datetime import datetime
from typing import Optional, List

# Add the ScriptDataImporterPipeline directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'ScriptDataImporterPipeline'))

from ConfigReader import read_config
from DatabaseManager import initialize_database, get_database_manager


def setup_database(config):
    """
    Initialize database connections and verify schema.
    
    Args:
        config (ConfigData): Configuration object
        
    Returns:
        bool: True if database setup successful or not enabled
    """
    if not config.db_enabled:
        return True
    
    try:
        # Initialize database manager
        initialize_database(config)
        
        # Verify schema if database operations are enabled
        from DatabaseSetup import verify_schema, setup_database_schema
        
        print("Verifying database schema...")
        if not verify_schema(config):
            print("Schema verification failed - initializing database...")
            if setup_database_schema(config):
                print("✅ Database schema initialized successfully")
            else:
                print("❌ Database schema initialization failed")
                return False
        else:
            print("✅ Database schema verified")
        
        return True
        
    except Exception as e:
        print(f"Database setup error: {e}")
        return False


def handle_stock_operations(args, config):
    """
    Handle stock data pipeline operations.
    
    Args:
        args: Command line arguments
        config: Configuration object
        
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    from main import (
        process_single_symbol, sync_all_nifty_symbols, 
        sync_single_symbol, cron_incremental_update
    )
    import FileHandler
    
    try:
        # Apply command-line overrides to config
        effective_config = apply_config_overrides(config, args)
        
        if args.stock_mode == 'single':
            if not args.symbol:
                print("Error: --symbol is required for single mode")
                return 1
            
            print(f"Processing single symbol: {args.symbol}")
            success = process_single_symbol(
                args.symbol, 
                effective_config.data_file_path, 
                effective_config
            )
            return 0 if success else 1
        
        elif args.stock_mode == 'sync-all':
            print("Syncing all Nifty symbols...")
            csv_file = os.path.join("sources", effective_config.source_file)
            success_count, error_count = sync_all_nifty_symbols(
                effective_config.data_file_path, csv_file, effective_config
            )
            
        elif args.stock_mode == 'sync-symbol':
            if not args.symbol:
                print("Error: --symbol is required for sync-symbol mode")
                return 1
            
            print(f"Syncing single symbol: {args.symbol}")
            csv_file = os.path.join("sources", effective_config.source_file)
            success = sync_single_symbol(
                args.symbol, effective_config.data_file_path, csv_file, effective_config
            )
            return 0 if success else 1
            
        elif args.stock_mode == 'cron-update':
            print("Running cron incremental update...")
            csv_file = os.path.join("sources", effective_config.source_file)
            success_count, error_count = cron_incremental_update(
                effective_config.data_file_path, csv_file, effective_config
            )
        
        else:
            print(f"Unknown stock mode: {args.stock_mode}")
            return 1
        
        # Display summary for batch operations
        if args.stock_mode in ['sync-all', 'cron-update']:
            print(f"\nStock pipeline completed!")
            print(f"Successfully processed: {success_count} stocks")
            print(f"Errors encountered: {error_count} stocks")
            
            if error_count > 0:
                print(f"Warning: {error_count} stocks failed to process")
                return 1
        
        return 0
        
    except Exception as e:
        print(f"Stock pipeline error: {e}")
        return 1


def handle_indicators_operations(args, config):
    """
    Handle technical indicators pipeline operations.
    
    Args:
        args: Command line arguments
        config: Configuration object
        
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    from indicators_pipeline import (
        process_single_symbol, update_all_symbols
    )
    
    try:
        # Apply command-line overrides to config
        effective_config = apply_config_overrides(config, args)
        
        if args.indicators_mode == 'single':
            if not args.symbol:
                print("Error: --symbol is required for single mode")
                return 1
            
            print(f"Processing indicators for: {args.symbol}")
            success = process_single_symbol(
                args.symbol, effective_config, 
                args.indicators_mode == 'recalculate-all',
                args.from_date
            )
            return 0 if success else 1
        
        elif args.indicators_mode == 'update-all':
            print("Running incremental indicators update...")
            success_count, error_count = update_all_symbols(
                effective_config, False, args.from_date
            )
            
        elif args.indicators_mode == 'recalculate-all':
            print("Running full indicators recalculation...")
            success_count, error_count = update_all_symbols(
                effective_config, True, args.from_date
            )
        
        else:
            print(f"Unknown indicators mode: {args.indicators_mode}")
            return 1
        
        # Display summary for batch operations
        if args.indicators_mode in ['update-all', 'recalculate-all']:
            print(f"\nIndicators pipeline completed!")
            print(f"Successfully processed: {success_count} symbols")
            print(f"Errors encountered: {error_count} symbols")
            
            if error_count > 0:
                print(f"Warning: {error_count} symbols failed to process")
                return 1
        
        return 0
        
    except Exception as e:
        print(f"Indicators pipeline error: {e}")
        return 1


def handle_config_operations(args, config):
    """
    Handle configuration operations.
    
    Args:
        args: Command line arguments
        config: Configuration object
        
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    if args.config_mode == 'show':
        print("=" * 50)
        print("TradeSetup Configuration")
        print("=" * 50)
        print(f"Data Directory: {config.data_file_path}")
        print(f"Source File: {config.source_file}")
        print(f"Start Date: {config.start_date}")
        print(f"Database Enabled: {'YES' if config.db_enabled else 'NO'}")
        
        if config.db_enabled:
            print(f"Database Updates: {'YES' if config.db_update_enabled else 'NO'}")
            print(f"Database Logging: {'YES' if config.db_logging_enabled else 'NO'}")
            
            # Show database connection status
            try:
                db_manager = get_database_manager()
                if db_manager.is_enabled():
                    result = db_manager.execute_query("SELECT NOW()", fetch_one=True)
                    print(f"Database Connection: ✅ ACTIVE")
                    print(f"Database Time: {result[0] if result else 'N/A'}")
                else:
                    print(f"Database Connection: ❌ NOT INITIALIZED")
            except Exception as e:
                print(f"Database Connection: ❌ ERROR - {e}")
        
        # Show available symbols count
        try:
            import FileHandler
            csv_file = os.path.join("sources", config.source_file)
            if os.path.exists(csv_file):
                symbols = FileHandler.read_nifty_symbols(csv_file)
                print(f"Available Symbols: {len(symbols)} stocks")
            else:
                print(f"Source File: ❌ NOT FOUND")
        except Exception as e:
            print(f"Source File: ❌ ERROR - {e}")
        
        # Show stock filtering if configured
        if hasattr(config, 'stock_filter') and config.stock_filter:
            print(f"Stock Filter: {config.stock_filter}")
        
        if hasattr(config, 'date_range') and config.date_range:
            print(f"Date Range Override: {config.date_range}")
        
        return 0
    
    else:
        print(f"Unknown config mode: {args.config_mode}")
        return 1


def handle_status_operations(args, config):
    """
    Handle status operations.
    
    Args:
        args: Command line arguments
        config: Configuration object
        
    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    print("=" * 50)
    print("TradeSetup Pipeline Status")
    print("=" * 50)
    
    # Check data directory
    if os.path.exists(config.data_file_path):
        csv_files = [f for f in os.listdir(config.data_file_path) if f.endswith('.csv')]
        print(f"Data Directory: ✅ {len(csv_files)} CSV files")
    else:
        print(f"Data Directory: ❌ NOT FOUND")
    
    # Check source file
    csv_file = os.path.join("sources", config.source_file)
    if os.path.exists(csv_file):
        print(f"Source File: ✅ FOUND")
    else:
        print(f"Source File: ❌ NOT FOUND")
    
    # Check database status
    if config.db_enabled:
        try:
            db_manager = get_database_manager()
            if db_manager.is_enabled():
                # Check tables exist
                result = db_manager.execute_query("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name IN ('stocks', 'stock_price_daily', 'stock_indicators_daily')
                """, fetch_one=True)
                table_count = result[0] if result else 0
                
                if table_count >= 3:
                    print(f"Database Schema: ✅ COMPLETE")
                    
                    # Check data counts
                    stock_count = db_manager.execute_query(
                        "SELECT COUNT(*) FROM stocks", fetch_one=True
                    )[0]
                    price_count = db_manager.execute_query(
                        "SELECT COUNT(*) FROM stock_price_daily", fetch_one=True
                    )[0]
                    indicators_count = db_manager.execute_query(
                        "SELECT COUNT(*) FROM stock_indicators_daily", fetch_one=True
                    )[0]
                    
                    print(f"Stocks: {stock_count:,} records")
                    print(f"Price Data: {price_count:,} records")
                    print(f"Indicators: {indicators_count:,} records")
                else:
                    print(f"Database Schema: ❌ INCOMPLETE")
            else:
                print(f"Database: ❌ NOT CONNECTED")
        except Exception as e:
            print(f"Database: ❌ ERROR - {e}")
    else:
        print(f"Database: ⚠️ DISABLED")
    
    return 0


def apply_config_overrides(config, args):
    """
    Apply command-line arguments to override configuration settings.
    
    Args:
        config: Original configuration object
        args: Command line arguments
        
    Returns:
        Modified configuration object
    """
    # Create a copy to avoid modifying original
    import copy
    effective_config = copy.deepcopy(config)
    
    # Apply date range override
    if hasattr(args, 'from_date') and args.from_date:
        effective_config.start_date = args.from_date.strftime('%Y-%m-%d')
    
    # Apply symbol filter
    if hasattr(args, 'symbol') and args.symbol:
        effective_config.stock_filter = args.symbol
    
    return effective_config


def main():
    """
    Main entry point for the unified pipeline.
    """
    parser = argparse.ArgumentParser(
        description='TradeSetup Pipeline - Unified Stock Market Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Stock Data Operations:
    python pipeline.py stock single --symbol RELIANCE.NS
    python pipeline.py stock sync-all
    python pipeline.py stock cron-update
    
  Technical Indicators:
    python pipeline.py indicators update-all
    python pipeline.py indicators single --symbol RELIANCE.NS
    python pipeline.py indicators recalculate-all
    
  Configuration & Status:
    python pipeline.py config show
    python pipeline.py status
        """
    )
    
    # Global arguments
    parser.add_argument('--config', default='configs/config.json', 
                       help='Configuration file path')
    parser.add_argument('--from-date', type=str,
                       help='Override start date (YYYY-MM-DD)')
    parser.add_argument('--symbol', type=str,
                       help='Stock symbol for single operations')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Stock data subcommand
    stock_parser = subparsers.add_parser('stock', help='Stock data operations')
    stock_parser.add_argument('stock_mode', 
                             choices=['single', 'sync-all', 'sync-symbol', 'cron-update'],
                             help='Stock operation mode')
    
    # Indicators subcommand
    indicators_parser = subparsers.add_parser('indicators', help='Technical indicators operations')
    indicators_parser.add_argument('indicators_mode',
                                  choices=['single', 'update-all', 'recalculate-all'],
                                  help='Indicators operation mode')
    
    # Config subcommand
    config_parser = subparsers.add_parser('config', help='Configuration operations')
    config_parser.add_argument('config_mode', choices=['show'], help='Configuration mode')
    
    # Status subcommand
    status_parser = subparsers.add_parser('status', help='Show pipeline status')
    
    args = parser.parse_args()
    
    # Validate command
    if not args.command:
        parser.print_help()
        return 1
    
    # Parse from_date if provided
    if args.from_date:
        try:
            args.from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
        except ValueError:
            parser.error("Invalid date format. Use YYYY-MM-DD")
    
    # Display header
    print("=" * 70)
    print("TradeSetup Pipeline")
    print("=" * 70)
    print(f"Command: {args.command}")
    if hasattr(args, 'stock_mode'):
        print(f"Mode: {args.stock_mode}")
    elif hasattr(args, 'indicators_mode'):
        print(f"Mode: {args.indicators_mode}")
    if args.symbol:
        print(f"Symbol: {args.symbol}")
    if args.from_date:
        print(f"From Date: {args.from_date.strftime('%Y-%m-%d')}")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 70)
    
    # Load configuration
    if not os.path.exists(args.config):
        print(f"Error: Configuration file not found: {args.config}")
        return 1
    
    config = read_config(args.config)
    
    # Setup database if needed
    if args.command in ['stock', 'indicators'] and not setup_database(config):
        print("Database setup failed - cannot proceed with database operations")
        return 1
    
    # Route to appropriate handler
    try:
        if args.command == 'stock':
            return handle_stock_operations(args, config)
        elif args.command == 'indicators':
            return handle_indicators_operations(args, config)
        elif args.command == 'config':
            return handle_config_operations(args, config)
        elif args.command == 'status':
            return handle_status_operations(args, config)
        else:
            print(f"Unknown command: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Pipeline error: {e}")
        return 1
    finally:
        # Cleanup database connections
        try:
            db_manager = get_database_manager()
            db_manager.close_pool()
        except:
            pass


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)