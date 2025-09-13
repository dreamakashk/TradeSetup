"""
IndicatorsWriter - Database Operations for Technical Indicators

This module handles database operations specifically for the stock_indicators_daily
table. It provides functions to upsert indicators data and manage incremental
updates efficiently.

Functions:
    upsert_indicators_data: Insert or update indicators in database
    get_latest_indicators_date: Find latest date for indicators data
    delete_indicators_data: Remove indicators data for reprocessing

Author: TradeSetup Team
Created: 2025-09-13
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Optional, List, Tuple
from datetime import datetime


def get_connection(config_data):
    """
    Create and return a PostgreSQL database connection using configuration.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
    
    Returns:
        psycopg2.connection: Active database connection object
    
    Raises:
        psycopg2.Error: If connection to database fails
        ValueError: If database is not enabled in configuration
    """
    if not config_data.db_enabled:
        raise ValueError("Database operations are not enabled in configuration")
    
    db_params = config_data.get_db_connection_params()
    if not db_params:
        raise ValueError("Database connection parameters not available")
    
    return psycopg2.connect(**db_params)


def get_latest_indicators_date(config_data, symbol: str) -> Optional[datetime]:
    """
    Get the latest date for which indicators data exists for a given symbol.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        symbol (str): Stock symbol (e.g., 'RELIANCE.NS')
    
    Returns:
        datetime: Latest date with indicators data, or None if no data exists
    """
    if not config_data.db_enabled:
        return None
    
    try:
        # Remove exchange suffix for database query
        clean_symbol = symbol.replace('.NS', '').replace('.BO', '')
        
        conn = get_connection(config_data)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT MAX(date) FROM stock_indicators_daily 
            WHERE ticker = %s
        """, (clean_symbol,))
        
        result = cur.fetchone()
        latest_date = result[0] if result[0] else None
        
        cur.close()
        conn.close()
        
        return latest_date
        
    except Exception as e:
        if config_data.db_logging_enabled:
            print(f"Error getting latest indicators date for {symbol}: {e}")
        return None


def get_latest_price_date(config_data, symbol: str) -> Optional[datetime]:
    """
    Get the latest date for which price data exists for a given symbol.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        symbol (str): Stock symbol (e.g., 'RELIANCE.NS')
    
    Returns:
        datetime: Latest date with price data, or None if no data exists
    """
    if not config_data.db_enabled:
        return None
    
    try:
        # Remove exchange suffix for database query
        clean_symbol = symbol.replace('.NS', '').replace('.BO', '')
        
        conn = get_connection(config_data)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT MAX(date) FROM stock_price_daily 
            WHERE ticker = %s
        """, (clean_symbol,))
        
        result = cur.fetchone()
        latest_date = result[0] if result[0] else None
        
        cur.close()
        conn.close()
        
        return latest_date
        
    except Exception as e:
        if config_data.db_logging_enabled:
            print(f"Error getting latest price date for {symbol}: {e}")
        return None


def upsert_indicators_data(config_data, indicators_df: pd.DataFrame, symbol: str):
    """
    Insert or update indicators data in the stock_indicators_daily table using batch operations.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        indicators_df (pd.DataFrame): Indicators data with columns:
            - ticker, date, ema_10, ema_20, ema_50, ema_100, ema_200,
              rsi, atr, supertrend, obv, ad, volume_surge
        symbol (str): Stock symbol for logging purposes
    
    Returns:
        None
    
    Raises:
        psycopg2.Error: If database operation fails
    """
    if not config_data.db_enabled:
        return
    
    # Check if database updates are enabled
    if not config_data.db_update_enabled:
        if config_data.db_logging_enabled:
            print(f"Database updates disabled - skipping indicators data for {symbol}")
        return
    
    if indicators_df.empty:
        if config_data.db_logging_enabled:
            print(f"No indicators data to insert for {symbol}")
        return
    
    if config_data.db_logging_enabled:
        print(f"Inserting {len(indicators_df)} indicators records for symbol: {symbol}")
    
    # Prepare data for batch insert
    data_tuples = []
    for _, row in indicators_df.iterrows():
        data_tuple = (
            row['ticker'],
            row['date'],
            float(row['ema_10']) if pd.notna(row['ema_10']) else None,
            float(row['ema_20']) if pd.notna(row['ema_20']) else None,
            float(row['ema_50']) if pd.notna(row['ema_50']) else None,
            float(row['ema_100']) if pd.notna(row['ema_100']) else None,
            float(row['ema_200']) if pd.notna(row['ema_200']) else None,
            float(row['rsi']) if pd.notna(row['rsi']) else None,
            float(row['atr']) if pd.notna(row['atr']) else None,
            float(row['supertrend']) if pd.notna(row['supertrend']) else None,
            float(row['obv']) if pd.notna(row['obv']) else None,
            float(row['ad']) if pd.notna(row['ad']) else None,
            float(row['volume_surge']) if pd.notna(row['volume_surge']) else None
        )
        data_tuples.append(data_tuple)
    
    conn = get_connection(config_data)
    cur = conn.cursor()
    
    try:
        # Use batch insert with ON CONFLICT for efficient upserts
        upsert_query = """
            INSERT INTO stock_indicators_daily 
            (ticker, date, ema_10, ema_20, ema_50, ema_100, ema_200, 
             rsi, atr, supertrend, obv, ad, volume_surge)
            VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
                ema_10 = EXCLUDED.ema_10,
                ema_20 = EXCLUDED.ema_20,
                ema_50 = EXCLUDED.ema_50,
                ema_100 = EXCLUDED.ema_100,
                ema_200 = EXCLUDED.ema_200,
                rsi = EXCLUDED.rsi,
                atr = EXCLUDED.atr,
                supertrend = EXCLUDED.supertrend,
                obv = EXCLUDED.obv,
                ad = EXCLUDED.ad,
                volume_surge = EXCLUDED.volume_surge;
        """
        
        # Execute batch upsert
        execute_values(
            cur, 
            upsert_query,
            data_tuples,
            template=None,
            page_size=1000  # Process in chunks of 1000 for memory efficiency
        )
        
        # Commit all changes as a single transaction
        conn.commit()
        rows_affected = cur.rowcount if cur.rowcount > 0 else len(indicators_df)
        
        if config_data.db_logging_enabled:
            print(f"✓ Successfully batch-upserted {rows_affected} indicators rows for {symbol}")
            try:
                # Handle date range logging safely
                if 'date' in indicators_df.columns:
                    date_series = pd.to_datetime(indicators_df['date'])
                    start_date = date_series.min().strftime('%Y-%m-%d')
                    end_date = date_series.max().strftime('%Y-%m-%d')
                    print(f"  Date range: {start_date} to {end_date}")
                    
                    # Show sample indicator values for latest date
                    latest_row = indicators_df.iloc[-1]
                    print(f"  Latest EMA-20: {latest_row['ema_20']:.2f}" if pd.notna(latest_row['ema_20']) else "")
                    print(f"  Latest RSI: {latest_row['rsi']:.2f}" if pd.notna(latest_row['rsi']) else "")
                    
            except Exception as log_error:
                print(f"  (Date range logging failed: {log_error})")
        
    except Exception as e:
        conn.rollback()
        print(f"Failed to batch upsert indicators data for {symbol}: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def fetch_price_data_from_db(config_data, symbol: str, start_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Fetch stock price data from database for indicators calculation.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        symbol (str): Stock symbol (e.g., 'RELIANCE.NS')
        start_date (datetime, optional): Start date for data fetch
    
    Returns:
        pd.DataFrame: Price data with Date index and OHLCV columns
    """
    if not config_data.db_enabled:
        raise ValueError("Database operations are not enabled")
    
    # Remove exchange suffix for database query
    clean_symbol = symbol.replace('.NS', '').replace('.BO', '')
    
    conn = get_connection(config_data)
    
    try:
        # Build query with optional date filter
        query = """
            SELECT date, open, high, low, close, volume
            FROM stock_price_daily 
            WHERE ticker = %s
        """
        params = [clean_symbol]
        
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        
        query += " ORDER BY date ASC"
        
        # Fetch data
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['date'])
        
        if df.empty:
            return df
        
        # Set date as index and rename columns to match expected format
        df.set_index('date', inplace=True)
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        return df
        
    finally:
        conn.close()


def get_symbols_needing_indicators_update(config_data) -> List[Tuple[str, Optional[datetime]]]:
    """
    Get list of symbols that need indicators updates along with their latest indicators date.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
    
    Returns:
        List[Tuple[str, datetime]]: List of (symbol, latest_indicators_date) tuples
    """
    if not config_data.db_enabled:
        return []
    
    conn = get_connection(config_data)
    cur = conn.cursor()
    
    try:
        # Get all symbols that have price data but missing or outdated indicators
        query = """
            SELECT DISTINCT p.ticker,
                   MAX(p.date) as latest_price_date,
                   MAX(i.date) as latest_indicators_date
            FROM stock_price_daily p
            LEFT JOIN stock_indicators_daily i ON p.ticker = i.ticker
            GROUP BY p.ticker
            HAVING MAX(p.date) > COALESCE(MAX(i.date), '1900-01-01')
            ORDER BY p.ticker
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        symbols_to_update = []
        for ticker, latest_price_date, latest_indicators_date in results:
            symbols_to_update.append((ticker, latest_indicators_date))
        
        return symbols_to_update
        
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # Test the indicators writer with sample configuration
    from ConfigReader import read_config
    import os
    
    print("Testing IndicatorsWriter...")
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "config.json")
    config = read_config(config_path)
    
    if config.db_enabled:
        try:
            # Test getting symbols needing updates
            symbols = get_symbols_needing_indicators_update(config)
            print(f"Found {len(symbols)} symbols needing indicators updates")
            
            if symbols:
                first_symbol = symbols[0]
                print(f"First symbol: {first_symbol[0]}, Latest indicators date: {first_symbol[1]}")
        except Exception as e:
            print(f"Database test failed: {e}")
    else:
        print("Database not enabled in configuration")