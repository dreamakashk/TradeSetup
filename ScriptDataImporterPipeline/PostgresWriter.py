"""
PostgresWriter - Database Operations Module

This module handles PostgreSQL database operations for the TradeSetup application.
Provides functions to write stock market data to PostgreSQL/TimescaleDB tables
with upsert capabilities to handle duplicate data gracefully.

Note: This module is optional and the application can work in CSV-only mode
if database operations are not required.

Functions:
    get_connection: Create database connection
    upsert_stock_data: Insert or update stock data in database

Author: TradeSetup Team
Created: 2025-09-13
"""

import psycopg2     # PostgreSQL database adapter
import pandas as pd # For DataFrame operations
from psycopg2.extras import execute_values  # For efficient batch operations

def get_connection(config_data):
    """
    Create and return a PostgreSQL database connection using configuration.
    
    Uses the database configuration from ConfigData to establish a connection.
    Supports both environment variables and configuration file parameters.
    
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

def upsert_stock_metadata(config_data, symbol: str, company_info: dict):
    """
    Insert or update stock metadata in the stocks table.
    
    Creates or updates the stock record with company information including
    name, sector, industry, and other metadata. This should be called before
    inserting price data to ensure referential integrity.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        symbol (str): Stock ticker symbol (e.g., 'RELIANCE')
        company_info (dict): Company information from yfinance
    
    Returns:
        None
    
    Raises:
        psycopg2.Error: If database operation fails
    """
    if not config_data.db_enabled:
        return
    
    # Remove .NS suffix for database storage (keep base symbol)
    clean_symbol = symbol.replace('.NS', '').replace('.BO', '')
    
    conn = get_connection(config_data)
    cur = conn.cursor()
    
    try:
        # Upsert stock metadata
        cur.execute("""
            INSERT INTO stocks (ticker, company_name, sector, industry, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                updated_at = CURRENT_TIMESTAMP;
        """, (
            clean_symbol,
            company_info.get('long_name', '')[:255] if company_info.get('long_name') else None,
            company_info.get('sector', '')[:32] if company_info.get('sector') else None,
            company_info.get('industry', '')[:32] if company_info.get('industry') else None,
            True  # is_active
        ))
        
        conn.commit()
        print(f"Updated stock metadata for {clean_symbol}")
        
    except Exception as e:
        conn.rollback()
        print(f"Failed to update stock metadata for {symbol}: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def upsert_stock_price_data(config_data, df: pd.DataFrame, symbol: str):
    """
    Insert or update stock price data in the stock_price_daily table using efficient batch operations.
    
    Performs high-performance batch upsert operation using execute_values for optimal
    scalability with large datasets. Uses a two-step process: batch insert with ON CONFLICT
    to handle duplicates efficiently.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        df (pd.DataFrame): Stock data with columns:
            - Date: Trading date (index or column)
            - Open, High, Low, Close: Price data
            - Volume: Trading volume
            - Dividends: Dividend amounts (optional)
            - Stock Splits: Stock split ratios (optional)
        symbol (str): Stock symbol for the data (e.g., 'RELIANCE.NS')
    
    Returns:
        None
    
    Raises:
        psycopg2.Error: If database operation fails
    
    Note:
        - Uses actual database schema with ticker/date primary key
        - Removes exchange suffix (.NS/.BO) for database storage
        - Handles missing dividend/split data gracefully
        - Optimized for high-performance batch operations (10-100x faster than row-by-row)
    """
    if not config_data.db_enabled:
        return
    
    if df.empty:
        print(f"No data to insert for {symbol}")
        return
    
    # Remove .NS suffix for database storage (keep base symbol)
    clean_symbol = symbol.replace('.NS', '').replace('.BO', '')
    
    # Prepare data for batch insert - convert DataFrame to list of tuples
    data_tuples = []
    for _, row in df.iterrows():
        data_tuple = (
            clean_symbol,                        # ticker (without exchange suffix)
            row['Date'],                         # date
            float(row['Open']) if pd.notna(row['Open']) else None,     # open
            float(row['High']) if pd.notna(row['High']) else None,     # high  
            float(row['Low']) if pd.notna(row['Low']) else None,       # low
            float(row['Close']) if pd.notna(row['Close']) else None,   # close
            int(row['Volume']) if pd.notna(row['Volume']) else None,   # volume
            float(row.get('Dividends', 0)) if pd.notna(row.get('Dividends', 0)) else 0.0,  # dividend
            float(row.get('Stock Splits', 0)) if pd.notna(row.get('Stock Splits', 0)) else 0.0  # split_factor
        )
        data_tuples.append(data_tuple)
    
    conn = get_connection(config_data)
    cur = conn.cursor()
    
    try:
        # Use batch insert with ON CONFLICT for efficient upserts
        # This is 10-100x faster than individual row inserts
        upsert_query = """
            INSERT INTO stock_price_daily 
            (ticker, date, open, high, low, close, volume, dividend, split_factor, created, updated)
            VALUES %s
            ON CONFLICT (ticker, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                dividend = EXCLUDED.dividend,
                split_factor = EXCLUDED.split_factor,
                updated = CURRENT_TIMESTAMP;
        """
        
        # Execute batch upsert with all data at once
        execute_values(
            cur, 
            upsert_query,
            data_tuples,
            template=None,
            page_size=1000  # Process in chunks of 1000 for memory efficiency
        )
        
        # Commit all changes as a single transaction
        conn.commit()
        rows_affected = cur.rowcount if cur.rowcount > 0 else len(df)
        print(f"Successfully batch-upserted {rows_affected} rows for {clean_symbol} in stock_price_daily table")
        
    except Exception as e:
        conn.rollback()
        print(f"Failed to batch upsert stock data for {symbol}: {e}")
        raise
    finally:
        cur.close()
        conn.close()
