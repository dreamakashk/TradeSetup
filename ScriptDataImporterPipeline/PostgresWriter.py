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

# Database connection configuration
# TODO: Replace with actual database credentials or use environment variables
DB_CONFIG = {
    'host': 'localhost',        # Database server hostname
    'port': 5432,              # PostgreSQL port (default: 5432)
    'database': 'your_database', # Database name
    'user': 'your_user',       # Database username
    'password': 'your_password' # Database password
}

def get_connection():
    """
    Create and return a PostgreSQL database connection.
    
    Uses the configuration from DB_CONFIG to establish a connection to the
    PostgreSQL database. This function should be called whenever database
    operations are needed.
    
    Returns:
        psycopg2.connection: Active database connection object
    
    Raises:
        psycopg2.Error: If connection to database fails
    """
    return psycopg2.connect(**DB_CONFIG)

def upsert_stock_data(df: pd.DataFrame, table_name: str, symbol: str):
    """
    Insert or update stock data from DataFrame into PostgreSQL table.
    
    Performs an upsert operation (INSERT ... ON CONFLICT DO UPDATE) to handle
    both new data insertion and updates to existing records. This ensures
    data integrity when re-processing historical data or updating with
    corrected values.
    
    Args:
        df (pd.DataFrame): Stock data with columns:
            - Date: Trading date (index or column)
            - Open, High, Low, Close: Price data
            - Volume: Trading volume
            - Dividends: Dividend amounts (optional)
            - Stock Splits: Stock split ratios (optional)
        table_name (str): Target database table name (e.g., 'stock_prices')
        symbol (str): Stock symbol for the data (e.g., 'RELIANCE.NS')
    
    Returns:
        None
    
    Raises:
        psycopg2.Error: If database operation fails
    
    Note:
        - Assumes table has a composite primary key on (symbol, date)
        - Missing Dividends/Stock Splits columns default to 0
        - All database changes are committed as a single transaction
    """
    # Establish database connection
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Process each row from the DataFrame
        for _, row in df.iterrows():
            # Execute upsert SQL with conflict resolution
            cur.execute(f'''
                INSERT INTO {table_name} (symbol, date, open, high, low, close, volume, dividends, stock_splits)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    dividends = EXCLUDED.dividends,
                    stock_splits = EXCLUDED.stock_splits;
            ''', (
                symbol,                           # Stock symbol
                row['Date'],                      # Trading date
                row['Open'],                      # Opening price
                row['High'],                      # Highest price
                row['Low'],                       # Lowest price
                row['Close'],                     # Closing price
                row['Volume'],                    # Trading volume
                row.get('Dividends', 0),         # Dividend amount (default: 0)
                row.get('Stock Splits', 0)       # Stock split ratio (default: 0)
            ))
        
        # Commit all changes as a single transaction
        conn.commit()
        
    finally:
        # Ensure resources are properly cleaned up
        cur.close()
        conn.close()
