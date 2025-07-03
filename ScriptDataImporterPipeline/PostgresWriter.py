import psycopg2
import pandas as pd

# Update these with your actual database credentials
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'your_database',
    'user': 'your_user',
    'password': 'your_password'
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def upsert_stock_data(df: pd.DataFrame, table_name: str, symbol: str):
    """
    Upsert stock data from DataFrame into PostgreSQL table.
    Assumes DataFrame has columns: Date, Open, High, Low, Close, Volume, Dividends, Stock Splits
    """
    conn = get_connection()
    cur = conn.cursor()
    for _, row in df.iterrows():
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
            symbol,
            row['Date'],
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume'],
            row.get('Dividends', 0),
            row.get('Stock Splits', 0)
        ))
    conn.commit()
    cur.close()
    conn.close()
