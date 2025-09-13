"""
TechnicalIndicators - Technical Analysis Calculation Module

This module provides functions to calculate various technical indicators from
stock price data. All indicators are calculated using pandas DataFrames and
are optimized for batch processing of multiple stocks.

Indicators included:
- Exponential Moving Averages (EMA 10, 20, 50, 100, 200)
- Relative Strength Index (RSI)
- Average True Range (ATR)
- Supertrend
- On Balance Volume (OBV)
- Accumulation/Distribution Line (AD)
- Volume Surge

Author: TradeSetup Team
Created: 2025-09-13
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional


def calculate_ema(prices: pd.Series, period: int) -> pd.Series:
    """
    Calculate Exponential Moving Average (EMA) for given period.
    
    Args:
        prices (pd.Series): Price series (typically close prices)
        period (int): EMA period (e.g., 10, 20, 50, 100, 200)
    
    Returns:
        pd.Series: EMA values
    """
    return prices.ewm(span=period, adjust=False).mean()


def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        prices (pd.Series): Price series (typically close prices)
        period (int): RSI period (default: 14)
    
    Returns:
        pd.Series: RSI values (0-100)
    """
    delta = prices.diff()
    gains = delta.where(delta > 0, 0)
    losses = -delta.where(delta < 0, 0)
    
    avg_gains = gains.rolling(window=period).mean()
    avg_losses = losses.rolling(window=period).mean()
    
    rs = avg_gains / avg_losses
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Average True Range (ATR).
    
    Args:
        high (pd.Series): High prices
        low (pd.Series): Low prices
        close (pd.Series): Close prices
        period (int): ATR period (default: 14)
    
    Returns:
        pd.Series: ATR values
    """
    # True Range calculation
    hl = high - low
    hc = np.abs(high - close.shift())
    lc = np.abs(low - close.shift())
    
    true_range = np.maximum(hl, np.maximum(hc, lc))
    atr = true_range.rolling(window=period).mean()
    
    return atr


def calculate_supertrend(high: pd.Series, low: pd.Series, close: pd.Series, 
                        period: int = 10, multiplier: float = 3.0) -> pd.Series:
    """
    Calculate Supertrend indicator.
    
    Args:
        high (pd.Series): High prices
        low (pd.Series): Low prices
        close (pd.Series): Close prices
        period (int): ATR period (default: 10)
        multiplier (float): ATR multiplier (default: 3.0)
    
    Returns:
        pd.Series: Supertrend values
    """
    atr = calculate_atr(high, low, close, period)
    hl2 = (high + low) / 2
    
    # Upper and Lower bands
    upper_band = hl2 + (multiplier * atr)
    lower_band = hl2 - (multiplier * atr)
    
    # Supertrend calculation
    supertrend = pd.Series(index=close.index, dtype=float)
    trend = pd.Series(index=close.index, dtype=int)
    
    for i in range(len(close)):
        if i == 0:
            supertrend.iloc[i] = upper_band.iloc[i]
            trend.iloc[i] = 1
        else:
            # Upper band calculation
            if upper_band.iloc[i] < upper_band.iloc[i-1] or close.iloc[i-1] > upper_band.iloc[i-1]:
                final_upper = upper_band.iloc[i]
            else:
                final_upper = upper_band.iloc[i-1]
            
            # Lower band calculation  
            if lower_band.iloc[i] > lower_band.iloc[i-1] or close.iloc[i-1] < lower_band.iloc[i-1]:
                final_lower = lower_band.iloc[i]
            else:
                final_lower = lower_band.iloc[i-1]
            
            # Trend determination
            if trend.iloc[i-1] == 1 and close.iloc[i] <= final_lower:
                trend.iloc[i] = -1
            elif trend.iloc[i-1] == -1 and close.iloc[i] >= final_upper:
                trend.iloc[i] = 1
            else:
                trend.iloc[i] = trend.iloc[i-1]
            
            # Supertrend value
            if trend.iloc[i] == 1:
                supertrend.iloc[i] = final_lower
            else:
                supertrend.iloc[i] = final_upper
    
    return supertrend


def calculate_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """
    Calculate On Balance Volume (OBV).
    
    Args:
        close (pd.Series): Close prices
        volume (pd.Series): Trading volume
    
    Returns:
        pd.Series: OBV values
    """
    price_change = close.diff()
    obv_values = []
    obv = 0
    
    for i, change in enumerate(price_change):
        if pd.isna(change):
            obv_values.append(obv)
        elif change > 0:
            obv += volume.iloc[i]
            obv_values.append(obv)
        elif change < 0:
            obv -= volume.iloc[i]
            obv_values.append(obv)
        else:
            obv_values.append(obv)
    
    return pd.Series(obv_values, index=close.index)


def calculate_ad_line(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series) -> pd.Series:
    """
    Calculate Accumulation/Distribution Line (AD).
    
    Args:
        high (pd.Series): High prices
        low (pd.Series): Low prices
        close (pd.Series): Close prices
        volume (pd.Series): Trading volume
    
    Returns:
        pd.Series: AD Line values
    """
    # Money Flow Multiplier
    mfm = ((close - low) - (high - close)) / (high - low)
    mfm = mfm.fillna(0)  # Handle division by zero when high == low
    
    # Money Flow Volume
    mfv = mfm * volume
    
    # Accumulation/Distribution Line (cumulative sum of MFV)
    ad_line = mfv.cumsum()
    
    return ad_line


def calculate_volume_surge(volume: pd.Series, period: int = 20) -> pd.Series:
    """
    Calculate Volume Surge indicator (volume ratio to moving average).
    
    Args:
        volume (pd.Series): Trading volume
        period (int): Period for volume moving average (default: 20)
    
    Returns:
        pd.Series: Volume surge ratio
    """
    volume_ma = volume.rolling(window=period).mean()
    volume_surge = volume / volume_ma
    
    return volume_surge


def calculate_all_indicators(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """
    Calculate all technical indicators for a stock price DataFrame.
    
    Args:
        df (pd.DataFrame): Stock price data with columns:
            - Date (index), Open, High, Low, Close, Volume
    
    Returns:
        Dict[str, pd.Series]: Dictionary with all calculated indicators
    """
    indicators = {}
    
    # Exponential Moving Averages
    indicators['ema_10'] = calculate_ema(df['Close'], 10)
    indicators['ema_20'] = calculate_ema(df['Close'], 20)
    indicators['ema_50'] = calculate_ema(df['Close'], 50)
    indicators['ema_100'] = calculate_ema(df['Close'], 100)
    indicators['ema_200'] = calculate_ema(df['Close'], 200)
    
    # RSI
    indicators['rsi'] = calculate_rsi(df['Close'], 14)
    
    # ATR
    indicators['atr'] = calculate_atr(df['High'], df['Low'], df['Close'], 14)
    
    # Supertrend
    indicators['supertrend'] = calculate_supertrend(df['High'], df['Low'], df['Close'], 10, 3.0)
    
    # OBV
    indicators['obv'] = calculate_obv(df['Close'], df['Volume'])
    
    # AD Line
    indicators['ad'] = calculate_ad_line(df['High'], df['Low'], df['Close'], df['Volume'])
    
    # Volume Surge
    indicators['volume_surge'] = calculate_volume_surge(df['Volume'], 20)
    
    return indicators


def create_indicators_dataframe(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Create a complete indicators DataFrame ready for database insertion.
    
    Args:
        df (pd.DataFrame): Stock price data with Date index
        symbol (str): Stock symbol (e.g., 'RELIANCE')
    
    Returns:
        pd.DataFrame: Indicators DataFrame with ticker and date columns
    """
    # Calculate all indicators
    indicators = calculate_all_indicators(df)
    
    # Create indicators DataFrame
    indicators_df = pd.DataFrame(indicators, index=df.index)
    
    # Add ticker and date columns for database
    clean_symbol = symbol.replace('.NS', '').replace('.BO', '')
    indicators_df['ticker'] = clean_symbol
    indicators_df['date'] = indicators_df.index
    
    # Reorder columns to match database schema
    columns_order = [
        'ticker', 'date', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
        'rsi', 'atr', 'supertrend', 'obv', 'ad', 'volume_surge'
    ]
    
    indicators_df = indicators_df[columns_order]
    
    # Reset index to make date a regular column
    indicators_df = indicators_df.reset_index(drop=True)
    
    return indicators_df


if __name__ == "__main__":
    # Test the indicators with sample data
    import yfinance as yf
    
    print("Testing Technical Indicators with RELIANCE.NS...")
    
    # Fetch sample data
    ticker = yf.Ticker("RELIANCE.NS")
    data = ticker.history(period="6mo")
    
    # Calculate indicators
    indicators = calculate_all_indicators(data)
    
    # Display latest values
    print(f"\nLatest Indicator Values ({data.index[-1].strftime('%Y-%m-%d')}):")
    for name, series in indicators.items():
        latest_value = series.iloc[-1] if not pd.isna(series.iloc[-1]) else "N/A"
        print(f"{name.upper()}: {latest_value}")
    
    # Create complete indicators DataFrame
    indicators_df = create_indicators_dataframe(data, "RELIANCE.NS")
    print(f"\nIndicators DataFrame shape: {indicators_df.shape}")
    print("\nFirst 5 rows:")
    print(indicators_df.head())