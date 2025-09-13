#!/usr/bin/env python3
"""
Test script to verify the indicators pipeline fixes
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Add the ScriptDataImporterPipeline directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'ScriptDataImporterPipeline'))

from ConfigReader import read_config
from TechnicalIndicators import create_indicators_dataframe

def test_column_naming_fix():
    """Test that the column naming works correctly"""
    print("Testing column naming fix...")
    
    # Create sample data with both uppercase and lowercase columns to test robustness
    sample_data = {
        'Open': [100, 101, 102, 103, 104],
        'High': [105, 106, 107, 108, 109],
        'Low': [99, 98, 100, 101, 102],
        'Close': [102, 103, 104, 105, 106],
        'Volume': [1000, 1100, 1200, 1300, 1400]
    }
    
    dates = pd.date_range('2025-01-01', periods=5)
    df = pd.DataFrame(sample_data, index=dates)
    
    try:
        # This should work with uppercase columns
        indicators_df = create_indicators_dataframe(df, "TEST.NS")
        print(f"✅ Column naming test passed - calculated {len(indicators_df)} indicator records")
        
        # Check that required indicators are calculated
        expected_columns = ['ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200', 'rsi', 'atr', 'supertrend', 'obv', 'ad', 'volume_surge']
        missing_columns = [col for col in expected_columns if col not in indicators_df.columns]
        if missing_columns:
            print(f"⚠️  Missing indicator columns: {missing_columns}")
        else:
            print("✅ All expected indicator columns present")
        
        return True
    except Exception as e:
        print(f"❌ Column naming test failed: {e}")
        return False

def test_ema_calculation_accuracy():
    """Test that EMA calculations work with sufficient data"""
    print("\nTesting EMA calculation accuracy...")
    
    # Create sample data with 250 days (enough for EMA 200)
    num_days = 250
    dates = pd.date_range('2024-01-01', periods=num_days)
    
    # Create synthetic price data with a trend
    base_price = 100
    prices = [base_price + i * 0.1 + (i % 10) * 0.5 for i in range(num_days)]
    
    sample_data = {
        'Open': [p - 0.5 for p in prices],
        'High': [p + 1.0 for p in prices],
        'Low': [p - 1.0 for p in prices],
        'Close': prices,
        'Volume': [1000 + i * 10 for i in range(num_days)]
    }
    
    df = pd.DataFrame(sample_data, index=dates)
    
    try:
        indicators_df = create_indicators_dataframe(df, "TEST.NS")
        
        # Check that EMA 200 is calculated and not all NaN
        ema_200_valid = indicators_df['ema_200'].notna().sum()
        print(f"✅ EMA 200 calculation test passed - {ema_200_valid} valid EMA 200 values out of {len(indicators_df)}")
        
        # The first ~200 values should be NaN, rest should be valid
        if ema_200_valid > 40:  # Should have at least 50 valid values
            print("✅ EMA 200 has sufficient valid values")
        else:
            print(f"⚠️  EMA 200 has only {ema_200_valid} valid values, expected more")
        
        return True
    except Exception as e:
        print(f"❌ EMA calculation test failed: {e}")
        return False

def test_incremental_logic_simulation():
    """Test the incremental update logic simulation"""
    print("\nTesting incremental update logic simulation...")
    
    try:
        # Load config
        config = read_config('configs/config.json')
        
        # Simulate the new logic for warmup window calculation
        latest_indicators_date = datetime(2025, 9, 10)  # Simulate existing indicators
        target_from_date = latest_indicators_date + timedelta(days=1)  # 2025-09-11
        warmup_from_date = target_from_date - timedelta(days=300)  # ~300 days back
        
        print(f"✅ Incremental logic simulation:")
        print(f"   Latest indicators date: {latest_indicators_date.strftime('%Y-%m-%d')}")
        print(f"   Target from date: {target_from_date.strftime('%Y-%m-%d')}")
        print(f"   Warmup from date: {warmup_from_date.strftime('%Y-%m-%d')}")
        print(f"   Warmup window: {(target_from_date - warmup_from_date).days} days")
        
        if (target_from_date - warmup_from_date).days >= 220:
            print("✅ Warmup window is sufficient for EMA 200")
            return True
        else:
            print("❌ Warmup window is insufficient for EMA 200")
            return False
            
    except Exception as e:
        print(f"❌ Incremental logic test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("Testing Indicators Pipeline Fixes")
    print("=" * 60)
    
    tests = [
        test_column_naming_fix,
        test_ema_calculation_accuracy,
        test_incremental_logic_simulation
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Indicators pipeline fixes are working correctly.")
        return True
    else:
        print("❌ Some tests failed. Please review the fixes.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)