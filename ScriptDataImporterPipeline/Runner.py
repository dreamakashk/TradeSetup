import os
import ScriptDataFetcher 
from ConfigReader import read_config

if __name__ == "__main__":
    symbol = "BSE.NS"  # Change to your desired stock symbol
    info = ScriptDataFetcher.fetch_company_info(symbol)  # Fetch company info for BSE
    print(f"Company Info for {symbol}:")
    print(info)

    # Use config file for data path
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "config.json")
    config = read_config(config_path)
    print(f"Data will be saved to: {config.data_file_path}")

    # Fetch and save historical data
    data = ScriptDataFetcher.fetch_historical_data(symbol)
    print(f"Fetched data shape: {data.shape}")
    print(data.head())
    
    # Save to configured directory
    ScriptDataFetcher.save_data_to_csv(data, symbol, config.data_file_path)