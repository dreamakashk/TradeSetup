import os
import ScriptDataFetcher # import fetch_historical_data, save_data_to_csv
from ConfigReader import read_config

if __name__ == "__main__":
    symbol = "BSE.NS"  # Change to your desired stock symbol
    info = ScriptDataFetcher.fetch_company_info(symbol)  # Fetch company info for Reliance Industries
    print(info)

    # # config_path = os.path.join(os.path.dirname(__file__), "config.json")
    # # config = read_config(config_path)
    # # print(config.data_file_path)


    
    data = ScriptDataFetcher.fetch_historical_data(symbol)
    print(data)
    # save_data_to_csv(data, symbol, config.data_file_path)  # Save to the path specified in config
    ScriptDataFetcher.save_data_to_csv(data, symbol, r"C:\Users\akashkatare\Src\TradeSetup\data")  # Updated directory path