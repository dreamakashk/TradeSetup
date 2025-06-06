import csv
import os


def save_data_to_csv(data, symbol: str, directory: str):
    """
    Save the DataFrame to a CSV file in the specified directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = os.path.join(directory, f"{symbol}.csv")
    data.to_csv(filename)
    print(f"Data saved to {filename}")

## write a function to donwload nifty symbols from a CSV file and check if the corresponding CSV files exist in a given directory.
## from https://niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv 
def read_nifty_symbols(csv_file):
    """
    Reads the given CSV file and returns a list of symbols.
    Prints each symbol and the total count.
    """
    symbols = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row["Symbol"].strip()
            symbols.append(symbol)
    return symbols

def check_symbol_file_exists(symbol, directory):
    """
    Checks if the CSV file for the given symbol exists in the specified directory.
    Returns True if exists, False otherwise.
    """
    file_path = os.path.join(directory, f"{symbol}.csv")
    return os.path.exists(file_path)

# def rename_ns_csv_files_to_csv(data_dir):
#     """
#     Renames all files in the data directory from 'SYMBOL.NS.csv' to 'SYMBOL.csv'.
#     """
#     for filename in os.listdir(data_dir):
#         if filename.endswith(".NS.csv"):
#             old_path = os.path.join(data_dir, filename)
#             new_filename = filename.replace(".NS.csv", ".csv")
#             new_path = os.path.join(data_dir, new_filename)
#             os.rename(old_path, new_path)
#             print(f"Renamed: {old_path} -> {new_path}")

# def delete_ns_csv_files(data_dir):
#     """
#     Deletes all files in the data directory that end with '.NS.csv'.
#     """
#     for filename in os.listdir(data_dir):
#         if filename.endswith(".NS.csv"):
#             file_path = os.path.join(data_dir, filename)
#             os.remove(file_path)
#             print(f"Deleted: {file_path}")


# if __name__ == "__main__":
#     data_dir = r"C:\Users\akashkatare\Src\TradeSetup\data"
#     # rename_ns_csv_files_to_csv(data_dir)
#     delete_ns_csv_files(data_dir)
