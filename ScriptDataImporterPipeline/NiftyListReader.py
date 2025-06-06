import csv
import os

def read_nifty_symbols(csv_file):
    """
    Reads the given CSV file and returns a list of symbols with '.NS' suffix.
    Prints each symbol and the total count.
    """
    symbols = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row["Symbol"].strip() + ".NS"
            symbols.append(symbol)
            print(symbol)
    return symbols

if __name__ == "__main__":
  symbols = read_nifty_symbols(os.path.join(os.path.dirname(__file__), "niftytotalmarket_list.csv"))
  print(f"Total symbols: {len(symbols)}")
  print(symbols[:10])  # Print first 10 symbols for verification