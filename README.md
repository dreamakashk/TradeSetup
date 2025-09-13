# TradeSetup - Stock Market Data Pipeline

A Python-based data pipeline for fetching and processing Indian stock market data using the yfinance library.

## Features

- 📈 Fetch historical stock data for NSE-listed companies
- 📊 Company information retrieval (sector, industry, market cap)
- 🔄 Bulk synchronization of Nifty stock universe
- 💾 CSV-based data storage with optional PostgreSQL support
- 🚀 Command-line interface for flexible operations

## Quick Start

### Fetch Single Stock Data
```bash
python main.py --mode single --symbol RELIANCE.NS
```

### Sync All Nifty Stocks
```bash
python main.py --mode sync-all
```

### Update Specific Symbol
```bash
python main.py --mode sync-symbol --symbol TCS.NS
```

## Available Commands

| Mode | Description | Example |
|------|-------------|---------|
| `single` | Fetch data for one stock symbol | `--mode single --symbol INFY.NS` |
| `sync-all` | Download/update all Nifty stocks | `--mode sync-all` |
| `sync-symbol` | Update existing CSV for one symbol | `--mode sync-symbol --symbol HDFC.NS` |

## Data Storage

- **CSV Files**: Stored in `./data/` directory (created automatically)
- **Format**: Date, Open, High, Low, Close, Volume, Dividends, Stock Splits
- **Database**: Optional PostgreSQL support (requires setup)

## Configuration

Edit `configs/config.json` to change data directory:
```json
{
    "data_file_path": "./data"
}
```

## Stock Symbol Format

Use NSE format with `.NS` suffix:
- `RELIANCE.NS` - Reliance Industries
- `TCS.NS` - Tata Consultancy Services  
- `INFY.NS` - Infosys
- `HDFC.NS` - HDFC Bank

## Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

Key dependencies:
- `yfinance` - Stock data API
- `pandas` - Data manipulation
- `requests` - HTTP requests
- `psycopg2-binary` - PostgreSQL connector (optional)

## Key Notes
Nifty full market list is available at https://niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv 


To make yFinance library working smoothly
- Install !pip install yfinance --upgrade --no-cache-dir