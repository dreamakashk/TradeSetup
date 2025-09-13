# TradeSetup - Stock Market Data Pipeline

A comprehensive Python-based stock market data pipeline for Indian stock exchanges (NSE/BSE) that fetches historical stock data, manages incremental updates, and calculates technical indicators with dual storage support (CSV + PostgreSQL/TimescaleDB).

## Features

- **Stock Data Pipeline**: Fetch and store historical stock data from yfinance
- **Technical Indicators**: Calculate 7 key technical indicators (EMA, RSI, ATR, Supertrend, OBV, AD, Volume Surge)
- **Incremental Processing**: Smart incremental updates with mathematical accuracy
- **Dual Storage**: CSV files with optional PostgreSQL/TimescaleDB database
- **Automated Cron Jobs**: Easy setup for daily automated data collection
- **Unified Interface**: Single command for all pipeline operations

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd TradeSetup

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Edit `configs/config.json` to configure your setup:

```json
{
    "data_file_path": "./data",
    "source_file": "niftytotalmarket_list.csv",
    "start_date": "2020-01-01",
    "database": {
        "enabled": false,
        "update_enabled": true,
        "logging_enabled": true
    }
}
```

### 3. Basic Usage

```bash
# Check pipeline status
python pipeline.py status

# Fetch data for single stock
python pipeline.py stock single --symbol RELIANCE.NS

# Update technical indicators
python pipeline.py indicators single --symbol RELIANCE.NS

# Show configuration
python pipeline.py config show
```

## Pipeline Commands

### Stock Data Operations

```bash
# Single stock data fetch
python pipeline.py stock single --symbol RELIANCE.NS

# Sync all Nifty stocks (full download)
python pipeline.py stock sync-all

# Incremental update (recommended for automation)
python pipeline.py stock cron-update

# Sync specific symbol with full history
python pipeline.py stock sync-symbol --symbol RELIANCE.NS
```

### Technical Indicators Operations

```bash
# Update indicators for all stocks (incremental)
python pipeline.py indicators update-all

# Calculate indicators for single stock
python pipeline.py indicators single --symbol RELIANCE.NS

# Recalculate all indicators from scratch
python pipeline.py indicators recalculate-all

# Update indicators from specific date
python pipeline.py indicators update-all --from-date 2025-01-01
```

### Configuration & Status

```bash
# Show current configuration
python pipeline.py config show

# Check pipeline status and data counts
python pipeline.py status
```

## Cron Job Setup

### Automated Daily Updates

The pipeline is designed for easy cron job automation. Here are the recommended setups:

#### Option 1: Simple Daily Updates (Recommended)

```bash
# Edit crontab
crontab -e

# Add these lines for daily updates after market hours (7:00 PM IST)
# Stock data update at 7:00 PM Monday-Friday
0 19 * * 1-5 cd /path/to/TradeSetup && python pipeline.py stock cron-update >> logs/stock_pipeline.log 2>&1

# Technical indicators update at 7:30 PM Monday-Friday  
30 19 * * 1-5 cd /path/to/TradeSetup && python pipeline.py indicators update-all >> logs/indicators_pipeline.log 2>&1
```

#### Option 2: Comprehensive Daily Automation

```bash
# Create logs directory
mkdir -p /path/to/TradeSetup/logs

# Edit crontab
crontab -e

# Add comprehensive automation with error handling
# Daily stock data update
0 19 * * 1-5 cd /path/to/TradeSetup && python pipeline.py stock cron-update >> logs/stock_$(date +\%Y\%m\%d).log 2>&1

# Daily indicators update (30 minutes after stock data)
30 19 * * 1-5 cd /path/to/TradeSetup && python pipeline.py indicators update-all >> logs/indicators_$(date +\%Y\%m\%d).log 2>&1

# Weekly status check (Sunday 8 PM)
0 20 * * 0 cd /path/to/TradeSetup && python pipeline.py status >> logs/status_$(date +\%Y\%m\%d).log 2>&1

# Monthly full recalculation (1st of month, 9 PM)
0 21 1 * * cd /path/to/TradeSetup && python pipeline.py indicators recalculate-all >> logs/monthly_recalc_$(date +\%Y\%m\%d).log 2>&1
```

#### Option 3: Custom Scheduling

```bash
# Custom schedules for different needs

# Every 2 hours during market hours (9 AM to 5 PM, Mon-Fri)
0 9-17/2 * * 1-5 cd /path/to/TradeSetup && python pipeline.py stock cron-update

# End of day comprehensive update (6 PM)
0 18 * * 1-5 cd /path/to/TradeSetup && python pipeline.py stock cron-update && python pipeline.py indicators update-all

# Weekend full sync (Saturday 10 PM)
0 22 * * 6 cd /path/to/TradeSetup && python pipeline.py stock sync-all && python pipeline.py indicators recalculate-all
```

### Cron Job Best Practices

1. **Timing**: Schedule after market hours (6 PM IST or later)
2. **Sequencing**: Run stock data first, then indicators (30-60 minutes apart)
3. **Logging**: Always redirect output to log files for debugging
4. **Error Handling**: Use `&&` to chain commands only on success
5. **Monitoring**: Set up weekly status checks

### Monitoring Cron Jobs

```bash
# Check cron job logs
tail -f logs/stock_pipeline.log
tail -f logs/indicators_pipeline.log

# Monitor pipeline status
python pipeline.py status

# Check last run results
grep -E "(Successfully|Error)" logs/stock_$(date +%Y%m%d).log
```

## Advanced Configuration

### Stock Filtering

Configure specific stocks or sectors in `config.json`:

```json
{
    "stock_filter": {
        "enabled": true,
        "symbols": ["RELIANCE", "TCS", "INFY"],
        "exclude_symbols": ["SMALLCAP1", "SMALLCAP2"],
        "sector_filter": ["Technology", "Finance"]
    }
}
```

### Date Range Override

Set custom date ranges:

```json
{
    "date_range": {
        "enabled": true,
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "use_relative_dates": false,
        "days_back": 365
    }
}
```

### Pipeline Performance Settings

Optimize for your system:

```json
{
    "pipeline_settings": {
        "batch_size": 50,
        "max_retries": 3,
        "retry_delay": 5,
        "parallel_processing": false,
        "max_workers": 4
    }
}
```

## Database Setup (Optional)

### PostgreSQL with TimescaleDB

1. **Install PostgreSQL and TimescaleDB**:
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo apt install timescaledb-postgresql

# Enable TimescaleDB
sudo -u postgres psql -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
```

2. **Create Database**:
```bash
sudo -u postgres createdb tradesetup
```

3. **Update Configuration**:
```json
{
    "database": {
        "enabled": true,
        "host": "localhost",
        "port": 5432,
        "database": "tradesetup",
        "user": "postgres",
        "password": "your_password"
    }
}
```

4. **Initialize Schema**:
```bash
python pipeline.py status  # This will auto-create schema if needed
```

## Technical Indicators

The pipeline calculates these technical indicators:

| Indicator | Period | Description |
|-----------|---------|-------------|
| EMA-10 | 10 days | Short-term trend |
| EMA-20 | 20 days | Medium-term trend |
| EMA-50 | 50 days | Long-term trend |
| EMA-100 | 100 days | Very long-term trend |
| EMA-200 | 200 days | Major trend |
| RSI | 14 days | Relative Strength Index (0-100) |
| ATR | 14 days | Average True Range (volatility) |
| Supertrend | 10, 3.0 | Trend following indicator |
| OBV | - | On Balance Volume |
| AD | - | Accumulation/Distribution Line |
| Volume Surge | 20 days | Volume vs average ratio |

## Legacy Commands (Still Supported)

For backwards compatibility, the original commands still work:

```bash
# Stock data operations
python main.py --mode single --symbol RELIANCE.NS
python main.py --mode sync-all
python main.py --mode cron-update

# Technical indicators operations
python indicators_pipeline.py --mode single --symbol RELIANCE.NS
python indicators_pipeline.py --mode update-all
python indicators_pipeline.py --mode recalculate-all
```

## File Structure

```
TradeSetup/
├── pipeline.py                 # Unified entry point
├── main.py                     # Legacy stock pipeline entry
├── indicators_pipeline.py      # Legacy indicators entry
├── configs/
│   ├── config.json            # Main configuration
│   └── db_Schema.sql          # Database schema
├── sources/
│   └── niftytotalmarket_list.csv  # Stock symbols
├── data/                      # CSV output directory
├── logs/                      # Log files (create manually)
└── ScriptDataImporterPipeline/
    ├── DatabaseManager.py     # Connection management
    ├── TechnicalIndicators.py # Indicators calculation
    ├── IndicatorsWriter.py    # Database operations
    ├── ScriptDataFetcher.py   # Stock data fetching
    ├── FileHandler.py         # CSV operations
    ├── PostgresWriter.py      # Database operations
    └── ConfigReader.py        # Configuration handling
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**:
   ```bash
   # Check database status
   python pipeline.py status
   
   # Disable database if needed
   # Set "enabled": false in config.json
   ```

2. **CSV File Permissions**:
   ```bash
   # Fix permissions
   chmod 755 data/
   chmod 644 data/*.csv
   ```

3. **Network/yfinance Issues**:
   ```bash
   # Test single symbol
   python pipeline.py stock single --symbol RELIANCE.NS
   
   # Check logs for specific errors
   tail -f logs/stock_pipeline.log
   ```

4. **Cron Job Not Running**:
   ```bash
   # Check cron service
   sudo systemctl status cron
   
   # Check cron logs
   grep CRON /var/log/syslog
   
   # Test command manually
   cd /path/to/TradeSetup && python pipeline.py stock cron-update
   ```

### Performance Optimization

- **For large datasets**: Enable database storage for better performance
- **For network issues**: Increase retry settings in config
- **For memory issues**: Reduce batch_size in pipeline_settings
- **For slow indicators**: Use incremental mode instead of full recalculation

## Stock Symbol Format

Use NSE format with `.NS` suffix:
- `RELIANCE.NS` - Reliance Industries
- `TCS.NS` - Tata Consultancy Services  
- `INFY.NS` - Infosys
- `HDFC.NS` - HDFC Bank

## Key Notes

- Nifty full market list is available at https://niftyindices.com/IndexConstituent/ind_niftytotalmarket_list.csv
- To make yFinance library working smoothly: `pip install yfinance --upgrade --no-cache-dir`
- Always schedule cron jobs after market hours (6 PM IST or later)
- Use incremental updates for daily automation (faster and more efficient)

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions:
- Create an issue on GitHub
- Check the troubleshooting section above
- Review log files for specific error messages