# TradeSetup - Stock Market Data Pipeline

## Overview
This is a Python-based stock market data pipeline that fetches, processes, and stores stock market data from Indian stock exchanges using the yfinance library. The project focuses on NSE (National Stock Exchange) listed stocks and Nifty indices.

## Recent Changes
- **2025-09-13**: Project imported and configured for Replit environment
  - Updated all file paths to use relative paths instead of Windows-specific paths
  - Created main.py as the primary entry point with CLI interface
  - Added requirements.txt for proper dependency management
  - Configured workflow for console-based execution
  - Added proper error handling for database imports (optional PostgreSQL support)
  - Fixed LSP issues and import resolution

## Project Architecture
- **main.py**: Primary entry point with CLI interface supporting multiple modes
- **ScriptDataImporterPipeline/**: Core pipeline modules
  - **ScriptDataFetcher.py**: Handles yfinance API calls for stock data
  - **NiftyScriptsDataSyncer.py**: Bulk operations for Nifty stock lists
  - **FileHandler.py**: CSV file operations and data persistence
  - **PostgresWriter.py**: Database operations (optional, requires PostgreSQL setup)
  - **ConfigReader.py**: Configuration file handling
- **configs/**: Configuration files including data directory paths
- **sources/**: Reference data including Nifty stock symbol lists
- **data/**: Output directory for CSV files (created automatically)

## User Preferences
- Prefer console-based operation for data pipeline tasks
- Use CSV files for primary data storage with optional database backup
- Support batch operations for efficiency
- Maintain compatibility with Indian stock market symbols (.NS suffix)

## Current State
- ✅ Core pipeline functional and tested
- ✅ CLI interface working with multiple modes (single, sync-all, sync-symbol)
- ✅ CSV data persistence working
- ✅ Workflow configured for Replit environment
- ⚠️ PostgreSQL database setup pending (optional feature)
- ✅ Error handling improved for optional database features

## Architecture Decisions
- **2025-09-13**: Chose to make database operations optional to allow CSV-only operation
- **2025-09-13**: Implemented lazy imports for database modules to prevent import errors
- **2025-09-13**: Used requirements.txt instead of maintaining vendored Python environment
- **2025-09-13**: Configured as console application rather than web service (appropriate for data pipeline)