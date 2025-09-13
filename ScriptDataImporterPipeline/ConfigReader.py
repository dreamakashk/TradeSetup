"""
ConfigReader - Configuration Management Module

This module handles reading and parsing configuration files for the TradeSetup
application. It provides a simple interface to load JSON configuration files
and access configuration parameters.

Classes:
    ConfigData: Container for configuration parameters

Functions:
    read_config: Load configuration from JSON file

Author: TradeSetup Team
Created: 2025-09-13
"""

import json  # For parsing JSON configuration files
from typing import Optional  # For type hints with optional values

class ConfigData:
    """
    Container class for application configuration data.
    
    This class holds configuration parameters loaded from the JSON config file.
    Stores data file path, source file, start date, and database configuration settings.
    
    Attributes:
        data_file_path (str): Directory path where CSV data files are stored
        source_file (str): Source file name for stock symbols list
        start_date (str): Start date for historical data fetching (YYYY-MM-DD)
        db_enabled (bool): Whether database operations are enabled
        db_update_enabled (bool): Whether database updates are enabled
        db_logging_enabled (bool): Whether database logging is enabled
        db_config (dict): Database connection configuration
    """
    def __init__(self, data_file_path: str, source_file: str = "niftytotalmarket_list.csv", 
                 start_date: str = "2020-01-01", db_config: Optional[dict] = None):
        """
        Initialize ConfigData with file path, source file, start date, and database configuration.
        
        Args:
            data_file_path (str): Path to directory for storing CSV files
            source_file (str): Source file name for stock symbols list
            start_date (str): Start date for historical data fetching (YYYY-MM-DD)
            db_config (dict): Database configuration parameters
        """
        self.data_file_path = data_file_path
        self.source_file = source_file
        self.start_date = start_date
        self.db_config = db_config or {}
        self.db_enabled = self.db_config.get('enabled', False)
        self.db_update_enabled = self.db_config.get('update_enabled', True)
        self.db_logging_enabled = self.db_config.get('logging_enabled', True)
    
    def get_db_connection_params(self):
        """
        Get database connection parameters, preferring environment variables.
        
        Returns:
            dict: Database connection parameters for psycopg2
        """
        if not self.db_enabled:
            return None
        
        import os
        
        if self.db_config.get('use_env_vars', True):
            # Use Replit database environment variables if available
            return {
                'host': os.environ.get('PGHOST', self.db_config.get('host', 'localhost')),
                'port': int(os.environ.get('PGPORT', self.db_config.get('port', 5432))),
                'database': os.environ.get('PGDATABASE', self.db_config.get('database', 'tradesetup')),
                'user': os.environ.get('PGUSER', self.db_config.get('user', 'postgres')),
                'password': os.environ.get('PGPASSWORD', self.db_config.get('password', ''))
            }
        else:
            # Use configuration file values directly
            return {
                'host': self.db_config.get('host', 'localhost'),
                'port': self.db_config.get('port', 5432),
                'database': self.db_config.get('database', 'tradesetup'),
                'user': self.db_config.get('user', 'postgres'),
                'password': self.db_config.get('password', '')
            }

def read_config(config_path: str) -> ConfigData:
    """
    Read configuration from a JSON file and return a ConfigData object.
    
    Loads configuration parameters from a JSON file and creates a ConfigData
    instance with the parsed values. Provides default values for missing
    configuration keys to ensure robustness.
    
    Args:
        config_path (str): Path to the JSON configuration file
    
    Returns:
        ConfigData: Object containing parsed configuration parameters
    
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        json.JSONDecodeError: If the configuration file contains invalid JSON
    """
    # Open and parse the JSON configuration file
    with open(config_path, 'r') as f:
        config_json = json.load(f)
    
    # Extract data file path with default value
    data_file_path = config_json.get("data_file_path", "./data")
    
    # Extract source file with default value
    source_file = config_json.get("source_file", "niftytotalmarket_list.csv")
    
    # Extract start date with default value
    start_date = config_json.get("start_date", "2020-01-01")
    
    # Extract database configuration
    db_config = config_json.get("database", {})
    
    # Create and return ConfigData object with parsed values
    return ConfigData(data_file_path, source_file, start_date, db_config)