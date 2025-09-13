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

class ConfigData:
    """
    Container class for application configuration data.
    
    This class holds configuration parameters loaded from the JSON config file.
    Currently stores the data file path, but can be extended for additional
    configuration options like database settings, API keys, etc.
    
    Attributes:
        data_file_path (str): Directory path where CSV data files are stored
    """
    def __init__(self, data_file_path: str):
        """
        Initialize ConfigData with the data file path.
        
        Args:
            data_file_path (str): Path to directory for storing CSV files
        """
        self.data_file_path = data_file_path

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
    
    # Extract data file path with empty string as default
    data_file_path = config_json.get("data_file_path", "")
    
    # Create and return ConfigData object with parsed values
    return ConfigData(data_file_path)