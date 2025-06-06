import json

class ConfigData:
    def __init__(self, data_file_path: str):
        self.data_file_path = data_file_path

def read_config(config_path: str) -> ConfigData:
    """
    Reads the config file in JSON format and returns a ConfigData object.
    """
    with open(config_path, 'r') as f:
        config_json = json.load(f)
    data_file_path = config_json.get("data_file_path", "")
    return ConfigData(data_file_path)