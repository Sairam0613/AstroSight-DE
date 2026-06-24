import os
import configparser

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

def get_api_key():
    config = configparser.ConfigParser()
    config_path = os.path.join(PROJECT_ROOT, "Configs.conf")
    config.read(config_path)
    return config.get("NASA","API_KEY")
