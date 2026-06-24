import json
from datetime import date, timedelta
import yaml
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

def load_yaml_file():
    with open(f"{PROJECT_ROOT}/Params_map_conf.yaml","r") as f:
        config_file = yaml.safe_load(f)
    return config_file

def resolve_values(value,config_file):
    if value not in list(config_file['param_map'].keys()):
        return value
    config = config_file['param_map'][value]
    today = date.today()
    if config['type']=='dynamic':
        return (today - timedelta(days = config["offset_days"])).strftime("%Y-%m-%d")
    elif config['type']=='month_start':
        return today.replace(day=1).strftime("%Y-%m-%d")
    elif config['type']=='year_start':
        return today.replace(month=1,day=1).strftime("%Y-%m-%d")
    return value

def resolve_params(requested_params_str):
    requested_params=json.loads(requested_params_str)
    config_file = load_yaml_file()
    resolved_params = {}
    for key,value in requested_params.items():
        resolved_params[key]=resolve_values(value,config_file)
    return resolved_params

def resolve_linked_params(src):
    config_file = load_yaml_file()
    return config_file['linked_events_map'][src]
# file = load_yaml_file()
# print(list(file['param_map'].keys()))
