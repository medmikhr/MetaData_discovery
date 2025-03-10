import pandas as pd

from DB_configs.config import load_config
from sql_to_json.settings import PROXY_MAPPING, FOLDER_PATH as FP


def proxy_level(entity='logical_entities'):
    filename = PROXY_MAPPING[entity][0]
    df = pd.read_csv(FP['csv'] + filename + '.csv')
    record_map = load_config('sql_to_json/entities_fields_mapping.ini', entity)
    df = df.set_index('id')
    df[PROXY_MAPPING[entity][1]] = df.index
    df.to_csv(FP['csv'] + filename + '.csv')
    df_e = df[record_map.keys()]
    df_e = df_e.rename(columns=record_map)
    return df_e


def create_upper_level (entity='logical_entities'):
    df = proxy_level(entity)
    df.to_csv(FP['csv'] + entity + '.csv')
    return df
