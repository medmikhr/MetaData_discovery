import os
# from pathlib import Path
from json import loads
import pandas as pd

from DB_configs.config import get_sections, db_connect, load_config
from sql_to_json.get_from_db import create_ph_level, output_file
from sql_to_json.proxy_levels import create_upper_level



if __name__ == '__main__':
    dir_path = os.path.join(os.getcwd(), 'PD_dataframes')

    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        open(file_path, 'w').close()

    for section in get_sections():
        db_creds = load_config(section = section)
        as_system = db_creds.pop('as')
        connection = db_connect(db_creds).cursor()
        cursor = connection

        create_ph_level(cursor, 'physical_tables', as_system)
        create_ph_level(cursor, 'physical_attributes', as_system)

        connection.close()

    for filename in os.listdir(dir_path):
        df_json = pd.read_csv(os.path.join(dir_path, filename))
        df_json = df_json.set_index('id')
        # output_file(loads(df_json.to_json(orient="index")), Path(filename).stem)
        output_file(loads(df_json.to_json(orient="index")), os.path.splitext(filename)[0])


    create_upper_level('logical_entities')
    create_upper_level('business_terms')
