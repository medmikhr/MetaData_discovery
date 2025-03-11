from DB_configs.config import get_sections, db_connect, load_config
from sql_to_json.get_from_db import create_ph_level
from sql_to_json.proxy_levels import create_upper_level
from sql_to_json.file_operations import clear_dfs, create_json_files
from sql_to_json.settings import FOLDER_PATH as FP, BASE_DIR


if __name__ == '__main__':
    dir_path = BASE_DIR / FP['csv']

    clear_dfs(dir_path)

    for section in get_sections():
        db_creds = load_config(section = section)
        as_system = db_creds.pop('as')
        connection = db_connect(db_creds).cursor()
        cursor = connection
        create_ph_level(cursor, 'physical_tables', as_system)
        create_ph_level(cursor, 'physical_attributes', as_system)
        connection.close()

    create_upper_level('logical_entities')
    create_upper_level('business_terms')

    create_json_files(dir_path)
