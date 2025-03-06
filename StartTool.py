from DB_configs.config import load_config
from sql_to_json.load_convert import connect, create_fmd_level
from sql_to_json.proxy_levels import create_upper_level


if __name__ == '__main__':
    connection = connect(load_config(section='psql_flights'))
    cursor = connection.cursor()

    create_fmd_level(cursor, 'physical_tables')
    # create_fmd_level(cursor, 'physical_attributes')

    connection.close()

    # create_upper_level('logical_entities')
    # create_upper_level('business_terms')
