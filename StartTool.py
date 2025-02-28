from DB_configs.config import load_config
from sql_to_json.load_convert import connect, fetch_data, read_sql_query, json_dump, physical_tables


if __name__ == '__main__':
    connection = connect(load_config(section='psql_flights'))
    cursor = connection.cursor()

    sql_output = fetch_data(cursor, read_sql_query('physical_tables'))
    json_dump('physical_tables', physical_tables(sql_output))

    connection.close()