from json import loads, dump
import psycopg2
from configparser import ConfigParser
import pandas as pd

SEAF_PREFIX = 'seaf.ia.'

def get_sections(filename='../DB_configs/database.ini'):
    config = ConfigParser()
    config.read(filename)
    print(config.items('psql_dvd'))
    sections = config.sections()
    return sections



def load_config(filename='../DB_configs/database.ini', section='psql_dvd'):
    parser = ConfigParser()
    parser.read(filename)
    configdict = {}
    params = parser.items(section)
    for param in params:
        configdict[param[0]] = param[1]
    return configdict

def db_connect(config):
    try:
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgresSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def read_sql_query(sql_file = 'physical_tables'):
    with open('../SQL_scripts/' + sql_file + '.sql', 'r') as file:
        query = file.read()
    file.close()
    return query

def fetch_query_data(con_cur, query_str, entity = 'physical_tables'):
    con_cur.execute(query_str)
    data = con_cur.fetchall()
    columns = [t[0] for t in cursor.description]
    output_df = pd.DataFrame(data, columns=columns)
    output_df.to_csv('../SQL_output/'+entity + '.csv', index=False)
    return output_df

def format_df(df, system, entity = 'physical_tables'):
    df['system'] = system
    df['id'] = ['_'.join([row.system, row.database, row.schema_name, row.title ]) for row in df.itertuples()]
    df = df.set_index('id')
    df.to_csv('../SQL_output/' + entity + '.csv')
    return df

def output_file(entities_json, entity = 'physical_tables'):
    fullname = '../Output_JSON/' + entity + '.json'
    json_dict = {SEAF_PREFIX + entity:entities_json}
    with open(fullname, "w", encoding="UTF-8") as file:
        dump(json_dict, file, ensure_ascii=False, indent=2)
    file.close()


if __name__ == '__main__':
    dbs_list = get_sections()
    sql_query = read_sql_query(sql_file='physical_tables')


    db_creds = load_config(section='psql_dvd')
    as_system = db_creds.pop('as')
    connection = db_connect(db_creds)
    cursor = connection.cursor()
    df_output = fetch_query_data(cursor, sql_query)
    df_json = format_df(df_output, as_system)
    result = df_json.to_json(orient="index")
    print(result)
    parsed = loads(result)
    print(parsed)
    output_file(parsed)
    connection.close()
