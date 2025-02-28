import json
import psycopg2

def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgresSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def read_sql_query(sql_file):
    with open('SQL_scripts/' + sql_file + '.sql', 'r') as file:
        query = file.read()
    file.close()
    return query

def fetch_data(cursor, querystr):
    cursor.execute(querystr)
    data = cursor.fetchall()
    return data

def json_dump(filename, entities_json):
    fullname = 'Output_JSON/' + filename + '.json'
    json_dump = {filename:entities_json}
    with open(fullname, "w", encoding="UTF-8") as file:
        json.dump(json_dump, file, ensure_ascii=False, indent=2)
    file.close()

def physical_tables(sql_output):
    entities_json = {}
    for row in sql_output:
        record = {
            'name': row[2],
            'description': row[2],
            'as': 'AS1',
            'rdb': 'postgresql',
            'db': row[0],
            'schema': row[1],
            'logic_link': row[2]
        }
        entities_json[row[2]] = record
    return entities_json
