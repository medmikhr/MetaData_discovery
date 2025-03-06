import json
import psycopg2

from DB_configs.config import load_config

SEAF_PREFIX = 'seaf.ia.'

def connect(config):
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

def fetch_query_data(cursor, query_str):
    cursor.execute(query_str)
    data = cursor.fetchall()
    return data

def convert_json(sql_output, entity_type):
    record_map = load_config(filename='sql_to_json/entities_fields_mapping.ini', section=entity_type)
    entities_json = {}
    for row in sql_output:
        record = {}
        for k, v in record_map.items():
            record[k] = row[int(v)]
        e_id = '_'.join([row[int(record_map['database'])], row[int(record_map['title'])], row[int(record_map['title'])]])
        entities_json[e_id] = record
    return entities_json

def create_fmd_level (cursor, entity):
    sql_output = fetch_query_data(cursor, read_sql_query(entity))
    json_dump = convert_json(sql_output, entity)
    output_file(entity, json_dump)


def output_file(filename, entities_json):
    fullname = 'Output_JSON/' + filename + '.json'
    json_dict = {SEAF_PREFIX + filename:entities_json}
    with open(fullname, "w", encoding="UTF-8") as file:
        json.dump(json_dict, file, ensure_ascii=False, indent=2)
    file.close()

def create_pht_json(sql_output):
    entities_json = {}
    for row in sql_output:
        record = {
            'title': row[2],
            'description': row[2],
            'database': row[0],
            'schema_name': row[1],
            'size': row[3],
        }
        e_id = '_'.join([row[0], row[1], row[2]])
        entities_json[e_id] = record
    return entities_json

def create_pha_json(sql_output):
    entities_json = {}
    for row in sql_output:
        record = {
            'title': row[0],
            'description': row[0],
            'type': row[1],
            'pk_flag': row[2],
            'fk_flag': row[3],
            'physical_table': row[4],
        }
        e_id = '_'.join([row[0], row[1], row[2]])
        entities_json[e_id] = record
    return entities_json


def create_pht_level (cursor, entity):
    sql_output = fetch_query_data(cursor, read_sql_query(entity))
    json_dump = create_pht_json(sql_output)
    output_file(entity, json_dump)

