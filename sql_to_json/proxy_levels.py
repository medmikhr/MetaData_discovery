import json

from sql_to_json.load_convert import output_file
from DB_configs.config import load_config, SEAF_PREFIX


mapping = {
    'logical_entities': ['physical_tables', 'data_objects'],
    'business_terms': ['logical_entities', 'business_objects']
}


def proxy_level(filename = 'physical_tables', entity_type='logical_entities'):
    with open('Output_JSON/' + filename + '.json', 'r') as file:
        data = json.load(file)
    file.close()
    record_map = load_config('sql_to_json/entities_fields_mapping.ini', entity_type)
    entities_json = {}
    filepath = SEAF_PREFIX + filename
    for item in data[filepath].items():
        data[filepath][item[0]][mapping[entity_type][1]] = item[0]
        vals = item[1]
        record = {}
        for k, v in record_map.items():
            record[k] = vals[v]
        entities_json[item[0]] = record
    output_file(filename, data[filepath])
    return entities_json


def create_upper_level (entity_type='logical_entities'):
    json_dump = proxy_level(mapping[entity_type][0], entity_type)
    output_file(entity_type, json_dump)

#
# def create_le_json(filename = 'physical_tables'):
#     with open('Output_JSON/' + filename + '.json', 'r') as file:
#         data = json.load(file)
#     file.close()
#     entities_json = {}
#     for item in data[filename].items():
#         data[filename][item[0]]['data_objects'] = item[0]
#         vals = item[1]
#         record = {
#             'title': vals['title'],
#             'description': vals['description'],
#             'master_system': vals['system'],
#         }
#         entities_json[item[0]] = record
#     output_file(filename, data[filename])
#     return entities_json
#
#
# def create_bt_json(filename = 'logical_entities'):
#     with open('Output_JSON/' + filename + '.json', 'r') as file:
#         data = json.load(file)
#     file.close()
#     entities_json = {}
#     for item in data[filename].items():
#         data[filename][item[0]]['business_objects'] = item[0]
#         vals = item[1]
#         record = {
#             'title': vals['title'],
#             'description': vals['description'],
#         }
#         entities_json[item[0]] = record
#     output_file(filename, data[filename])
#     return entities_json
