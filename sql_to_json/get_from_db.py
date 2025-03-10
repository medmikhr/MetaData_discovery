from json import loads, dump
import pandas as pd
import os

from DB_configs.config import SEAF_PREFIX


def read_sql_query(sql_file = 'physical_tables'):
    with open('SQL_scripts/' + sql_file + '.sql', 'r') as file:
        query = file.read()
    file.close()
    return query


def fetch_query_data(con_cur, query_str, entity = 'physical_tables'):
    con_cur.execute(query_str)
    data = con_cur.fetchall()
    columns = [t[0] for t in con_cur.description]
    output_df = pd.DataFrame(data, columns=columns)
    # output_df.to_csv('PD_dataframes/'+entity + '.csv', index=False)
    return output_df


def write_df(df, system, entity = 'physical_tables'):
    if entity == 'physical_tables': df['system'] = system
    df['id'] = system + '_' + df['id']
    df = df.set_index('id')
    output_path = 'PD_dataframes/' + entity + '.csv'
    # header_switch = os.path.exists(os.path.join(os.getcwd(), output_path))
    # header_switch = (lambda x: True if x == 0 else False)(os.path.getsize(os.path.join(os.getcwd(), output_path)))
    header_switch = True if os.path.getsize(os.path.join(os.getcwd(), output_path)) == 0 else False
    df.to_csv(output_path, mode='a', header=header_switch)
    return df


def output_file(entities_json, entity = 'physical_tables'):
    fullname = 'Output_JSON/' + entity + '.json'
    json_dict = {SEAF_PREFIX + entity:entities_json}
    with open(fullname, "w", encoding="UTF-8") as file:
        dump(json_dict, file, ensure_ascii=False, indent=2)
    file.close()


def create_ph_level (con_cursor, entity, system):
    sql_query = read_sql_query(entity)
    sql_output = fetch_query_data(con_cursor, sql_query, entity)
    df_output = write_df(sql_output, system, entity)
    # output_file(loads(df_json.to_json(orient="index")), entity)
    return df_output

# def format_df_pht_old(df, system, entity = 'physical_tables'):
#     df['system'] = system
#     df['id'] = ['_'.join([row.system, row.database, row.schema_name, row.title ]) for row in df.itertuples()]
#     df = df.set_index('id')
#     df.to_csv('PD_dataframes/' + entity + '.csv')
#     return df
