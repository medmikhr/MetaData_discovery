import pandas as pd
import os

from sql_to_json.settings import FOLDER_PATH as FP


def read_sql_query(sql_file = 'physical_tables'):
    with open(FP['sql'] + sql_file + '.sql', 'r') as file:
        query = file.read()
    file.close()
    return query


def fetch_query_data(con_cur, query_str):
    con_cur.execute(query_str)
    data = con_cur.fetchall()
    columns = [t[0] for t in con_cur.description]
    output_df = pd.DataFrame(data, columns=columns)
    return output_df


def write_df(df, system, entity = 'physical_tables'):
    if entity == 'physical_tables':
        df['system'] = system
        df['dbms'] = 'postgres'
    #     df['id'] = ['_'.join([row.system, row.database, row.table_schema, row.table_name]) for row in
    #                           df.itertuples()]
    # else:
    #     df['id'] = system + df['physical_table'] + '.' + df['title']
    df['id'] = system + '_' + df['id']
    df = df.set_index('id')
    output_path = FP['csv'] + entity + '.csv'
    header_switch = True if os.path.getsize(os.path.join(os.getcwd(), output_path)) == 0 else False
    df.to_csv(output_path, mode='a', header=header_switch)
    return df


def create_ph_level (con_cursor, entity, system):
    sql_query = read_sql_query(entity)
    sql_output = fetch_query_data(con_cursor, sql_query)
    df_output = write_df(sql_output, system, entity)
    return df_output
