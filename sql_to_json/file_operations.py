import os
from json import dump, loads
import pandas as pd

from sql_to_json.settings import SEAF_PREFIX, FOLDER_PATH as FP

def full_path(folder):
    return os.path.join(os.getcwd(), folder)

def clear_dfs(dir_path):
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        open(file_path, 'w').close()

def output_file(df, entity='physical_tables'):
    fullname = FP['json'] + entity + '.json'
    json_dict = {SEAF_PREFIX + entity: df}
    with open(fullname, "w", encoding="UTF-8") as file:
        dump(json_dict, file, ensure_ascii=False, indent=2)
    file.close()

def create_json_files(dir_path):
    for filename in os.listdir(dir_path):
        df_json = pd.read_csv(os.path.join(dir_path, filename))
        df_json = df_json.set_index('id')
        output_file(loads(df_json.to_json(orient="index")), os.path.splitext(filename)[0])
