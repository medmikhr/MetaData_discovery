from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SEAF_PREFIX = 'seaf.ia.'

FOLDER_PATH = {
    'sql': 'SQL_scripts/',
    'csv': 'PD_dataframes/',
    'json': 'Output_JSON/'
}

PROXY_MAPPING = {
    'logical_entities': ['physical_tables', 'data_objects'],
    'business_terms': ['logical_entities', 'business_objects']
}
