from src.utilities.exceptions import CustomException
import json
import pandas as pd
from itertools import chain
import os

def get_directory_path(path, levels, directory_name):
    path_splits = path.split('\\')[:-levels]
    return_str = ''
    for element in path_splits:
        return_str += element + '\\'
    return os.path.join(return_str.rstrip('\\'), directory_name)

def read_json(json_file):
    try:
        with open(json_file, 'r') as json_file:
            config = json.load(json_file)
            return config
    except Exception as e:
        raise CustomException(e)


def get_full_relationship(df):
    df['full_name_table1'] = df[["Database1", "Schema1", "Table1"]].apply(lambda x: '.'.join(x.values), axis=1)
    df['full_name_table2'] = df[["Database2", "Schema2", "Table2"]].apply(lambda x: '.'.join(x.values), axis=1)

    df[["left_column", "right_column"]] = df['Condition'].str.split('=', expand=True)

    df['full_left_column'] = df[["full_name_table1", "left_column"]].apply(lambda x: '.'.join(x.values), axis=1)
    df['full_right_column'] = df[["full_name_table2", "right_column"]].apply(lambda x: '.'.join(x.values), axis=1)

    df['join_condition'] = df.apply(lambda x: '%s = %s' % (x['full_left_column'], x['full_right_column']), axis=1)
    return df


def read_csv(csv_file):
    try:
        relationships_df = pd.read_csv(csv_file)
        return get_full_relationship(relationships_df)
    except Exception as e:
        raise CustomException(e)

def get_full_table_name(json_data):
    table_list = []
    for i in json_data["source_data"]:
        database = i["database"]
        schema = i["schema"]
        table_list.append(
            list(map(lambda table: f"{database}.{schema}.{table}", list(i["table_column_mapping"].keys()))))
    return list(chain(*table_list))
