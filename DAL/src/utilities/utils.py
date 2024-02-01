from src.utilities.exceptions import CustomException
import json
import pandas as pd
from itertools import chain
import os
import sys

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

    df = df[["full_name_table1", "full_name_table2", "join_condition"]]
    return df


def read_csv(csv_file):
    try:
        relationships_df = pd.read_csv(csv_file)
        return get_full_relationship(relationships_df)
    except Exception as e:
        raise CustomException(e)

def get_full_table_name(json_data):
    table_list = []
    for i in json_data:
        database = json_data["database"]
        schema = json_data["schema"]
        table_list.append(
            list(map(lambda table: f"{database}.{schema}.{table}", list(json_data["table_column_mapping"].keys()))))
    return list(chain(*table_list))


def query_yes_no(question, default="no"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")
