from src.utilities.exceptions import CustomException
import json
import pandas as pd
from itertools import chain
import os
import sys

def get_file_path(path, levels, file_name):
    """
    Fetches the file path and normalises the path according to the os.

    Args:
        path (str): Location path of the filename provided.
        levels (int): Number of levels of the filename.
        file_name (str): String of the Filename provided.

    Returns:
        str: Normalised path for the provided path, level and filename.
    """    
    path = os.path.normpath(path)
    new_path = os.path.join(os.path.join(path, *([os.pardir] * levels)), file_name)

    return os.path.normpath(new_path)


def read_json(json_file):
    """
    Reads the JSON file in read mode, and loads in variable 'config' and returns the config.

    Args:
        json_file (str): Path to the JSON file.

    Raises:
        CustomException: Function of the Custom Exception function. Found at DAL>utilities>exceptions.py

    Returns:
        dict: Dictionary of the json after being read and typed into dict.
    """    
    try:
        with open(json_file, 'r') as json_file:
            config = json.load(json_file)
            return config
    except Exception as e:
        raise CustomException(e)


def get_full_relationship(df):
    """
    Takes the dataframe with the table names and performs the permutaion and combinations
    to fetch the relationships and spit out as a dataframe.

    Args:
        df (dataframe): Dataframe of the table names read from the JSON Data.

    Returns:
        dataframe: Returns the realtionship dataframe from the relationship tables CSV file.
    """    
    df['full_name_table1'] = df[["Database1", "Schema1", "Table1"]].apply(lambda x: '.'.join(x.values), axis=1)
    df['full_name_table2'] = df[["Database2", "Schema2", "Table2"]].apply(lambda x: '.'.join(x.values), axis=1)

    df[["left_column", "right_column"]] = df['Condition'].str.split('=', expand=True)

    df['full_left_column'] = df[["full_name_table1", "left_column"]].apply(lambda x: '.'.join(x.values), axis=1)
    df['full_right_column'] = df[["full_name_table2", "right_column"]].apply(lambda x: '.'.join(x.values), axis=1)

    df['join_condition'] = df.apply(lambda x: '%s = %s' % (x['full_left_column'], x['full_right_column']), axis=1)

    df = df[["full_name_table1", "full_name_table2", "join_condition"]]
    return df


def read_csv(csv_file):
    """
    Reads the CSV file and returns the data into a dataframe.

    Args:
        csv_file (str): Path of the CSV file located.

    Raises:
        CustomException: Function of the Custom Exception function. Found at DAL>utilities>exceptions.py

    Returns:
        dataframe: Returns the dataframe after running the function get_full_relationship().
    """    
    try:
        relationships_df = pd.read_csv(csv_file)
        return get_full_relationship(relationships_df)
    except Exception as e:
        raise CustomException(e)


def get_full_table_name(json_data):
    """
    Reads the JSON data and fetches the fully qualified table name as per Snowflake standard.

    Args:
        json_data (variable): Variable which stores the JSON data after reading it.

    Returns:
        list: List of the fully qualified table name from the dataframe.
    """    
    table_list = []
    for i in json_data["source_data"]:
        database = i["database"]
        schema = i["schema"]
        table_list.append(
            list(map(lambda table: f"{database}.{schema}.{table}", list(i["table_column_mapping"].keys()))))
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
