import json
import os
import pandas as pd
from itertools import chain
import sys
from jinja2 import Environment, FileSystemLoader
from src.utilities.utils import get_parent_directory, read_json
from src.utilities.exceptions import CustomException


def dynamic_sql_query(json_file, csv_file):
    json_data = read_json(json_file)
    relationships_df = read_csv(csv_file)
    sql_query = generate_sql_query(json_data, relationships_df)

    return sql_query



def get_full_table_name(json_data):
    table_list = []
    for i in json_data["source_data"]:
        database = i["database"]
        schema = i["schema"]
        table_list.append(
            list(map(lambda table: f"{database}.{schema}.{table}", list(i["table_column_mapping"].keys()))))
    return list(chain(*table_list))

def get_full_table_name_from_relationsip_table(df):
    df['full_name_table1'] = df[["Database1", "Schema1", "Table1"]].apply(lambda x: '.'.join(x.values), axis=1)
    df['full_name_table2'] = df[["Database2", "Schema2", "Table2"]].apply(lambda x: '.'.join(x.values), axis=1)

    condition_list = df['Condition'].str.split("=")
    df[["left_column", "right_column"]] = df['Condition'].str.split('=', expand=True)

    df['full_left_column'] = df[["Database1", "Schema1", "left_column"]].apply(lambda x: '.'.join(x.values), axis=1)
    df['full_right_column'] = df[["Database2", "Schema2", "right_column"]].apply(lambda x: '.'.join(x.values), axis=1)
    
    df['join_condition']=df.apply(lambda x:'%s = %s' % (x['full_left_column'],x['full_right_column']),axis=1)
    return df

def read_csv(csv_file):
    try:
        relationships_df = pd.read_csv(csv_file)
        return get_full_table_name_from_relationsip_table(relationships_df)
    except Exception as e:
        raise CustomException(e)


def generate_sql_query(json_data, relationships_df):
    try:

        json_tables = get_full_table_name(json_data)
        
        relevant_relationships_df = relationships_df[
            (relationships_df['full_name_table1'].isin(json_tables)) &
            (relationships_df['full_name_table2'].isin(json_tables))
            ]

        if len(json_tables) == len(relevant_relationships_df):
            relevant_relationships_df = relevant_relationships_df.drop(len(relevant_relationships_df) - 1, axis='index')

        parent_directory = get_parent_directory(path=str(os.path.dirname(__file__)),
                                    levels=2)
        jinja_file_dir = os.path.join(parent_directory, "templates")

        env = Environment(loader=FileSystemLoader(jinja_file_dir))
        template = env.get_template('template_create_generator.jinja')

        rendered_query = template.render(
            config=json_data,
            relevant_relationships_df=relevant_relationships_df
        )

        return ' '.join(rendered_query.split('\n'))
    except Exception as e:
        raise CustomException(e)
