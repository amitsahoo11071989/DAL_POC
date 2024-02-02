import os
import itertools
import pandas as pd
import sys
from jinja2 import Environment, FileSystemLoader
from src.cli.commands.query_execution import execute_query
from src.utilities.utils import (
    get_directory_path,
    read_json,
    read_csv,
    get_full_table_name)
from src.utilities.exceptions import CustomException




def dynamic_sql_query(json_file, csv_file):
    json_data = read_json(json_file)
    relationships_df = read_csv(csv_file)
    validate_json_data(json_data)

    sql_query = generate_sql_query(json_data, relationships_df)
    return sql_query

         

def validate_json_data(json_data):

    templates_directory_path = get_directory_path(
            path=str(os.path.dirname(__file__)),
            levels=2,
            directory_name="templates")
    
    env = Environment(loader=FileSystemLoader(templates_directory_path))
    template = env.get_template('template_show_columns.jinja')
    
    for data in json_data["source_data"]:
        database = data["database"]
        schema = data["schema"]

        table_with_non_matching_columns= {}
        for table in data["table_column_mapping"].keys():
            column_list = []

            show_table_query = template.render(
            database=database,
            schema=schema,
            table=table
            )

            result = execute_query(show_table_query)

            for row in result:
                    column_list.append(row[2])

            given_columns = data["table_column_mapping"][table]

            non_matching_columns = list(set(given_columns) - set(column_list))

            table_with_non_matching_columns[table] = non_matching_columns

    spelling_check_bool = [True if len(x)>0 else False for x in 
                           table_with_non_matching_columns.values()]

    if True in spelling_check_bool:
        error_mesg = ""
        for table, columns in table_with_non_matching_columns.items():
            if columns:
                error_mesg = error_mesg + f"\n {table} table does not have {str(columns)[1:-1]} columns\n"
        sys.stdout.write(error_mesg)
        sys.exit()

def generate_sql_query(json_data, relationships_df):
    try:
        json_tables = get_full_table_name(json_data)

        possible_combinations_tables = list(itertools.permutations(json_tables, 2))

        possible_combination_df = pd.DataFrame(possible_combinations_tables, 
                                               columns =['full_name_table1', 'full_name_table2'])
        
        relevant_df = pd.merge(possible_combination_df, relationships_df, on=["full_name_table1", "full_name_table2"], how='inner')
        
        templates_directory_path = get_directory_path(
            path=str(os.path.dirname(__file__)),
            levels=2,
            directory_name="templates"
        )

        env = Environment(loader=FileSystemLoader(templates_directory_path))
        template = env.get_template('template_create_generator.jinja')

        rendered_query = template.render(
            config=json_data,
            relevant_df=relevant_df
        )

        return ' '.join(rendered_query.split('\n'))
    except Exception as e:
        raise CustomException(e)
