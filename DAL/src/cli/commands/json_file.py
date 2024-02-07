import os
import itertools
import pandas as pd
import sys
from jinja2 import Environment, FileSystemLoader
from src.utilities.snowflake_connector import SnowflakeUtils
from src.utilities.utils import (
    get_file_path,
    read_json,
    read_csv,
    get_full_table_name)
from src.utilities.exceptions import CustomException


def dynamic_sql_query(json_file, csv_file):
    """
    Reads the passed in JSON and CSV files and generates a dynamic SQL query based on their configuration.

    Args:
        json_file (str): Path to the JSON configuration file.
        csv_file (str): Path to the CSV file containing relationships data.

    Returns:
        str: The dynamically generated sql query.
    """    
    json_data = read_json(json_file)
    relationships_df = read_csv(csv_file)
    validate_json_data(json_data)

    sql_query = generate_sql_query(json_data, relationships_df)
    return sql_query
         

def validate_json_data(json_data):
    """
    Validates the JSON configuration data for correctness against database schema.

    Args:
        json_data (dict): The JSON configuration data containing database, schema details.
    """
    templates_file_path = get_file_path(
            path=str(os.path.dirname(__file__)),
            levels=2,
            file_name="templates")
    
    env = Environment(loader=FileSystemLoader(templates_file_path))
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

            snnw = SnowflakeUtils()
            result = snnw.execute_query(show_table_query)

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
    """
    Uses templates to generate a dynamic SQL query based on JSON configuration and relationships data.


    Args:
        json_data (dict): The JSON configuration data containing details about tables.
        relationships_df (pd.DataFrame): DataFrame containing information about relationships between tables.

    Raises:
        CustomException: If an error occurs during SQL query generation.

    Returns:
        str: The dynamically generated sql query.
    """    
    try:
        json_tables = get_full_table_name(json_data)

        possible_combinations_tables = list(itertools.permutations(json_tables, 2))

        possible_combination_df = pd.DataFrame(possible_combinations_tables, 
                                               columns =['full_name_table1', 'full_name_table2'])
        
        relevant_df = pd.merge(possible_combination_df, relationships_df, on=["full_name_table1", "full_name_table2"], how='inner')
        
        templates_file_path = get_file_path(
            path=str(os.path.dirname(__file__)),
            levels=2,
            file_name="templates"
        )

        pairwise_consecutive_tables_list = list(zip(json_tables,json_tables[1:]))
        joins_string_list = [get_join_string_for_each_pair(table_pair, relevant_df) 
                             for table_pair in pairwise_consecutive_tables_list]
        
        joins_string = '\n'.join(joins_string_list)

        env = Environment(loader=FileSystemLoader(templates_file_path))
        template = env.get_template('template_create_generator.jinja')

        rendered_query = template.render(
            config=json_data,
            json_tables = json_tables,
            joins_string = joins_string
        )

        return ' '.join(rendered_query.split('\n'))
    except Exception as e:
        raise CustomException(e)


def get_join_string_for_each_pair(table_pair, relevant_df):
    """
    Get the JOIN string for a pair of tables based on their relationship in relationships DataFrame.

    Args:
        table_pair (tuple): A tuple containing two table names.
        relevant_df (pd.DataFrame): DataFrame containing information about relationships between tables.

    Returns:
        str: The JOIN string for the given pair of tables.
    """    
    result = relevant_df.loc[(
            (
                (relevant_df['full_name_table1']==table_pair[0]) & 
                (relevant_df['full_name_table2']==table_pair[1])
                ) |
                (
                (relevant_df['full_name_table2']==table_pair[0]) &
                    (relevant_df['full_name_table1']==table_pair[1])
                    )
                    ),'join_condition'].item()
    return f' JOIN {table_pair[1]} ON {result}'
