import os

from jinja2 import Environment, FileSystemLoader
from src.utilities.utils import (
    get_directory_path,
    read_json,
    get_full_relationship,
    read_csv,
    get_full_table_name)
from src.utilities.exceptions import CustomException


def dynamic_sql_query(json_file, csv_file):
    json_data = read_json(json_file)
    relationships_df = read_csv(csv_file)
    sql_query = generate_sql_query(json_data, relationships_df)

    return sql_query


def generate_sql_query(json_data, relationships_df):
    try:

        json_tables = get_full_table_name(json_data)
        
        relevant_relationships_df = relationships_df[
            (relationships_df['full_name_table1'].isin(json_tables)) &
            (relationships_df['full_name_table2'].isin(json_tables))
            ]

        if len(json_tables) == len(relevant_relationships_df):
            relevant_relationships_df = relevant_relationships_df.drop(len(relevant_relationships_df) - 1, axis='index')

        templates_directory_path = get_directory_path(
            path=str(os.path.dirname(__file__)),
            levels=2,
            directory_name="templates"
        )

        env = Environment(loader=FileSystemLoader(templates_directory_path))
        template = env.get_template('template_create_generator.jinja')

        rendered_query = template.render(
            config=json_data,
            relevant_relationships_df=relevant_relationships_df
        )

        return ' '.join(rendered_query.split('\n'))
    except Exception as e:
        raise CustomException(e)
