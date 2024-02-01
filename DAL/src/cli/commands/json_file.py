import os
import itertools
import pandas as pd
from jinja2 import Environment, FileSystemLoader
from src.utilities.utils import (
    get_file_path,
    read_json,
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

        possible_combinations_tables = list(itertools.permutations(json_tables, 2))

        possible_combination_df = pd.DataFrame(possible_combinations_tables, 
                                               columns =['full_name_table1', 'full_name_table2'])
        
        relevant_df = pd.merge(possible_combination_df, relationships_df, on=["full_name_table1", "full_name_table2"], how='inner')
        
        templates_file_path = get_file_path(
            path=str(os.path.dirname(__file__)),
            levels=2,
            file_name="templates"
        )

        env = Environment(loader=FileSystemLoader(templates_file_path))
        template = env.get_template('template_create_generator.jinja')

        rendered_query = template.render(
            config=json_data,
            relevant_df=relevant_df
        )

        return ' '.join(rendered_query.split('\n'))
    except Exception as e:
        raise CustomException(e)
