import os
import itertools
import pandas as pd
from jinja2 import Environment, FileSystemLoader
from src.utilities.utils import (
    get_directory_path,
    read_json,
    read_csv,
    get_full_table_name)
from src.utilities.exceptions import CustomException

env = Environment(loader=FileSystemLoader(r"/Users/ravitejagorti/Desktop/DAL_POC/DAL/src/templates"))


def dynamic_sql_query(json_file, csv_file):
    json_data = read_json(json_file)
    relationships_df = read_csv(csv_file)
    sql_query = [generate_sql_query(source, relationships_df) for source in json_data['source_data']]
    template_create = env.get_template('template_create_generator.jinja')
        
    rendered_query = template_create.render(
        config=json_data,
    )
    return '\n'.join(sql_query)


def generate_sql_query(source, relationships_df):
    try:
        json_tables = get_full_table_name(source)

        possible_combinations_tables = list(itertools.permutations(json_tables, 2))

        possible_combination_df = pd.DataFrame(possible_combinations_tables, 
                                               columns =['full_name_table1', 'full_name_table2'])
        
        relevant_df = pd.merge(possible_combination_df, relationships_df, on=["full_name_table1", "full_name_table2"], how='inner')
        
        templates_directory_path = get_directory_path(
            path=str(os.path.dirname(__file__)),
            levels=2,
            directory_name="templates"
        )
        #templates_directory_path = os.path.join('../','../','templates')


        def get_join_string_for_each_pair(tup):
            result = relevant_df.loc[(((relevant_df['full_name_table1']==tup[0]) & (relevant_df['full_name_table2']==tup[1])) | ((relevant_df['full_name_table2']==tup[0]) & (relevant_df['full_name_table1']==tup[1]))),'join_condition'].tolist()
            return f' JOIN {tup[1]} ON {result}'
        

        ordered_pairwise_tables_list = list(zip(json_tables,json_tables[1:]))
        joins_string_list = [get_join_string_for_each_pair(i) for i in ordered_pairwise_tables_list]
        joins_string = '\n'.join(joins_string_list)

        
        
        template_sql = env.get_template('template_sql_generator.jinja')
        print(source)
        rendered_query = template_sql.render(
            config = source,
            json_tables = json_tables,
            joins_string = joins_string
        )

        return ' '.join(rendered_query.split('\n'))
    except Exception as e:
        raise CustomException(e)
