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
        final_joining_conditions = []
        for i in (0,len(json_tables)-2):
            table1 = json_tables[i]
            table2 = json_tables[i+1]
            filtered_df = relevant_relationships_df[(relevant_relationships_df['full_name_table1'] == table1) & (relevant_relationships_df['full_name_table2'] == table2) | (relevant_relationships_df['full_name_table1'] == table2) & (relevant_relationships_df['full_name_table2'] == table1)]
            #temp_df1 = relevant_relationships_df[(relevant_relationships_df['full_name_table1']==table1) & (relevant_relationships_df['full_name_table2']==table2)]
            # temp_df2 = temp_df1[temp_df1['full_name_table2']==table2]
            final_joining_conditions.append(' JOIN {} ON {}'.format(table2, filtered_df['join_condition'].values[0]))
        print(final_joining_conditions)
        env = Environment(loader=FileSystemLoader(templates_directory_path))
        template = env.get_template('template_create_generator.jinja')
        #print("\n",json_data,"\n",relevant_relationships_df)
        rendered_query = template.render(
            final_joining_conditions = final_joining_conditions,
            json_tables = json_tables,
            config=json_data,
            relevant_relationships_df=relevant_relationships_df
        )

        return ' '.join(rendered_query.split('\n'))
    except Exception as e:
        raise CustomException(e)
