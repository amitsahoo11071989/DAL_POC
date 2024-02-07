import os
import itertools
import pandas as pd
import sys
from jinja2 import Environment, FileSystemLoader
from typing import List, Dict
from src.cli.commands.query_execution import execute_query
from pydantic import BaseModel
from src.utilities.utils import (
    get_file_path,
    read_json,
    read_csv,
    get_full_table_name)
from src.utilities.exceptions import CustomException
class SqlGenerator:
    def __init__(self,json_file,csv_file) -> None:
        self.json_file = json_file
        self.csv_file = csv_file

    def dynamic_sql_query(self):
        """
        Reads the passed in JSON and CSV files and generates a dynamic SQL query based on their configuration.

        Args:
            json_file (str): Path to the JSON configuration file.
            csv_file (str): Path to the CSV file containing relationships data.

        Returns:
            str: The dynamically generated sql query.
        """    

        json_data = read_json(self.json_file)
        json_validation = JsonValidation(json_data)
        try:
            json_validation()
        except Exception as e:
            raise CustomException(e)
        
        relationships_df = read_csv(self.csv_file)
        relationships_df = relationships_df
        sql_query = self.generate_sql_query(json_data, relationships_df)
        # return sql_query
            
        
        
    


    def generate_sql_query(self,json_data, relationships_df):
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
            joins_string_list = [self.get_join_string_for_each_pair(table_pair, relevant_df) 
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


    def get_join_string_for_each_pair(self,table_pair, relevant_df):
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



class JsonValidation:
    def __init__(self,json_data) -> None:
        self.json_data = json_data

    def validate_json(self):
        class SourceData(BaseModel):
            database: str
            schema: str
            table_column_mapping: Dict[str, List[str]]

        class JsonStructure(BaseModel):
            target_table_name: str
            target_lag: str
            warehouse: str
            source_data: list[SourceData]
        
        validation_result = JsonStructure(**self.json_data)
        print("*******")
        return validation_result
    
    def validate_json_data(self):
        """
        Validates the JSON configuration data for correctness against database schema.

        Args:
            json_data (dict): The JSON configuration data containing database, schema details.
        """
        print("#########")
        templates_file_path = get_file_path(
                path=str(os.path.dirname(__file__)),
                levels=2,
                file_name="templates")
        
        env = Environment(loader=FileSystemLoader(templates_file_path))
        template = env.get_template('template_show_columns.jinja')
        
        for data in self.json_data["source_data"]:
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
    
    def run(self):
        self.validate_json()
        self.validate_json_data()

    __call__ = run
            
