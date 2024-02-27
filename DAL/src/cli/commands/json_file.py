import os
import itertools
import pandas as pd

from jinja2 import Environment, FileSystemLoader
from utilities.snowflake_connector import SnowflakeUtils

from utilities.utils import get_file_path, read_json, read_csv, get_full_table_name, read_sql
from utilities.exceptions import CustomException
from cli.commands.json_validation import JsonValidation


class SqlGenerator:
    
    def __init__(self, json_file) -> None:

        self.json_data = read_json(json_file)

        self.templates_file_path = get_file_path(
            path=str(os.path.dirname(__file__)), levels=2, file_name="templates"
        )

        self.sql_file_path = get_file_path(
            path=str(os.path.dirname(__file__)), 
            levels=4, 
            file_name=f"Data_Samples/{self.json_data['source_sql']}" 
        )

        self.sql_command = read_sql(self.sql_file_path)

    def dynamic_sql_query(self):
        """
        Reads the passed in JSON and CSV files and generates a dynamic SQL query based on their configuration.

        Args:
            json_file (str): Path to the JSON configuration file.
            csv_file (str): Path to the CSV file containing relationships data.

        Returns:
            str: The dynamically generated sql query.
        """

        json_validation = JsonValidation(self.json_data,
                                         self.templates_file_path, 
                                         self.sql_command)
        try:
            json_validation()
        except Exception as e:
            raise CustomException(e)

        env = Environment(loader=FileSystemLoader(self.templates_file_path))
        template = env.get_template("template_create_generator.jinja")

        return template.render(config=self.json_data,
                                         sql = self.sql_command)
    