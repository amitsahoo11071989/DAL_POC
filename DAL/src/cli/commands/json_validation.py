import sys
import os
import re

from jinja2 import Environment, FileSystemLoader


from cli.commands.json_schema import JsonStructure
from utilities.exceptions import CustomException
from utilities.utils import get_file_path
from utilities.snowflake_connector import SnowflakeUtils


class JsonValidation:
    def __init__(self, json_data, templates_file_path, sql_command):
        self.json_data = json_data
        self.templates_file_path = templates_file_path
        self.sql_command = sql_command

    def validate_json_format(self):
        try:
            JsonStructure(**self.json_data)
        except Exception as e:
            raise CustomException(e)
        return None

    def validate_json_column(self):
        """
        Validates the JSON configuration data for correctness against database schema.

        Args:
            json_data (JsonValidation): The JSON configuration data containing database, schema details.
        """
        env = Environment(loader=FileSystemLoader(self.templates_file_path))
        template = env.get_template("template_show_columns.jinja")

        #print(self.sql_command)

        # anchor_words = ["FROM", "JOIN"]

        # for anchor in anchor_words:
        #     res = self.sql_command[self.sql_command.find(anchor)+len(anchor):]

        column_pattern = r'\w++\.+\w+'
        table_pattern = r'\w++\.+\w++\.+\w++\s+\w+'
        columns =set( re.findall(column_pattern,self.sql_command))
        tables = set(re.findall(table_pattern,self.sql_command))
        table_column_mapping={}

        for i in tables:
            name,short_form = i.split()
            col_pattern = r'{}+\.+\w+'.format(short_form)
            col_list=[]
            for j in columns:
                if re.match(col_pattern,j):
                    col_list.append(j.split('.')[1])
            table_column_mapping[name]=col_list
        #print(table_column_mapping)

        # for data in self.json_data["source_data"]:
        #     database = data["database"]
        #     schema = data["schema"]

        table_with_non_matching_columns = {}
        for table in table_column_mapping.keys():
            actual_column_list = []

            show_table_query = template.render(
                table=table
            )

            sc = SnowflakeUtils()
            result = sc.execute_query(show_table_query)

            for row in result:
                actual_column_list.append(row[2])

            given_columns = table_column_mapping[table]

            non_matching_columns = list(set(given_columns) - set(actual_column_list))

            table_with_non_matching_columns[table] = non_matching_columns

        spelling_check_bool = [
            True if len(x) > 0 else False
            for x in table_with_non_matching_columns.values()
        ]

        if True in spelling_check_bool:
            error_mesg = ""
            for table, columns in table_with_non_matching_columns.items():
                if columns:
                    error_mesg = (
                        error_mesg
                        + f"\n {table} table does not have {str(columns)[1:-1]} columns\n"
                    )
            sys.stdout.write(error_mesg)
            sys.exit()

    def run(self):
        #self.validate_json_format()
        self.validate_json_column()

    __call__ = run
