import sys
import os

from jinja2 import Environment, FileSystemLoader


from cli.commands.json_schema import JsonStructure
from utilities.exceptions import CustomException
from utilities.utils import get_file_path
from utilities.snowflake_connector import SnowflakeUtils


class JsonValidation:
    def __init__(self, json_data) -> None:
        self.json_data = json_data

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
        templates_file_path = get_file_path(
            path=str(os.path.dirname(__file__)), levels=2, file_name="templates"
        )

        env = Environment(loader=FileSystemLoader(templates_file_path))
        template = env.get_template("template_show_columns.jinja")

        for data in self.json_data["source_data"]:
            database = data["database"]
            schema = data["schema"]

            table_with_non_matching_columns = {}
            for table in data["table_column_mapping"].keys():
                actual_column_list = []

                show_table_query = template.render(
                    database=database, schema=schema, table=table
                )

                sc = SnowflakeUtils()
                result = sc.execute_query(show_table_query)

                for row in result:
                    actual_column_list.append(row[2])

                given_columns = list(map(lambda column: column.split()[0],
                                     data["table_column_mapping"][table]))

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
        self.validate_json_format()
        self.validate_json_column()

    __call__ = run
