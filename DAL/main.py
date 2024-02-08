import sys
import os
import colorama

from src.utilities import get_file_path, query_yes_no
from src.utilities.snowflake_connector import SnowflakeUtils
from src.cli import SqlGenerator

from src.cli import InputParser

colorama.init()

def main():
    """
    Starting point of the DAL Program.
    This function needs to be run from the CLI with the JSON file as argument
    Executes the SQL Query in the Snowflake DB after fetching the,
    relationships between tables and establishing the JOIN statements between them.

    """    
    
    args = InputParser().argument_parser()

    json_file = get_file_path(path=str(os.path.dirname(__file__)),
                                               levels=1,
                                               file_name="Data_Samples/sample3.json")
    
    sql_generator = SqlGenerator(json_file if args.json_file is None else args.json_file)
    sql_query = sql_generator.dynamic_sql_query()
       

    review_query = "\n\nGenerated SQL Query:\n\n\n" + "\33[33m" + sql_query + "\33[0m"
    sys.stdout.write(review_query)

    boolean_input = query_yes_no("\n\n Proceed to execute the query?")

    if boolean_input:
        sc = SnowflakeUtils()
        results = sc.execute_query(sql_query)
        for row in results:
            sys.stdout.write(f"\33[92m {row[0]} \33[0m")
    else:
        sys.stdout.write("SQL query execution aborted.")


if __name__ == "__main__":
    main()
