import sys
import os
import colorama

from src.utilities import get_file_path, query_yes_no
from src.utilities.snowflake_connector import SnowflakeUtils
from src.cli import dynamic_sql_query
from src.cli import InputParser

colorama.init()


def main():
    """
    Starting point of the DAL Program.
    This function needs to be run from the CLI with the JSON file as argument
    Executes the SQL Query in the Snowflake DB after fetching the,
    relationships between tables and establishing the JOIN statements between them.

    """    
    relation_csv_dir_path = get_file_path(path=str(os.path.dirname(__file__)),
                                               levels=1,
                                               file_name="tables_relationships.csv")

    parser=InputParser()
    args = parser.argument_parser()

    json_file = get_file_path(path=str(os.path.dirname(__file__)),
                                               levels=1,
                                               file_name="Data_Samples/sample4.json")
    
    sql_query = dynamic_sql_query(json_file if args.json_file is None else args.json_file,
                                  relation_csv_dir_path)

    review_query = "\n\nGenerated SQL Query:\n\n\n" + "\33[33m" + sql_query + "\33[0m"
    sys.stdout.write(review_query)

    boolean_input = query_yes_no("\n\n Proceed to execute the query?")

    if boolean_input:
        results = SnowflakeUtils.execute_query(sql_query)
        for row in results:
            sys.stdout.write(f"\33[92m {row[0]} \33[0m")
    else:
        sys.stdout.write("SQL query execution aborted.")


if __name__ == "__main__":
    main()
