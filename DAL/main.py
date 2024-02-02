import sys
import os
import colorama

from src.utilities import get_directory_path, query_yes_no
from src.cli import dynamic_sql_query, execute_query
from src.cli import argument_parser

colorama.init()


def main():
    relation_csv_dir_path = get_directory_path(path=str(os.path.dirname(__file__)),
                                               levels=1,
                                               directory_name="tables_relationships.csv")

    # args=argument_parser()

    json_file = r"C:\Users\amit.sahoo\OneDrive - Argo Group\DAL\SOURCE_CODE\DAL\Data Samples\sample3.json"

    sql_query = dynamic_sql_query(  # args.json_file,
        json_file,
        relation_csv_dir_path)

    review_query = "\n\nGenerated SQL Query:\n\n\n" + "\33[33m" + sql_query + "\33[0m"
    sys.stdout.write(review_query)

    boolean_input = query_yes_no("\n\n Proceed to execute the query?")

    if boolean_input:
        results = execute_query(sql_query)
        for row in results:
            sys.stdout.write(f"\33[92m {row[0]} \33[0m")
    else:
        sys.stdout.write("SQL query execution aborted.")


if __name__ == "__main__":
    main()
