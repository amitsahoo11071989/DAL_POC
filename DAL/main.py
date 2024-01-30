import sys
import os
import colorama

from src.utilities import get_directory_path
from src.utilities import snowflake_connector as sc
from src.utilities.exceptions import CustomException
from src.cli import dynamic_sql_query
from src.cli import argument_parser

colorama.init()

def main():
    relation_csv_dir_path = get_directory_path(path = str(os.path.dirname(__file__)),
                                           levels=1,
                                            directory_name = "tables_relationships.csv")

    #args=argument_parser()

    json_file = r"C:\Users\amit.sahoo\OneDrive - Argo Group\DAL\SOURCE_CODE\DAL\Data Samples\sample4.json"

    sql_query = dynamic_sql_query(#args.json_file,
                                  json_file,
                                  relation_csv_dir_path)
    
    review_query = input("\n\nGenerated SQL Query:\n\n\n" + "\33[33m" + sql_query + "\33[0m" +"\n\nProceed to execute the query? (y/n): ").lower()
    
    
    print(review_query)
    if review_query == 'y':
            execute_query(sql_query)
    else:
        sys.stdout.write("SQL query execution aborted.")


def execute_query(sql_query):
    try:
        conn = sc.open_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        for row in results:
            sys.stdout.write(f"\33[92m {row[0]} \33[0m")
        cursor.close()
        sc.close_connection(conn)

    except Exception as e:
         raise CustomException(e)


if __name__ == "__main__":
    main()
    #print(sys.path)



