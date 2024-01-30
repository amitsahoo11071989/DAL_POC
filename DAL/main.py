import sys
import os


from src.utilities import get_directory_path
from src.utilities import snowflake_connector as sc
from src.utilities.exceptions import CustomException
from src.cli import dynamic_sql_query
from src.cli import argument_parser


def main():
    relation_csv_dir_path = get_directory_path(path = str(os.path.dirname(__file__)),
                                           levels=1,
                                            directory_name = "tables_relationships.csv")

    args=argument_parser()

    json_file = r"C:\Users\Amit\PycharmProjects\pythonProject\sample\pythonProject\Prototype\DAL_POC\sample3.json"

    sql_query = dynamic_sql_query(#args.json_file,
                                  json_file,
                                  relation_csv_dir_path)
    review_query = input("\n\nGenerated SQL Query:\n\n\n" +'\33[33m' +sql_query +'\033[0m' +"\n\nProceed to execute the query? (yes/no): ").lower()
    
    
    print(review_query)
    # if review_query == 'yes':
    #         execute_query(sql_query)
    # else:
    #     print("SQL query execution aborted.")

    #print(sys.path)

def execute_query(sql_query):
    try:
        conn = sc.open_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        for row in results:
            print(row)

        cursor.close()
        sc.close_connection(conn)
    except Exception as e:
         raise CustomException(e)


if __name__ == "__main__":
    main()
    #print(sys.path)



