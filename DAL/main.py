import sys
import os


from src.utilities import get_parent_directory
from src.utilities import snowflake_connector as sc
from src.utilities.exceptions import CustomException
from src.cli import dynamic_sql_query
from src.cli import argument_parser


def main():
    parent_dir_path = get_parent_directory(path = str(os.path.dirname(__file__)),
                                           levels=1)
    file_path = os.path.join(parent_dir_path, "tables_relationships.csv")

    args=argument_parser()

    json_file = r"C:\Users\amit.sahoo\OneDrive - Argo Group\DAL\SOURCE_CODE\DAL\sample4.json"

    sql_query = dynamic_sql_query(#args.json_file,
                                  json_file,
                                  file_path)
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



