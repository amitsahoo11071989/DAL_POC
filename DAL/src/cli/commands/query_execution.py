import sys
from src.utilities import snowflake_connector as sc
from src.utilities.exceptions import CustomException

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