import sys
from src.utilities import snowflake_connector as sc
from src.utilities.exceptions import CustomException

def execute_query(sql_query):
    try:
        conn = sc.open_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        return results
        
    except Exception as e:
         raise CustomException(e)
    
    finally:
        cursor.close()
        sc.close_connection(conn)
        
        