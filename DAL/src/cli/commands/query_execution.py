import sys
from src.utilities import snowflake_connector as sc
from src.utilities.exceptions import CustomException

def execute_query(sql_query):
    """
    Connects to Snowflake and executes the sql_query that is passed in.

    Args:
        sql_query (str): The SQL query to be executed.

    Raises:
        CustomException: If an error occurs during query execution.

    Returns:
        list: Returns the execution results as a list.
    """    
    try:
        conn = sc.open_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        return results
        
    except Exception as e:
         raise CustomException(e)
    
    finally:
        if 'conn' in locals():
            cursor.close()
            sc.close_connection(conn)
        
        