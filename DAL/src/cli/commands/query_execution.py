import sys
from src.utilities import snowflake_connector as sc
from src.utilities.exceptions import CustomException
import snowflake

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

    # except Exception as e:
    #      raise CustomException(e)
    
    except snowflake.connector.errors.ProgrammingError as e:
        # default error message
        # print(e)
        # customer error message
        # print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

        err = e.msg
        err_list = err.split("\n")
        if err_list[1].find('SQL compilation error') and err_list[1].find('already exists.'):
            sys.stdout.write("The SQL Query returned the error below\n\n",err,"\n\n")
            sys.stdout.write("Do you wish to:\n1. Re-run the SQL with new table name.\n2.Abort the execution.")
            sys.exit(0)
        else:
            raise CustomException(e)