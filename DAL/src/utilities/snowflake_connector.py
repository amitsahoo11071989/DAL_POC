import snowflake.connector
from dotenv import load_dotenv
from src.utilities import get_file_path
import os


dotenv_path = get_file_path(path=str(os.path.dirname(__file__)),
                                  levels=3,
                                  file_name=".env")



def open_connection():
    """
    Connects the python program to snowflake sql instance using the credentials in environment.

    Returns:
        conn: Returns the conn object for running the sql statements.
    """
    load_dotenv(dotenv_path=dotenv_path)

    conn = snowflake.connector.connect(
        user = os.getenv("USER"),
        password = os.getenv("PASSWORD"),
        account = os.getenv("ACCOUNT")
    )
    return conn

def close_connection(conn):
    """Closes the snowflake connection.
    Args:
        conn (object): Object of the snowflake cursor object.
    """    
    conn.close()
