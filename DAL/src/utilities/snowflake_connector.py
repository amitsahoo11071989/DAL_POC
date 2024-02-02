import snowflake.connector
from dotenv import load_dotenv
from src.utilities import get_file_path
import os

dotenv_path = get_file_path(path=str(os.path.dirname(__file__)),
                                  levels=3,
                                  file_name=".env")



def open_connection():

    load_dotenv(dotenv_path=dotenv_path)

    conn = snowflake.connector.connect(
        user = os.getenv("USER"),
        password = os.getenv("PASSWORD"),
        account = os.getenv("ACCOUNT")
    )
    return conn

def close_connection(conn):
    conn.close()
