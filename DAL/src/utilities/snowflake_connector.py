import snowflake.connector
from dotenv import load_dotenv
from src.utilities import get_directory_path
import os

dotenv_path = get_directory_path(path=str(os.path.dirname(__file__)),
                                  levels=3,
                                  directory_name=".env")



def open_connection():

    load_dotenv(dotenv_path=dotenv_path)

    conn = snowflake.connector.connect(
        # user = os.getenv("USER"),
        # password = os.getenv("PASSWORD"),
        # account = os.getenv("ACCOUNT")

        user = "pratik.sinha@argogroupus.com",
        authenticator="externalbrowser",
        account = "argo.us-east-1"
    )
    return conn

def close_connection(conn):
    conn.close()
