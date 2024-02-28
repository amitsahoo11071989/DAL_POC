from dotenv import load_dotenv
from .utils import get_file_path
from .exceptions import CustomException
from snowflake import connector
import os

dotenv_path = get_file_path(
    path=str(os.path.dirname(__file__)), levels=3, file_name=".env"
)


class SnowflakeUtils:
    def __init__(self) -> None:
        load_dotenv(dotenv_path=dotenv_path)
        self.user = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.account = os.getenv("ACCOUNT")

    def open_connection(self):
        """
        Connects the python program to snowflake sql instance using the credentials in environment.

        Returns:
            conn: Returns the conn object for running the sql statements.
        """
        conn = connector.connect(
            user=self.user, password=self.password, account=self.account
        )
        return conn

    def close_connection(self, conn):
        """Closes the snowflake connection.
        Args:
            conn (object): Object of the snowflake cursor object.
        """
        conn.close()

    def execute_query(self, sql_query):
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
            conn = self.open_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query)
            results = cursor.fetchall()
            return results

        except Exception as e:
            raise CustomException(e)

        finally:
            if 'conn' in locals():
                cursor.close()
                self.close_connection(conn)
