import snowflake.connector

account = 'argo'
user = 'AMIT'
password = '***'

def open_connection():

    conn = snowflake.connector.connect(
        user = user,
        password = password,
        account = account
    )
    return conn

def close_connection(conn):
    conn.close()
