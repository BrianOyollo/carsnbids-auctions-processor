import os
from dotenv import load_dotenv
from airflow.models.connection import Connection

script_dir = os.path.dirname(os.path.abspath(__file__))
env_file_path = os.path.expanduser("~/airflow/.env")

load_dotenv(dotenv_path=env_file_path)


# load aws credentials
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# DB
## POSTGRESQL
POSTGRES_DB_USER = os.getenv('DB_USER')
POSTGRES_DB_PASSWORD = os.getenv('DB_PASSWORD')
POSTGRES_DB_HOST = os.getenv('DB_HOST')
POSTGRES_DB_PORT = os.getenv('DB_PORT')
POSTGRES_DB_NAME = os.getenv('DB_NAME')



def setup_aws_connection(connection_name:str, aws_access_key:str,aws_secret_access:str, aws_region:str)->str:
    """
    Creates an Airflow AWS connection URI and appends it to a .env file for automatic registration.

    This function constructs a valid Airflow connection string for AWS using the provided access credentials and region.
    It then appends the connection string to the specified `.env` file, allowing Airflow to register the connection
    via environment variables (Airflow reads all variables prefixed with `AIRFLOW_CONN_`).

    Parameters:
        connection_name (str): The name of the Airflow connection (e.g., 'aws_default').
        aws_access_key (str): The AWS access key ID.
        aws_secret_access (str): The AWS secret access key.
        aws_region (str): AWS region (e.g., 'us-east-1').

    Returns:
        str: The constructed Airflow AWS connection URI.

    """
    conn = Connection(
        conn_id=connection_name,
        conn_type="aws",
        login=aws_access_key,
        password=aws_secret_access,
        extra={
            "region_name": aws_region,
        },
    )
    conn_uri = conn.get_uri()
    env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"

    # write connection uri to .env
    with open(env_file_path, 'a') as f:
        f.write(f"{env_key}={conn_uri}\n")
        print(f"{env_key} added to .env")



def setup_postgres_connection(
        connection_name: str, 
        db_user: str, 
        db_password: str,
        db_host: str, 
        db_port: int, 
        db_name: str,
        env_file_path: str = '.env'
    )->str:
    """
    Creates a PostgreSQL Airflow connection URI and appends it to a .env file.

    Args:
        connection_name (str): Name of the Airflow connection (e.g., 'pg_conn').
        db_user (str): PostgreSQL username.
        db_password (str): PostgreSQL password.
        db_host (str): Hostname or IP of the PostgreSQL server.
        db_port (int): Port number (typically 5432).
        db_name (str): Database name.
        env_file_path (str): Path to the .env file (default: '.env').

    Returns:
        str: The connection URI added to the .env file.
    """
    conn = Connection(
        conn_id=connection_name,
        conn_type="postgresql",
        description="postgresql db connection",
        host=db_host,
        port=db_port,
        login=db_user,
        password=db_password,
        schema=db_name,
    )
    env_key = f"AIRFLOW_CONN_{connection_name.upper()}"
    conn_uri = conn.get_uri()
    with open(env_file_path, 'a') as f:
        f.write(f"{env_key}={conn_uri}\n")
        print(f"{env_key} added to {env_file_path}")

    return conn_uri


setup_aws_connection('aws_default', AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_REGION)
setup_postgres_connection("postgres_default_local", POSTGRES_DB_USER,POSTGRES_DB_PASSWORD,POSTGRES_DB_HOST,POSTGRES_DB_PORT,POSTGRES_DB_NAME)