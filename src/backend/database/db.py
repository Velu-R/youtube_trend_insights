import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from src.data.dataset import get_dataset
from src.backend.utils.logger import get_logger

connection = None 
# Load environment variables from .env
load_dotenv()

# Configure logging
logger = get_logger()

TABLE = 'youtube_trending_data'

"""Initialize the database connection."""
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
connection = None

def connect():
    """Establish a connection to the Supabase database."""
    global connection
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        logger.info("Database connection established successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        connection = None

def execute_query(query, params=None, fetch="all"):
    """
    Execute a SQL query with optional parameters.

    :param query: SQL query to execute.
    :param params: Tuple of parameters for the query.
    :param fetch: Mode for fetching results - "all" (default), "one", or None for no fetching.
    :return: Query result if fetching is enabled, else None.
    """
    global connection  

    if not query:
        logger.error("Query is empty. Aborting execution.")
        return None  

    if connection is None:
        logger.warning("Database connection is not initialized. Attempting to reconnect.")
        connect()
        if connection is None:
            logger.error("Failed to establish database connection.")
            return None

    try:
        with connection.cursor() as cursor:
            logger.info(f"Executing query: {query} with params: {params}")  
            cursor.execute(query, params if params else ())
            # Fetch results if required
            if fetch == "all":
                result = cursor.fetchall()
                logger.info(f"Fetched {len(result)} rows.")
            elif fetch == "one":
                result = cursor.fetchone()
                logger.info("Fetched 1 row.")
            else:
                result = None

            # Commit only if it's not a SELECT query
            if not query.strip().lower().startswith("select"):
                connection.commit()
                logger.info("Committed successfully.")

            return result
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return None
    finally:
        if cursor:
            cursor.close()

def table_exists(table_name: str) -> bool:
    """Check if a table exists in the database."""
    query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);"
    result = execute_query(query, (table_name.lower(),), fetch=True)
    return result[0][0] if result else False

def create_table_from_dataframe(df: pd.DataFrame, table_name: str):
    """Dynamically create a table based on the DataFrame columns if it doesn't exist."""
    if connection is None:
        connect()

    if table_exists(table_name):
        logger.warning(f"Table '{table_name}' already exists.")
        return

    column_definitions = []
    for column in df.columns:
        dtype = str(df[column].dtype)
        if "int" in dtype:
            pg_type = "BIGINT"
        elif "float" in dtype:
            pg_type = "DOUBLE PRECISION"
        elif "bool" in dtype:
            pg_type = "BOOLEAN"
        elif "datetime" in dtype:
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"  # Default to TEXT for strings
        
        column_definitions.append(f"{column} {pg_type}")

    create_query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)});"

    try:
        execute_query(create_query)
        logger.info(f"Table '{table_name}' created successfully.")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")

def store_dataframe(df: pd.DataFrame, table_name: str):
    """Store a Pandas DataFrame into a Supabase PostgreSQL table."""
    try:
        if connection is None:
            connect()

        create_table_from_dataframe(df, table_name)

        with connection.cursor() as cursor:
            # Generate column names dynamically
            columns = ', '.join(df.columns)
            values = ', '.join(['%s' for _ in df.columns])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

            # Insert all rows in bulk
            psycopg2.extras.execute_batch(cursor, insert_query, df.values)

        connection.commit()
        logger.info(f"Data inserted successfully into '{table_name}'!")

    except Exception as e:
        logger.error(f"Failed to insert data: {e}")

def close():
    """Close the database connection."""
    if connection:
        connection.close()
        logger.info("Connection closed.")

# if __name__ == "__main__":
#     # db = Database()
#     connect()
#     query ="""
#         SELECT * 
#         FROM youtube_trending_data
#         ORDER BY likes DESC
        # LIMIT 1;"""
#     # Load dataset and insert into database
#     # df = get_dataset()
#     # store_dataframe(df, "youtube_trending_data")
    # result = execute_query(query)
    # print(result)

    # Close connection
    # close()