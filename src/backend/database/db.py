import os
import pandas as pd
import psycopg2
from psycopg2 import sql,extras
from dotenv import load_dotenv
from src.data.dataset import get_dataset
from src.backend.utils.logger import get_logger

logger = get_logger()

# Load environment variables from .env
load_dotenv()

class Database:
    def __init__(self):
        """Initialize the database connection."""
        self.USER = os.getenv("user")
        self.PASSWORD = os.getenv("password")
        self.HOST = os.getenv("host")
        self.PORT = os.getenv("port")
        self.DBNAME = os.getenv("dbname")
        self.connection = None

    def connect(self):
        """Establish a connection to the Supabase database."""
        try:
            self.connection = psycopg2.connect(
                user=self.USER,
                password=self.PASSWORD,
                host=self.HOST,
                port=self.PORT,
                dbname=self.DBNAME
            )
            logger.info("Connection successful!")
        except Exception as e:
            logger.info(f"Failed to connect: {e}")

    def execute_query(self, query, params=None, fetch=False):
        """Execute a query with optional parameters."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchall()
                self.connection.commit()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);"
        result = self.execute_query(query, (table_name.lower(),), fetch=True)
        return result[0][0] if result else False

    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str):
        """Dynamically create a table based on the DataFrame columns if it doesn't exist."""
        if self.connection is None:
            self.connect()

        if self.table_exists(table_name):
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
            self.execute_query(create_query)
            logger.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")

    def store_dataframe(self, df: pd.DataFrame, table_name: str):
        """Store a Pandas DataFrame into a Supabase PostgreSQL table."""
        try:
            if self.connection is None:
                self.connect()

            self.create_table_from_dataframe(df, table_name)

            with self.connection.cursor() as cursor:
                # Generate column names dynamically
                columns = ', '.join(df.columns)
                values = ', '.join(['%s' for _ in df.columns])
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

                # Insert all rows in bulk
                psycopg2.extras.execute_batch(cursor, insert_query, df.values)

            self.connection.commit()
            logger.info(f"Data inserted successfully into '{table_name}'!")

        except Exception as e:
            logger.error(f"Failed to insert data: {e}")

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Connection closed.")

# if __name__ == "__main__":
    # db = Database()
    # db.connect()

    # Load dataset and insert into database
    # df = get_dataset()
    # db.store_dataframe(df, "youtube_trending_data")

    # Close connection
    # db.close()