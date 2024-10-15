import time
import os
import argparse

from snowflake.snowpark import Session
from snowflake.ml.data.data_connector import DataConnector
from implementations.ray_data_ingester import RayDataIngester


# Environment variables below will be automatically populated by Snowflake.
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Custom environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")


def get_login_token():
    with open("/snowflake/session/token", "r") as f:
        return f.read()


def get_connection_params():
    return {
        "account": SNOWFLAKE_ACCOUNT,
        "host": SNOWFLAKE_HOST,
        "authenticator": "oauth",
        "token": get_login_token(),
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA
    }


def run_job(num_rows):
    # Start a Snowflake session, run the query and write results to specified table
    with Session.builder.configs(get_connection_params()).create() as session:
        # Print out current session context information.
        database = session.get_current_database()
        schema = session.get_current_schema()
        warehouse = session.get_current_warehouse()
        role = session.get_current_role()
        print(
            f"Connection succeeded. Current session context: database={database}, schema={schema}, warehouse={warehouse}, role={role}"
        )

        df = session.sql(f"SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES LIMIT {num_rows}")
        dc = DataConnector.from_dataframe(df, ingestor_class=RayDataIngester)

        start_time = time.time()
        dcx = dc.to_pandas()
        end_time = time.time()

        elapsed_time = end_time - start_time
        print(f"dc.to_pandas() execution time: {elapsed_time:.2f} seconds")

        memory_in_gb = dcx.memory_usage(deep=True).sum() / (1024 ** 3)
        print(f"Memory usage: {memory_in_gb:.2f} GB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Snowflake Ray Data Ingestion Job')
    parser.add_argument('--num_rows', type=int, default=100000000, help='Number of rows to fetch from the table')

    args = parser.parse_args()

    run_job(args.num_rows)
