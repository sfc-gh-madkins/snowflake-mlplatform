import os
import uuid
from datetime import datetime

import pytest
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


# Create a unique database/schema for each test run
DATABASE = f"SNOWFLAKE_MLPLATFORM_TEST_{uuid.uuid4().hex[:8].upper()}"
SCHEMA = f"SNOWFLAKE_MLPLATFORM_TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8].upper()}"

def get_connection_parameters():
    passphrase = os.environ.get("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_PASSPHRASE")
    private_key = os.environ.get("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_RSAKEY")
    if private_key is None:
        raise ValueError("No private key found in environment variables")
    
    # Convert the key to bytes
    private_key_bytes = private_key.encode('utf-8')
    
    private_key_obj = serialization.load_pem_private_key(
            private_key_bytes,
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
    connection_params = {
        "ACCOUNT": os.environ.get("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_ACCOUNT"),
        "USER": os.environ.get("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_USER"),
        "PASSWORD": os.environ.get("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_PASSWORD"),
        "ROLE": os.environ.get("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_ROLE"),
        "WAREHOUSE": os.environ.get("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_WAREHOUSE"),
        "private_key": private_key_obj,
    }
    assert all(connection_params.values()), "Please set all the required environment variables for Snowflake connection."
    return connection_params


def get_session_path():
    if "SNOWFLAKE_DATABASE" in os.environ and "SNOWFLAKE_SCHEMA" in os.environ:
        database = os.environ.get("SNOWFLAKE_DATABASE")
        schema = os.environ.get("SNOWFLAKE_SCHEMA")
        env_flag = True
    else:
        database = DATABASE
        schema = SCHEMA
        env_flag = False
    return database, schema, env_flag


def pytest_addoption(parser):
    parser.addoption("--snowflake-session", action="store", default="live")


def pytest_sessionstart(session):
    print("Pytest session is starting...")
    if session.config.getoption("--snowflake-session") != "local":
        session = Session.builder.configs(get_connection_parameters()).create()

        database, schema, env_flag = get_session_path()
        if env_flag:
            session.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
            session.sql(f"USE DATABASE {database}").collect()
            session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}").collect()
        else:
            session.sql(f"CREATE DATABASE {database}").collect()
            session.sql(f"CREATE SCHEMA {schema}").collect()



def pytest_sessionfinish(session, exitstatus):
    print(f"Pytest session finished with exit status: {exitstatus}")
    if session.config.getoption("--snowflake-session") != "local":
        session = Session.builder.configs(get_connection_parameters()).create()

        database, _, _ = get_session_path()
        session.sql(f"DROP DATABASE {database}").collect()


@pytest.fixture(scope="module")
def session(request) -> Session:
    if request.config.getoption("--snowflake-session") == "local":
        return Session.builder.config("local_testing", True).create()
    else:
        session = Session.builder.configs(get_connection_parameters()).create()

        database, schema, _ = get_session_path()
        session.sql(f"USE SCHEMA {database}.{schema}").collect()
        return session
