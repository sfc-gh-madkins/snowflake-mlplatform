import argparse
import ast
import os

from snowflake.snowpark import Session


def clone_models(session: Session, prod_database: str, prod_schema: str):
    model_path = f"{prod_database}.{prod_schema}"
    existing_models = session.sql(f"SHOW MODELS IN {model_path}").collect()

    existing_model_versions = []
    for existing_model in existing_models:
        versions = ast.literal_eval(existing_model.versions)
        for version in versions:
            existing_model_versions.append((f"{existing_model.name}", f"{version}"))

    for model, version in existing_model_versions:
        session.sql(
            f"CREATE MODEL {model} WITH VERSION {version} FROM MODEL {model_path}.{model} VERSION {version}"
        ).collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--prod_database", required=True, help="")
    parser.add_argument("--prod_schema", required=True, help="")
    args = parser.parse_args()
    
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
    connection_parameters = {
        "ACCOUNT": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_ACCOUNT"),
        "USER": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_USER"),
      #  "PASSWORD": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_PASSWORD"),
        "ROLE": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_ROLE"),
        "WAREHOUSE": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_WAREHOUSE"),
        "DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        "private_key": private_key_obj,
    }

    session = Session.builder.configs(connection_parameters).create()
    clone_models(session, args.prod_database, args.prod_schema)
