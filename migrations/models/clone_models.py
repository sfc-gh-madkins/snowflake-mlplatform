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

    connection_parameters = {
        "ACCOUNT": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_ACCOUNT"),
        "USER": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_USER"),
        "PASSWORD": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_PASSWORD"),
        "ROLE": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_ROLE"),
        "WAREHOUSE": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_WAREHOUSE"),
        "DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
    }

    session = Session.builder.configs(connection_parameters).create()
    clone_models(session, args.prod_database, args.prod_schema)
