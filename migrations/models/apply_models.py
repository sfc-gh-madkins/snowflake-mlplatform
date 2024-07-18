import ast
import os

from snowflake.snowpark import Session
from snowflake.ml.registry import Registry

from mlplatform.models.models import models


def apply_model_changes(session: Session):
    mr = Registry(
        session=session,
        database_name=session.get_current_database(),
        schema_name=session.get_current_schema(),
    )

    # Incrementally add new models
    existing_models = session.sql(
        f"SHOW MODELS IN {session.get_current_database()}.{session.get_current_schema()}"
    ).collect()
    existing_model_versions = []
    for existing_model in existing_models:
        versions = ast.literal_eval(existing_model.versions)
        for version in versions:
            existing_model_versions.append(f"{existing_model.name}:{version}")

    for model in models(session):
        target_model_version = f"{model['model_name']}:{model['version_name']}"
        if target_model_version not in existing_model_versions:
            mr.log_model(**model)


if __name__ == "__main__":

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
    apply_model_changes(session)
