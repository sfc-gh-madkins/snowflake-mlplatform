import ast
import os

from snowflake.ml.feature_store import CreationMode, FeatureStore
from snowflake.snowpark import Session

from mlplatform.datasets.datasets import datasets


def apply_dataset_changes(session: Session):

    fs = FeatureStore(
        session=session,
        database=session.get_current_database(),
        name=session.get_current_schema(),
        default_warehouse=session.get_current_warehouse(),
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    # Incrementally add new datasets
    existing_datasets = session.sql(
        f"SHOW DATASETS IN {session.get_current_database()}.{session.get_current_schema()}"
    ).collect()
    existing_dataset_versions = []
    for existing_dataset in existing_datasets:
        versions = ast.literal_eval(existing_dataset.versions)
        for version in versions:
            existing_dataset_versions.append(f"{existing_dataset.name}:{version}")

    for dataset in datasets(session):
        target_datset_version = f"{dataset['name']}:{dataset['version']}"
        if target_datset_version not in existing_dataset_versions:
            fs.generate_dataset(**dataset)


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
    apply_dataset_changes(session)
