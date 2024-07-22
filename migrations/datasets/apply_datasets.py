import ast
import os

from snowflake.ml.feature_store import CreationMode, FeatureStore
from snowflake.snowpark import Session

from mlplatform.datasets.datasets import datasets
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

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
        "PASSWORD": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_PASSWORD"),
        "ROLE": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_ROLE"),
        "WAREHOUSE": os.getenv("SNOWFLAKE_CONNECTIONS_SNOWCONNECTION_WAREHOUSE"),
        "DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        "private_key": private_key_obj,
    }

    session = Session.builder.configs(connection_parameters).create()
    apply_dataset_changes(session)
