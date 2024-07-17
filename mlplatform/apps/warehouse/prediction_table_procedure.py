from snowflake.ml.dataset import load_dataset
from snowflake.ml.feature_store import FeatureStore
from snowflake.ml.registry import Registry
from snowflake.snowpark import DataFrame, Session


def predict_tp(
    session: Session,
    spine_table_name: str,
    model: str,
    dataset: str,
) -> DataFrame:
    model_name, model_version, model_function = model.split(":")
    dataset_name, dataset_version = dataset.split(":")

    fs = FeatureStore(
        session=session,
        database=session.get_current_database(),
        name=session.get_current_schema(),
        default_warehouse=session.get_current_warehouse(),
    )

    mr = Registry(
        session=session,
        database_name=session.get_current_database(),
        schema_name=session.get_current_schema(),
    )

    model_ref = mr.get_model(model_name).version(model_version)
    dataset_ref = load_dataset(session, name=dataset_name, version=dataset_version)
    # datase_ref = model.lineage(domain_filter=["dataset"], direction="upstream")[0]

    feature_df = fs.retrieve_feature_values(
        spine_df=session.table(spine_table_name),
        features=fs.load_feature_views_from_dataset(dataset_ref),
        spine_timestamp_col=dataset.selected_version._get_metadata().properties.spine_timestamp_col,
    )

    prediction_df = model_ref.run(feature_df, function_name=model_function)

    return prediction_df
