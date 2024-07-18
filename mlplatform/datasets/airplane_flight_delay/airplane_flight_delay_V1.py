from typing import List, Literal, Optional

from snowflake.ml.feature_store import FeatureView
from snowflake.snowpark import DataFrame


def airplane_flight_delay_dataset_V1(
    spine_df: DataFrame,
    feature_views: List[FeatureView],
    version: str,
    exclude_columns: Optional[List[str]] = None,
    include_feature_view_timestamp_col: bool = False,
    desc: str = "",
    output_type: Literal["dataset"] = "dataset",
) -> dict:
    assert True  # confirm columns exist in spine_df
    dataset_dict = {
        "name": "US_FLIGHT_DELAYS",
        "spine_timestamp_col": "SCHEDULED_DEPARTURE_UTC",
        "spine_label_cols": ["DEPARTING_DELAY"],
        "spine_df": spine_df,
        "version": version,
        "features": feature_views,
        "exclude_columns": exclude_columns,
        "include_feature_view_timestamp_col": include_feature_view_timestamp_col,
        "desc": desc,
        "output_type": output_type,
    }
    return dataset_dict
