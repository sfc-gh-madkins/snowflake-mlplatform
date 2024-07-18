from typing import Optional

from snowflake.ml.feature_store import FeatureView  # type: ignore
from snowflake.snowpark import DataFrame

from mlplatform.features.entities import plane_entity


def plane_attributes_feature_view_V1(
    plane_attributes_df: DataFrame,
    refresh_freq: Optional[str] = None,
    overwrite: Optional[bool] = False,
) -> dict:
    plane_attributes_fv = FeatureView(
        name="PLANE",
        feature_df=plane_attributes_df,
        entities=[plane_entity],
        refresh_freq=refresh_freq,
    )
    plane_attributes_fv_dict = {
        "feature_view": plane_attributes_fv,
        "version": "V1",
        "overwrite": overwrite,
    }
    return plane_attributes_fv_dict
