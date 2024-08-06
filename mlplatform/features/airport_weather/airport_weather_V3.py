from typing import Optional

from snowflake.ml.feature_store import FeatureView  # type: ignore
from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import col

from mlplatform.features.entities import airport_entity


def airport_weather_transform_V3(airport_weather_df: DataFrame):
    """
    Calculate the average rain in the last 30, 60 and 120 minutes for each airport
    """

    transformed_airport_weather_df = airport_weather_df.analytics.moving_agg(
        aggs={"RAIN_MM_H": ["AVG"]},
        window_sizes=[30, 60, 120],
        group_by=["AIRPORT_ZIP_CODE"],
        order_by=["AIRPORT_ZIP_CODE", "DATETIME_UTC"],
    ).select(
        "DATETIME_UTC",
        "AIRPORT_ZIP_CODE",
        col("RAIN_MM_H_AVG_30").as_("AVG30MIN_RAIN_MM_H"),
        col("RAIN_MM_H_AVG_60").as_("AVG60MIN_RAIN_MM_H"),
        col("RAIN_MM_H_AVG_120").as_("AVG120MIN_RAIN_MM_H"),
    )

    return transformed_airport_weather_df


def airport_weather_feature_view_V3(
    airport_weather_df: DataFrame,
    refresh_freq: Optional[str] = None,
    overwrite: Optional[bool] = False,
) -> dict:
    airport_weather_fv = FeatureView(
        name="AIRPORT_WEATHER",
        feature_df=airport_weather_transform_V3(airport_weather_df),
        timestamp_col="DATETIME_UTC",
        entities=[airport_entity],  # join_keys=["AIRPORT_ZIP_CODE"]
        refresh_freq=refresh_freq,
    )

    airport_weather_fv_dict = {
        "feature_view": airport_weather_fv,
        "version": "V3",
        "overwrite": overwrite,
    }
    return airport_weather_fv_dict
