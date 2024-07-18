from typing_extensions import Optional

from snowflake.ml.feature_store import FeatureView  # type: ignore
from snowflake.snowpark import DataFrame, Window
from snowflake.snowpark.functions import avg, col

from mlplatform.features.entities import airport_entity


def airport_weather_transform_V1(airport_weather_df: DataFrame):
    """
    Calculate the average rain in the last 30 and 60 minutes for each airport
    """

    window = (
        Window.partition_by("AIRPORT_ZIP_CODE")
        .order_by("DATETIME_UTC")
        .rows_between(-29, Window.CURRENT_ROW)
    )
    window2 = (
        Window.partition_by("AIRPORT_ZIP_CODE")
        .order_by("DATETIME_UTC")
        .rows_between(-59, Window.CURRENT_ROW)
    )
    transformed_airport_weather_df = airport_weather_df.select(
        "DATETIME_UTC",
        "AIRPORT_ZIP_CODE",
        avg("RAIN_MM_H").over(window).alias("AVG30MIN_RAIN_MM_H"),
        avg("RAIN_MM_H").over(window2).alias("AVG60MIN_RAIN_MM_H"),
    ).sort(col("AIRPORT_ZIP_CODE"), col("DATETIME_UTC"))

    return transformed_airport_weather_df


def airport_weather_feature_view_V1(
    airport_weather_df: DataFrame,
    refresh_freq: Optional[str] = None,
    overwrite: Optional[bool] = False,
) -> dict:
    airport_weather_fv = FeatureView(
        name="AIRPORT_WEATHER",
        feature_df=airport_weather_transform_V1(airport_weather_df),
        timestamp_col="DATETIME_UTC",
        entities=[airport_entity],  # join_keys=["AIRPORT_ZIP_CODE"]
        refresh_freq=refresh_freq,
    )

    airport_weather_fv_dict = {
        "feature_view": airport_weather_fv,
        "version": "V1",
        "overwrite": overwrite,
    }
    return airport_weather_fv_dict
