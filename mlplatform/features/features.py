from snowflake.snowpark import Session

from mlplatform.features.airport_weather.airport_weather_V1 import (
    airport_weather_feature_view_V1,
)
from mlplatform.features.airport_weather.airport_weather_V2 import (
    airport_weather_feature_view_V2,
)
from mlplatform.features.plane_attributes.plane_attributes_V1 import (
    plane_attributes_feature_view_V1,
)


def feature_views(session: Session):
    feature_views = [
        airport_weather_feature_view_V1(
            airport_weather_df=session.table("AIRPORT_WEATHER_STATION"),
            # refresh_freq='1 minute'
        ),
        airport_weather_feature_view_V2(
            airport_weather_df=session.table("AIRPORT_WEATHER_STATION"),
            # refresh_freq='1 minute'
        ),
        plane_attributes_feature_view_V1(
            plane_attributes_df=session.table("PLANE_MODEL_ATTRIBUTES"),
            overwrite=True,
        ),
    ]

    return feature_views
