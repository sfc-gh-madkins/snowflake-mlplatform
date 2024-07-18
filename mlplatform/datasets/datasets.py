from snowflake.snowpark import Session
from snowflake.ml.feature_store import FeatureStore

from mlplatform.datasets.airplane_flight_delay.airplane_flight_delay_V1 import (
    airplane_flight_delay_dataset_V1,
)


def datasets(session: Session):
    fs = FeatureStore(
        session=session,
        database=session.get_current_database(),
        name=session.get_current_schema(),
        default_warehouse=session.get_current_warehouse(),
    )

    weather_fv_V1 = fs.get_feature_view(name="AIRPORT_WEATHER", version="V1")
    weather_fv_V2 = fs.get_feature_view(name="AIRPORT_WEATHER", version="V2")
    plane_fv_V1 = fs.get_feature_view(name="PLANE", version="V1")

    datasets = [
        airplane_flight_delay_dataset_V1(
            session.sql("""
            SELECT
                SCHEDULED_DEPARTURE_UTC, AIRPORT_ZIP_CODE, PLANE_MODEL, DEPARTING_DELAY,
                TICKETS_SOLD,
            FROM
                US_FLIGHT_SCHEDULES
            """),
            [weather_fv_V1, plane_fv_V1], "V1"
        ),
        airplane_flight_delay_dataset_V1(
            session.sql("""
            SELECT
                SCHEDULED_DEPARTURE_UTC, AIRPORT_ZIP_CODE, PLANE_MODEL, DEPARTING_DELAY,
                TICKETS_SOLD,
            FROM
                US_FLIGHT_SCHEDULES
            """),
            [weather_fv_V2, plane_fv_V1], "V2"
        ),
    ]
    return datasets
