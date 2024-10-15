from snowflake.snowpark import Session
from snowflake.ml.dataset import load_dataset

from mlplatform.models.airplane_flight_delay.airplane_flight_delay_V1 import (
    airplane_flight_delay_model_V1,
)
from mlplatform.models.airplane_flight_delay.airplane_flight_delay_V2 import (
    airplane_flight_delay_model_V2,
)


def models(session: Session):
    models = [
        airplane_flight_delay_model_V1(load_dataset(session, "US_FLIGHT_DELAYS", "V1")),
        airplane_flight_delay_model_V2(load_dataset(session, "US_FLIGHT_DELAYS", "V2")),
        airplane_flight_delay_model_V2(load_dataset(session, "US_FLIGHT_DELAYS", "V3")),
    ]
    return models
