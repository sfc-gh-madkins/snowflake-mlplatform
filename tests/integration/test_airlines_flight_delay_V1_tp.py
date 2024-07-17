from datetime import datetime
import pytest
import uuid

from snowflake.ml.feature_store import CreationMode, FeatureStore
from snowflake.snowpark import Session
from snowflake.ml.registry import Registry

from mlplatform.features.entities import airport_entity, plane_entity
from mlplatform.features.airport_weather.airport_weather_V1 import (
    airport_weather_feature_view_V1,
)

from mlplatform.datasets.airplane_flight_delay.airplane_flight_delay_V1 import (
    airplane_flight_delay_dataset_V1,
)
from mlplatform.models.airplane_flight_delay.airplane_flight_delay_V1 import (
    airplane_flight_delay_model_V1,
)
from mlplatform.apps.warehouse.prediction_table_procedure import predict_tp


DATASET = "US_FLIGHT_DELAYS:V1"
MODEL = "AIRLINE_FLIGHT_DELAY_MODEL:V1:predict"
SPINE_TABLE_NAME = f"SPINE_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:10].upper()}"

@pytest.mark.integration
def test_airlines_flight_delay_V1_tp_hold(session: Session):
    assert True

#@pytest.mark.integration
def test_airlines_flight_delay_V1_tp(session: Session):

    session.create_dataframe().write.mode("overwrite").save_as_table(SPINE_TABLE_NAME, table_type="transient")

    actual_df_predict = predict_tp(
        session,
        spine_table_name=SPINE_TABLE_NAME,
        model=MODEL,
        dataset=DATASET,
    )

    actual_df_predict_proba = predict_tp(
        session,
        spine_table_name=SPINE_TABLE_NAME,
        model=MODEL,
        dataset=DATASET,
    )

    expected_df_predict = session.create_dataframe()
    expected_df_predict_proba = session.create_dataframe()
    assert expected_df_predict == actual_df_predict
    assert expected_df_predict_proba == actual_df_predict_proba
