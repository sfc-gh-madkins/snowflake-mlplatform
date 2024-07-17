from datetime import datetime, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from snowflake.snowpark import Row, Session

from mlplatform.features.airport_weather.airport_weather_V1 import (
    airport_weather_transform_V1,
)


@pytest.mark.skipif(
    condition="config.getvalue('--snowflake-session') == 'local'",
    reason="Test case disabled for local testing due to Snowpark limitation",
)
@pytest.mark.unit
def test_airport_weather_transform_V1(session: Session):
    np.random.seed(42)
    df = pd.DataFrame(
        {
            "DATETIME_UTC": np.repeat(
                [
                    datetime(2023, 1, 1, 0, 0, 0) + timedelta(minutes=i)
                    for i in range(100)
                ],
                2,
            ),
            "AIRPORT_ZIP_CODE": np.tile(["12345", "67890"], 100),
            "RAIN_MM_H": np.random.randint(1, 11, 200),
        }
    )

    actual_spdf = airport_weather_transform_V1(session.create_dataframe(df))

    expected_spdf = session.create_dataframe(
        [
            Row(
                DATETIME_UTC=datetime(2023, 1, 1, 1, 35),
                AIRPORT_ZIP_CODE="67890",
                RAIN_MM_H_AVG_30=Decimal("5.033"),
                RAIN_MM_H_AVG_60=Decimal("5.550"),
            ),
            Row(
                DATETIME_UTC=datetime(2023, 1, 1, 1, 36),
                AIRPORT_ZIP_CODE="67890",
                RAIN_MM_H_AVG_30=Decimal("4.866"),
                RAIN_MM_H_AVG_60=Decimal("5.483"),
            ),
            Row(
                DATETIME_UTC=datetime(2023, 1, 1, 1, 37),
                AIRPORT_ZIP_CODE="67890",
                RAIN_MM_H_AVG_30=Decimal("4.800"),
                RAIN_MM_H_AVG_60=Decimal("5.450"),
            ),
            Row(
                DATETIME_UTC=datetime(2023, 1, 1, 1, 38),
                AIRPORT_ZIP_CODE="67890",
                RAIN_MM_H_AVG_30=Decimal("4.766"),
                RAIN_MM_H_AVG_60=Decimal("5.366"),
            ),
            Row(
                DATETIME_UTC=datetime(2023, 1, 1, 1, 39),
                AIRPORT_ZIP_CODE="67890",
                RAIN_MM_H_AVG_30=Decimal("4.833"),
                RAIN_MM_H_AVG_60=Decimal("5.316"),
            ),
        ]
    )

    assert actual_spdf.collect()[-5:] == expected_spdf.collect()
