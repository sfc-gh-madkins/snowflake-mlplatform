import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder

from snowflake.ml.dataset import Dataset


def airplane_flight_delay_pipeline(X: pd.DataFrame, y: pd.Series):

    def datetime_transforms(feature: pd.DataFrame):
        datetime_series = pd.to_datetime(feature.iloc[:, 0])

        hours = datetime_series.dt.hour
        days_of_week = datetime_series.dt.dayofweek  # Monday=0, Sunday=6

        # Combine into a DataFrame
        transformed_df = pd.DataFrame({"hour": hours, "day_of_week": days_of_week})
        return transformed_df


    numeric_features = [
        "TICKETS_SOLD",
        "AVG30MIN_RAIN_MM_H",
        "AVG60MIN_RAIN_MM_H",
        "AVG120MIN_RAIN_MM_H",
        "SEATING_CAPACITY",
    ]
    categorical_features = ["PLANE_MODEL"]
    time_feature = ["SCHEDULED_DEPARTURE_UTC"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat_ohe", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            (
                "num",
                SimpleImputer(strategy="constant", fill_value=-1),
                numeric_features,
            ),
            (
                "datetime_transforms",
                FunctionTransformer(datetime_transforms, validate=False),
                time_feature,
            ),
        ],
    )

    model = Pipeline(
        steps=[("preprocessor", preprocessor), ("classifier", GradientBoostingClassifier())]
    )

    model.fit(X, y)

    return model


def airplane_flight_delay_model_V2(dataset: Dataset) -> dict:
    X = dataset.read.to_pandas()
    y = X.pop(dataset.selected_version.label_cols[0])

    model = airplane_flight_delay_pipeline(X, y)

    model_dict = {
        "model": model,
        "model_name": "AIRLINE_FLIGHT_DELAY_MODEL",
        "version_name": "V2",
        "conda_dependencies": ["snowflake-ml-python"],
        "sample_input_data": X.head(),
    }
    return model_dict
