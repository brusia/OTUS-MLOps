# This is an example feature definition file

from datetime import timedelta

import pandas as pd
import numpy as np

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    Project,
    PushSource,
    RequestSource,
    ValueType,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, UnixTimestamp, Int32

# Define a project for the feature repo
project = Project(name="otus_mlops_feature_store", description="A project for driver statistics")

# Define an entity for the driver. You can think of an entity as a primary key used to
# fetch features.
driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT32)

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        # Field(name="driver_id", dtype=Int32),
        Field(name="event_timestamp", dtype=UnixTimestamp),
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64, description="Average daily trips"),
    ],
    online=True,
    source=driver_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "driver_performance"},
)

# Defines a way to push data (to be available offline, online or both) into Feast.
driver_stats_push_source = PushSource(
    name="driver_stats_push_source",
    batch_source=driver_stats_source,
)


# создали feature view, фокусирующийся на качестве вождения
driver_quality_fv = FeatureView(
    name="driver_quality_metrics",
    entities=[driver],
    ttl=timedelta(days=2),
    schema=[
        Field(name="conv_rate", dtype=Float32, description="Conversion rate - percentage of accepted rides"),
        Field(name="acc_rate", dtype=Float32, description="Acceptance rate - percentage of ride requests accepted"),
    ],
    online=True,
    source=driver_stats_source,
    tags={
        "team": "driver_analytics", 
        "category": "quality",
    },
)

# будем принимать коэффициенты весов для conv_rate и acc_rate
linear_coef_request = RequestSource(
    name="linear_coefficients",
    schema=[
        Field(name="conv_rate_coef", dtype=Float64),
        Field(name="acc_rate_coef", dtype=Float64),
    ],
)


# вычисляемая на лету линейная комбинация признаков, использующая заданные значения коэффиицентов
@on_demand_feature_view(
    name="driver_activity_rates_linear_combination_fv",
    sources=[driver_quality_fv, linear_coef_request],
    schema=[
        Field(name="linear_combination", dtype=Float64),
    ],
)
def transformed_rates_linear_combination_fresh(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["linear_combination"] = inputs["conv_rate_coef"] * inputs["conv_rate"] + \
        inputs["acc_rate_coef"] * inputs["acc_rate"]
    return df

# сервис, работающий с созданным преобразованием
driver_activity_rates_linear = FeatureService(
    name="driver_activity_rates_linear_combination",
    features=[driver_quality_fv, transformed_rates_linear_combination_fresh],
)

# создали fileSource c заранее посчитанной продуктивностью
driver_productivity_source = FileSource(
    name="driver_hourly_stats_source_with_productivity",
    path="data/driver_stats_with_productivity.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# создали feature view для этого ресурса
driver_productivity_fv = FeatureView(
    name="driver_productivity_metrics",
    entities=[driver],
    ttl=timedelta(days=7),
    schema=[
        Field(name="productivity", dtype=Float32, description="Driver productivity from file source."),
    ],
    online=True,
    source=driver_productivity_source,
    tags={
        "team": "driver_analytics", 
        "category": "productivity",
    },
)

