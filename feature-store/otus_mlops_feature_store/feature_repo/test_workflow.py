from pathlib import Path
import subprocess
from datetime import datetime

import pandas as pd

from feast import FeatureStore
from feast.data_source import PushMode


def update_data_source():
    productivity_source_path = Path("data").joinpath("driver_stats_with_productivity.parquet")
    if not productivity_source_path.exists():
        driver_stats_path = productivity_source_path.parent.joinpath("driver_stats.parquet")
        driver_stats_df = pd.read_parquet(driver_stats_path)

        driver_stats_df["productivity"] = driver_stats_df["conv_rate"] * driver_stats_df["acc_rate"] * driver_stats_df["avg_daily_trips"]

        driver_stats_df.to_parquet(productivity_source_path)

def run_demo():
    store = FeatureStore(repo_path=".")

    print("\n--- Create entity ---")
    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001, 1002, 1003],
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 8, 12, 10),
                datetime(2021, 4, 12, 16, 40, 26),
            ],
            "conv_rate_coef": [2.5, 0.3, 2],
            "acc_rate_coef": [1.2, 0.7, 15],
        }
    )
    print(entity_df.head())

    print("\n--- Historical features ---")
    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_quality_metrics:conv_rate",
            "driver_quality_metrics:acc_rate",
            "driver_activity_rates_linear_combination_fv:linear_combination",
            "driver_productivity_metrics:productivity"
        ],
    ).to_df()
    print(training_df.head())

    online_features = store.get_online_features(
    features=[
        "driver_quality_metrics:conv_rate",
        "driver_quality_metrics:acc_rate",
        "driver_activity_rates_linear_combination_fv:linear_combination",
        "driver_productivity_metrics:productivity"
    ],
    entity_rows=[{"driver_id": 1001, "conv_rate_coef": 10.1, "acc_rate_coef": 1.01}, {"driver_id": 1002, "conv_rate_coef": 12.05, "acc_rate_coef": 0.76}],
    ).to_dict()

    print("Online features for drivers 1001, 1002:")
    for key, value in online_features.items():
        print(f"{key}: {value}")

    feature_view_names = ["driver_quality_metrics", "driver_productivity_metrics"]
    for feature_view_name in feature_view_names:
        feature_view = store.get_feature_view(feature_view_name)
        print("\nFeature view metadata:")
        print(f"Name: {feature_view.name}")
        print(f"Entities: {feature_view.entities}")
        print(f"TTL: {feature_view.ttl}")
        print(f"Online: {feature_view.online}")
        print(f"Features: {[f.name for f in feature_view.features]}")


if __name__ == "__main__":
    update_data_source()
    run_demo()
