from pyspark.sql import functions as F
from evidently import DataDefinition, Dataset

from otus_mlops.internals.data.analysers import EvidentlyDataAnalyser
from otus_mlops.internals.data.analysers import SparkCustomDataAnalyser
# from otus_mlops.internals.data.analysers._tf import TFDataAnalyser
# from otus_mlops.internals.data.loaders._spark_raw import FraudRawDataLoader
from otus_mlops.internals.data.loaders import SparkRawDataLoader
from otus_mlops.internals.interfaces import LoadingMethod
import pandas as pd


def analyse():
    print("init analysers")
    spark_data_analyser = SparkCustomDataAnalyser(target_columns=["tx_fraud", "tx_fraud_scenario"])
    evidently_data_analyser = EvidentlyDataAnalyser()

    print("init loader")
    data_loader = SparkRawDataLoader()

    print("Data Schema")
    # TODO: setup from cfg
    schema = DataDefinition(
    numerical_columns=["transaction_id", "customer_id", "terminal_id", "tx_amount", "tx_time_seconds", "tx_time_days", "tx_fraud", "tx_fraud_scenario"],
    )
    print(schema)

    print("load data")
    dataset = data_loader.load()
    print(dataset.schema)

    print("analyse")
    res = spark_data_analyser.analyse(dataset)

    print("base statistics")
    print(len(res.base_stats))
    print(res.base_stats.head(n=10).to_string(index=False))

    # print("categorical statistics")
    # print(len(res.categorical_stats))
    # print(res.categorical_stats.head(n=10).to_string(index=False))

    print("Feature correlations")
    print(res.correlations)

    print("Targets")
    for target in res.targets:
        print(res.targets[target].head(10).to_string(index=False))

    pandas_data = dataset.limit(10000).withColumn("tx_datetime", F.col("tx_datetime").cast('string')).toPandas()
    pandas_data["tx_datetime"] = pd.to_datetime(pandas_data["tx_datetime"], errors='coerce')
    print(pandas_data.head(n=5).to_string(index=False))
    evidently_data_analyser.analyse(dataset=Dataset.from_pandas(data=pandas_data, data_definition=schema))

    print("Finished.")



if __name__ == "__main__":
    analyse()