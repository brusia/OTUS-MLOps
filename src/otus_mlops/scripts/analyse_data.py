from pyspark.sql import functions as F
from evidently import DataDefinition, Dataset, BinaryClassification, MulticlassClassification

from otus_mlops.internals.data.analysers import EvidentlyDataAnalyser, EVIDENTLY_REPORT_NAME
from otus_mlops.internals.data.analysers import SparkCustomDataAnalyser
from otus_mlops.internals.interfaces import LoadingMethod, REPORT_PATH
import pandas as pd

import json
from otus_mlops.internals.data.loaders._spark_raw import SparkRawDataLoader
from otus_mlops.remote.object_storage_client import ObjectStorageClient, BUCKET_NAME

def analyse():
    print("init loader")
    data_loader = SparkRawDataLoader()

    print("init analysers")
    spark_data_analyser = SparkCustomDataAnalyser(target_columns=["tx_fraud", "tx_fraud_scenario"])
    evidently_data_analyser = EvidentlyDataAnalyser()

    # print("init")
    storage_client = ObjectStorageClient()

    print("Data Schema")
    # TODO: setup from cfg, move to preprocessor for evidently
    schema = DataDefinition(
    numerical_columns=["transaction_id", "customer_id", "terminal_id", "tx_amount", "tx_time_seconds", "tx_time_days", "tx_fraud", "tx_fraud_scenario"],
    classification=[BinaryClassification(id="binary",target = "tx_fraud", prediction_labels = "prediction", prediction_probas = ["tx_amount",  "terminal_id"]),
                    MulticlassClassification(id="multi", target = "tx_fraud_scenario", prediction_labels = "prediction", prediction_probas = ["tx_amount",  "terminal_id"])])
    print(schema)

    print("load data")
    dataset = data_loader.load()
    print(dataset.schema)

    print("analyse")

    print(dataset.show(5))
    res = spark_data_analyser.analyse(dataset)

    
    print("base statistics")
    # print(len(res.base_stats))
    print(res.base_stats.show())

    # print("categorical statistics")
    # print(len(res.categorical_stats))
    # print(res.categorical_stats.head(n=10).to_string(index=False))

    print("Feature correlations")
    print(res.correlations)

    print("Targets")
    for target in res.targets:
        print(res.targets[target].head(10))

    pandas_data = dataset.limit(10000).withColumn("tx_datetime", F.col("tx_datetime").cast('string')).toPandas()
    pandas_data["tx_datetime"] = pd.to_datetime(pandas_data["tx_datetime"], errors='coerce')
    print(pandas_data.head(n=5).to_string(index=False))
    # print(pandas_data.isnull().sum())
    
    evidently_data_analyser.analyse(dataset=Dataset.from_pandas(data=pandas_data.iloc[:5000], data_definition=schema),
                                    ref=Dataset.from_pandas(data=pandas_data.iloc[5000:], data_definition=schema))

    print("uploading")
    data_loader.upload_data(res.base_stats, "data-analyse/base_statistics.csv")
    data_loader.upload_data(res.correlations, "data-analyse/correlations.csv")
    # data_loader.upload_data(res.base_stats, "data-analyse/base_statistics.csv")
    
    json.dumps(res.histo, "histo.json")
    json.dumps(res.targets, "targets.json")
    storage_client.upload_file("histo.json", f"s3a://{BUCKET_NAME}/data-analyse/histo.json")
    storage_client.upload_file("targets.json", f"s3a://{BUCKET_NAME}/data-analyse/targets.json")
    storage_client.upload_file(REPORT_PATH.absolute().joinpath(EVIDENTLY_REPORT_NAME), f"s3a://{BUCKET_NAME}/data-analyse/{EVIDENTLY_REPORT_NAME}")
    
    print("Finished.")



if __name__ == "__main__":
    analyse()