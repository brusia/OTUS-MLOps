from pathlib import Path
from typing import Final, Any
from pyspark.sql import functions as F
from evidently import DataDefinition, Dataset, BinaryClassification, MulticlassClassification
import boto3
from otus_mlops.internals.data.analysers import (
    EvidentlyDataAnalyser,
    EVIDENTLY_REPORT_NAME,
    SparkBaseStatisticsDataAnalyser,
    RupturesDataAnalyser,
)
from otus_mlops.internals.data.analysers import StatisticalTestsDataAnalyser
from otus_mlops.internals.data.analysers.evidently import (
    REPORT_EXT as EVIDENTLY_REPORT_EXT,
    SPLIT_PARTS_REPORT_NAME as EVIDENTLY_REPORT_NAME,
    WHOLE_DATA_REPORT_NAME as EVIDENTLY_FULL_REPORT_NAME
)
from otus_mlops.internals.interfaces import LoadingMethod, REPORTS_PATH
import pandas as pd

import json
import logging
from otus_mlops.internals.data.loaders._spark_raw import SparkRawDataLoader
from otus_mlops.internals.interfaces import IDataAnalyser

_logger = logging.getLogger(__name__)

BUCKET_NAME: Final[str] = "brusia-bucket"
RAW_STATISTICS_FOLDER_NAME: Final[str] = "raw-statistics"
BASE_STATISTICS_FILE_NAME: Final[Path] = Path("base_statistics.json")
CORRELATION_MATRIX_FILE_NAME: Final[Path] = Path("correlations.csv")
HISTO_FILE_NAME: Final[Path] = Path("histo.csv")


def analyse():
    def _upload_file(s3: Any, file_name: Path):
        s3.upload_file(
            Filename=file_name.as_posix(),
            Bucket=BUCKET_NAME,
            Key=file_name.as_posix(),
        )

    _logger.info("Initialize loader.")
    with SparkRawDataLoader() as data_loader:
        _logger.info("Initialize data analysers.")
        base_data_analyser = SparkBaseStatisticsDataAnalyser()

        evidently_data_analyser = EvidentlyDataAnalyser()
        ruptures_data_analyser = RupturesDataAnalyser()
        statistical_data_analyser = StatisticalTestsDataAnalyser()

        print("load data")
        dataset = data_loader.load()
        print(dataset.schema)

        print("analyse")

        print(dataset.show(5))
        whole_data_base_stats = base_data_analyser.analyse(dataset)

        # Choose data splitted reference and test data (70% for reference, 30% for split)
        min_date = F.unix_timestamp("tx_datetime").agg({"tx_datetime": "min"}).collect()[0][0]
        max_date = F.unix_timestamp("tx_datetime").agg({"tx_datetime": "max"}).collect()[0][0]
        splitted_date = F.from_unixtime(min_date + (max_date - min_date) * 0.7).cast("timestamp")

        ref_data = dataset.filter(F.col("tx_datetime") < splitted_date)
        test_data = dataset.filter(F.col("tx_datetime") >= splitted_date)

        # chech for debug
        print("ref data datas")
        ref_data.agg(F.min("tx_datetime").alias("min_date"), 
                    F.max("tx_datetime").alias("max_date")).show()
        
        print("test data datas")
        test_data.agg(F.min("tx_datetime").alias("min_date"), 
                    F.max("tx_datetime").alias("max_date")).show()

        ref_data_statistics = base_data_analyser.analyse(ref_data)
        test_data_statistics = base_data_analyser.analyse(test_data)

        print("base statistics")
        print(whole_data_base_stats.base_stats)

        print("ref statistics")
        print(ref_data_statistics.base_stats)

        print("test statistics")
        print(test_data_statistics.base_stats)

        print("Feature correlations (whole)")
        print(whole_data_base_stats.correlations)

        print("ref correlations (whole)")
        print(ref_data_statistics.correlations)

        print("test correlations (whole)")
        print(test_data_statistics.correlations)

        # pandas_data = dataset.limit(10000).withColumn("tx_datetime", F.col("tx_datetime").cast('string')).toPandas()
        # pandas_data["tx_datetime"] = pd.to_datetime(pandas_data["tx_datetime"], errors='coerce')
        # print(pandas_data.head(n=5).to_string(index=False))

        # _logger.info("Run statistics tests for splitted parts (whole dataset).")
        # for feature_name in test_data:
        #     statistical_data_analyser.analyse(test_data[feature_name], ref_data[feature_name], feature_name)

        ref_data_sampled_pandas: pd.DataFrame = ref_data.sample(n=10000, random_state=42).toPandas()
        test_data_sampled_pandas: pd.DataFrame = test_data.sample(n=1000, random_state=42).toPandas()

        whole_data_samples_pandas: pd.DataFrame = dataset.sample(n=100000, random_state=42).toPandas()

        schema = DataDefinition(
        numerical_columns=[
            "transaction_id",
            "customer_id",
            "terminal_id",
            "tx_amount",
            "tx_time_seconds",
            "tx_time_days",
            "tx_fraud",
            "tx_fraud_scenario",
        ],
        classification=[  # BinaryClassification(id="binary",target = "tx_fraud", prediction_labels = "prediction", prediction_probas = ["tx_amount",  "terminal_id"]),
                MulticlassClassification(
                    id="multi",
                    target="tx_fraud_scenario",
                    prediction_labels="prediction",
                    prediction_probas=["tx_amount", "terminal_id"],
                )
            ],
        )

        _logger.info("Run evdently analyser for whole dataset.")
        evidently_data_analyser.analyse(
            dataset=Dataset.from_pandas(data=whole_data_samples_pandas, data_definition=schema)
        )

        _logger.info("Run evdently analyser for splitted parts.")
        evidently_data_analyser.analyse(
            dataset=Dataset.from_pandas(data=test_data_sampled_pandas, data_definition=schema),
            ref=Dataset.from_pandas(data=ref_data_sampled_pandas, data_definition=schema),
        )

        _logger.info("Run ruptures analyser.")
        for feature_name in dataset.columns:
            ruptures_data_analyser.analyse(whole_data_samples_pandas[[feature_name]], feature_name=feature_name)

        _logger.info("Uploading results.")
        data_loader.upload_data(
            whole_data_base_stats.correlations,
            REPORTS_PATH.joinpath(RAW_STATISTICS_FOLDER_NAME, CORRELATION_MATRIX_FILE_NAME).as_posix(),
        )

        session = boto3.session.Session()
        s3 = session.client(service_name="s3", endpoint_url="https://storage.yandexcloud.net")

        # s3.put_object(
        #     Bucket=BUCKET_NAME,
        #     Key=REPORTS_PATH.joinpath(RAW_STATISTICS_FOLDER_NAME, HISTO_FILE_NAME).as_posix(),
        #     Body=json.dumps(whole_data_base_stats.histo),
        # )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=REPORTS_PATH.joinpath(RAW_STATISTICS_FOLDER_NAME, BASE_STATISTICS_FILE_NAME).as_posix(),
            Body=json.dumps(whole_data_base_stats.base_stats),
        )

        for item in REPORTS_PATH.rglob("*"):
            if item.is_file:
                _upload_file(s3, item.as_posix())

        _logger.info("Data analyse completed.")


if __name__ == "__main__":
    analyse()