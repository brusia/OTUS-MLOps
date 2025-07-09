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
from otus_mlops.internals.data.preprocess._fraud_data_preprocessor import FraudDataProcessor
from otus_mlops.internals.interfaces import LoadingMethod, REPORTS_PATH
import pandas as pd

import json
import logging
from otus_mlops.internals.data.loaders._spark_raw import SparkRawDataLoader
from otus_mlops.internals.interfaces import IDataAnalyser
from otus_mlops.internals.interfaces.base import BUCKET_NAME, NUMERICAL_COLUMNS, PREPROCESS_DATA_MODEL_PATH, S3_ENDPOINT_URL
from pyspark.sql import DataFrame as SparkDataFrame

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

# BUCKET_NAME: Final[str] = "brusia-bucket"
RAW_STATISTICS_FOLDER_NAME: Final[str] = "raw-statistics"
BASE_STATISTICS_FILE_NAME: Final[Path] = Path("base_statistics.json")
CORRELATION_MATRIX_FILE_NAME: Final[Path] = Path("correlations.csv")
# HISTO_FILE_NAME: Final[Path] = Path("histo.csv")
TIME_COLUMN_NAME: Final[str] = "tx_datetime"


TEST_DATA_SAMPLES_COUNT: Final[int] = 1000
REF_DATA_SAMPLE_COUNT: Final[int] = 10000
FULL_DATA_SAMPLES_COUNT: Final[int] = 100000

# TEST_DATA_SAMPLES_COUNT: Final[int] = 10
# REF_DATA_SAMPLE_COUNT: Final[int] = 100
# FULL_DATA_SAMPLES_COUNT: Final[int] = 100


ITERATIONS_COUNT: Final[int] = 10

def analyse(evidently: bool = False, ruptures: bool = False, statistics: bool = False):
    def _upload_file(s3: Any, file_name: Path):
        s3.upload_file(
            Filename=file_name.as_posix(),
            Bucket=BUCKET_NAME,
            Key=file_name.as_posix(),
        )

    def _convert_to_pandas(dataframe: SparkDataFrame, num_samples: int) -> pd.DataFrame:
        # print(dataframe.show(5))
        # print(num_samples)
        # print(TIME_COLUMN_NAME)
        # print("convert to pandas")
        pd_data_frame: pd.DataFrame = dataframe.limit(num_samples).withColumn(TIME_COLUMN_NAME, F.col(TIME_COLUMN_NAME).cast('string')).toPandas()
        # print("partly converted:")
        # print(pd_data_frame.head(n=5).to_string(index=False))
        pd_data_frame[TIME_COLUMN_NAME] = pd.to_datetime(pd_data_frame[TIME_COLUMN_NAME], errors='coerce')
        # print(pd_data_frame.head(n=5).to_string(index=False))
        return pd_data_frame

    _logger.info("Initialize loader.")

    s3 = boto3.session.Session().client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)

    with SparkRawDataLoader() as data_loader:
        _logger.info("Initialize data analysers.")
        base_data_analyser = SparkBaseStatisticsDataAnalyser()

        statistical_data_analyser = StatisticalTestsDataAnalyser()

        print("load data")
        dataset = data_loader.load()
        print(dataset.schema)

        print("dataset")
        print(dataset.show(5))

         # Choose data splitted reference and test data (70% for reference, 30% for split)
        min_date = dataset.agg(F.min(F.unix_timestamp(TIME_COLUMN_NAME))).collect()[0][0]
        max_date = dataset.agg(F.max(F.unix_timestamp(TIME_COLUMN_NAME))).collect()[0][0]
        splitted_date = F.from_unixtime(F.lit(min_date + (max_date - min_date) * 0.7)).cast("timestamp")

        ref_data = dataset.filter(F.col(TIME_COLUMN_NAME) < splitted_date)
        test_data = dataset.filter(F.col(TIME_COLUMN_NAME) >= splitted_date)

        if ruptures or evidently or statistics:
            ref_data_sampled_pandas = _convert_to_pandas(ref_data, REF_DATA_SAMPLE_COUNT)
            test_data_sampled_pandas = _convert_to_pandas(test_data, TEST_DATA_SAMPLES_COUNT)
            whole_data_samples_pandas = _convert_to_pandas(dataset, FULL_DATA_SAMPLES_COUNT) 

            # print("ref data pandas")
            # print(ref_data_sampled_pandas.head(5))

            # print("test data pandas")
            # print(test_data_sampled_pandas.head(5))

            # print("full data pandas")
            # print(whole_data_samples_pandas.head(5))

        if statistics:
        # dataset = dataset.limit(1000)
            whole_data_base_stats = base_data_analyser.analyse(dataset)

            ref_data_statistics = base_data_analyser.analyse(ref_data)
            test_data_statistics = base_data_analyser.analyse(test_data)

            for data_name, data in {"full_dataset_": whole_data_base_stats, "test_samples_": test_data_statistics, "ref_samples_": ref_data_statistics}.items():
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=REPORTS_PATH.joinpath(RAW_STATISTICS_FOLDER_NAME, data_name + BASE_STATISTICS_FILE_NAME.as_posix()).as_posix(),
                Body=json.dumps(data.base_stats),
            )

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=REPORTS_PATH.joinpath(RAW_STATISTICS_FOLDER_NAME, data_name + CORRELATION_MATRIX_FILE_NAME.as_posix()).as_posix(),
                Body=data.correlations.to_json(),
            )


        # print("base statistics")
        # print(whole_data_base_stats.base_stats)

        # print("ref statistics")
        # print(ref_data_statistics.base_stats)

        # print("test statistics")
        # print(test_data_statistics.base_stats)

        # print("Feature correlations (whole)")
        # print(whole_data_base_stats.correlations)

        # print("ref correlations (whole)")
        # print(ref_data_statistics.correlations)

        # print("test correlations (whole)")
        # print(test_data_statistics.correlations)

            _logger.info("Run statistics tests for splitted parts (whole dataset).")
            for feature_name in NUMERICAL_COLUMNS:
                # print(f"feature: {feature_name}")
                # print("test data")
                # print(test_data_sampled_pandas[feature_name].head(5))

                # print("ref data")
                # print(ref_data_sampled_pandas[feature_name].head(5))
                statistical_data_analyser.analyse(test_data_sampled_pandas[[feature_name]], ref_data_sampled_pandas[[feature_name]], feature_name)


        print("tx_fraud estimated")
        print(dataset.groupBy("tx_fraud").count().show())

        print("tx_fraud scenario estimated")
        print(dataset.groupBy("tx_fraud_scenario").count().show())

        if evidently:
            evidently_data_analyser = EvidentlyDataAnalyser()
            schema = DataDefinition(
                    numerical_columns=NUMERICAL_COLUMNS,
                    datetime_columns=[TIME_COLUMN_NAME],
                    classification=[
                    BinaryClassification(name="binary", target = "tx_fraud", prediction_labels="prediction"),
                            MulticlassClassification(
                                    name="multi",
                                    target="tx_fraud_scenario",
                                    prediction_labels="prediction",
                                )
                            ]
                        )

            _logger.info("Run evdently analyser for whole dataset.")
            evidently_data_analyser.analyse(
                dataset=Dataset.from_pandas(data=whole_data_samples_pandas, data_definition=schema),
            )

            _logger.info("Run evdently analyser for splitted parts.")
            evidently_data_analyser.analyse(
                dataset=Dataset.from_pandas(data=test_data_sampled_pandas, data_definition=schema),
                ref=Dataset.from_pandas(data=ref_data_sampled_pandas, data_definition=schema),
            )

        if ruptures:
            ruptures_data_analyser = RupturesDataAnalyser()
            _logger.info("Run ruptures analyser.")
            for feature_name in dataset.columns:
                if feature_name == TIME_COLUMN_NAME:
                    continue
                ruptures_data_analyser.analyse(whole_data_samples_pandas[[feature_name]], feature_name=feature_name)

        for item in REPORTS_PATH.rglob("*"):
            print(item)
            if Path(item).is_file():
                _upload_file(s3, item)

        _logger.info("Data analyse completed.")

        _logger.info("Start data preprocessing.")
        pipe_processor = FraudDataProcessor()
        pipe_processor.fit_model(ref_data, NUMERICAL_COLUMNS)

        for item in PREPROCESS_DATA_MODEL_PATH.rglob("*"):
            print(item)
            if Path(item).is_file():
                _upload_file(s3, item)

        _logger.info("Data analyse completed.")

if __name__ == "__main__":
    analyse(evidently=True, ruptures=True, statistics=True)