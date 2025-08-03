from pathlib import Path
import shutil
from typing import Final, Any, Union
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
from otus_mlops.internals.data.preprocess._fraud_data_preprocessor import FraudDataProcessor
from otus_mlops.internals.interfaces import REPORTS_PATH
import pandas as pd

import json
import logging
from otus_mlops.internals.data.loaders._spark_raw import SparkRawDataLoader
from otus_mlops.internals.interfaces.base import BUCKET_NAME, NUMERICAL_COLUMNS, PREPROCESS_DATA_MODEL_PATH, S3_ENDPOINT_URL
from pyspark.sql import DataFrame as SparkDataFrame

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

RAW_STATISTICS_FOLDER_NAME: Final[str] = "raw-statistics"
BASE_STATISTICS_FILE_NAME: Final[Path] = Path("base_statistics.json")
CORRELATION_MATRIX_FILE_NAME: Final[Path] = Path("correlations.csv")
TIME_COLUMN_NAME: Final[str] = "tx_datetime"


TEST_DATA_SAMPLES_COUNT: Final[int] = 1000
REF_DATA_SAMPLE_COUNT: Final[int] = 10000
FULL_DATA_SAMPLES_COUNT: Final[int] = 100000

OUTPUT_DATA_PATH: Final[Path] = Path("data/processed/")
PARTITIONS_COUNT = 10000

FULL_DATA_SAMPLES_COUNT: Final[int] = 1000



ITERATIONS_COUNT: Final[int] = 10

def analyse(evidently: bool = False, ruptures: bool = False, statistics: bool = False):
    def _upload_file(s3: Any, file_name: Path, destination: Union[Path, None] = None):
        s3.upload_file(
            Filename=file_name.as_posix(),
            Bucket=BUCKET_NAME,
            Key=destination.as_posix() if destination else file_name.as_posix(),
        )

    def _convert_to_pandas(dataframe: SparkDataFrame, num_samples: int = 0) -> pd.DataFrame:
        if num_samples > 0:
            dataframe: pd.DataFrame = dataframe.limit(num_samples)

        pd_data_frame: pd.DataFrame = dataframe.withColumn(TIME_COLUMN_NAME, F.col(TIME_COLUMN_NAME).cast('string')).toPandas()
        pd_data_frame[TIME_COLUMN_NAME] = pd.to_datetime(pd_data_frame[TIME_COLUMN_NAME], errors='coerce')
        return pd_data_frame

    _logger.info("Initialize loader.")

    s3 = boto3.session.Session().client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)

    with SparkRawDataLoader() as data_loader:
        _logger.info("Initialize data analysers.")

        print("load data")
        ref_data: Union[SparkDataFrame, None] = None
        ref_data_pandas: Union[pd.DataFrame, None] = None
        for data_name, dataset in data_loader.load():

            if ruptures or evidently or statistics:
                data_pandas = _convert_to_pandas(dataset)

            try:
                if statistics:
                    base_data_analyser = SparkBaseStatisticsDataAnalyser()
                    statistical_data_analyser = StatisticalTestsDataAnalyser()
                    base_statistics = base_data_analyser.analyse(dataset)

                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=REPORTS_PATH.joinpath(data_name, RAW_STATISTICS_FOLDER_NAME, BASE_STATISTICS_FILE_NAME.as_posix()).as_posix(),
                        Body=json.dumps(base_statistics.base_stats),
                    )

                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=REPORTS_PATH.joinpath(data_name, RAW_STATISTICS_FOLDER_NAME, CORRELATION_MATRIX_FILE_NAME.as_posix()).as_posix(),
                        Body=base_statistics.correlations.to_json(),
                    )

                    _logger.info("Run statistics tests for splitted parts (whole dataset).")
                    if ref_data_pandas is not None:
                        for feature_name in NUMERICAL_COLUMNS:
                            statistical_data_analyser.analyse(data_pandas[feature_name], ref_data_pandas[feature_name], feature_name, data_name)
            except Exception as ex:
                _logger.exception(ex)


            try:
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
                        dataset=Dataset.from_pandas(data=data_pandas, data_definition=schema),
                        data_name=data_name
                    )

                    if ref_data_pandas is not None:
                        _logger.info("Run evdently analyser for splitted parts.")
                        evidently_data_analyser.analyse(
                            dataset=Dataset.from_pandas(data=data_pandas, data_definition=schema),
                            ref=Dataset.from_pandas(data=ref_data_pandas, data_definition=schema),
                            data_name=data_name
                        )
            except Exception as ex:
                _logger.exception(ex)

            try:
                if ruptures:
                    ruptures_data_analyser = RupturesDataAnalyser()
                    _logger.info("Run ruptures analyser.")
                    for feature_name in dataset.columns:
                        if feature_name == TIME_COLUMN_NAME:
                            continue
                        ruptures_data_analyser.analyse(data_pandas.sample(n=min(FULL_DATA_SAMPLES_COUNT, len(data_pandas)))[[feature_name]], feature_name=feature_name, data_name=data_name)
            except Exception as ex:
                _logger.exception(ex)

            for item in REPORTS_PATH.rglob("*"):
                print(item)
                if Path(item).is_file():
                    _upload_file(s3, item)

            _logger.info("Data analyse completed.")


            try:
                if ref_data is not None:
                    _logger.info("Start data preprocessing.")

                    ref_data = ref_data.filter(
                            (F.col("transaction_id") > 0) &
                            (F.col("customer_id") > 0) &
                            (F.col("terminal_id") > 0) &
                            (F.col("tx_amount") > 0)
                        ).dropna()

                    pipe_processor = FraudDataProcessor()
                    pipe_processor.fit_model(ref_data, NUMERICAL_COLUMNS, data_name)

                    for item in Path("/tmp").joinpath(PREPROCESS_DATA_MODEL_PATH).rglob("*"):
                        print(item.relative_to(Path("/tmp")))
                        if Path(item).is_file():
                            _upload_file(s3, item, item.relative_to(Path("/tmp")))

                    _logger.info("Data analyse completed.")

                    processed_data: SparkDataFrame = pipe_processor.preprocess(dataset)
                    print(processed_data.show())
                    processed_data.write.mode('overwrite').parquet(f"file:///tmp/{OUTPUT_DATA_PATH.joinpath(data_name, 'data.parquet').as_posix()}")

                    for item in Path("/tmp").joinpath(OUTPUT_DATA_PATH, data_name).rglob("*"):
                        print(item.relative_to(Path("/tmp")))
                        if Path(item).is_file():
                            _upload_file(s3, item, item.relative_to(Path("/tmp")))

                    shutil.rmtree(Path("/tmp").joinpath(OUTPUT_DATA_PATH, data_name))
                    shutil.rmtree(Path("/tmp").joinpath(PREPROCESS_DATA_MODEL_PATH))
                    shutil.rmtree(REPORTS_PATH.joinpath(data_name))
                
                ref_data = dataset
                ref_data_pandas = data_pandas
            except Exception as ex:
                _logger.exception(ex)

if __name__ == "__main__":
    analyse(evidently=True, ruptures=False, statistics=True)