from __future__ import annotations
from contextlib import AbstractContextManager
from enum import auto
from strenum import StrEnum
from pathlib import Path
from typing import Final, Iterator, Union

import tqdm
import pandas as pd
import findspark
import os
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType
from otus_mlops.internals.interfaces import CSV_EXTENSION, PARQUET_EXTENSION, TXT_EXTENSION, IDataLoader, LoadingMethod
import sys
import logging
from logging import Logger

DEFAULT_DATA_DIR: Final[Path] = Path("/user/ubuntu/data")

ACCESS_KEY_VARIABLE_NAME: Final[str] = "AWS_ACCESS_KEY_ID"
SECRET_KEY_VARIABLE_NAME: Final[str] = "AWS_SECRET_ACCESS_KEY"

BUCKET_NAME: Final[str] = "brusia-bucket"
S3_ENDPOINT_URL: Final[str] = "https://storage.yandexcloud.net"


_logger: Logger = logging.getLogger(__name__)


class SparkRawDataLoader(IDataLoader[SparkDataFrame], AbstractContextManager):
    def __enter__(self):
        venv_python = os.path.join(os.path.dirname(sys.executable), "python")
        # os.environ["PYARROW_IGNORE_INIT"] = "yes"
        # os.environ["PYSPARK_PYTHON"] = venv_python
        # os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

        findspark.init()
        self._spark = (
            SparkSession.builder.appName("OTUS-MLOps")
            # .config("spark.sql.shuffle.partitions", "1000")
            .config("spark.pyspark.python", "python3")
            .config("spark.pyspark.driver.python", "python3")
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get(ACCESS_KEY_VARIABLE_NAME))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get(SECRET_KEY_VARIABLE_NAME))
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            # .config("spark.pyspark.python", venv_python)
            # .config("spark.pyspark.driver.python", venv_python)
            # .config("spark.executorEnv.PYSPARK_PYTHON", venv_python)
            .getOrCreate()
        )
        return super().__enter__()
    # @override
    # TODO: make schema customized
    def load(
        self, data_dir: str | Path = DEFAULT_DATA_DIR, loading_method: LoadingMethod = LoadingMethod.OneByOne
    ) -> Union[SparkDataFrame, Iterator[SparkDataFrame]]:
        custom_schema = StructType(
            [
                StructField("transaction_id", IntegerType(), True),
                StructField("tx_datetime", TimestampType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("terminal_id", IntegerType(), True),
                StructField("tx_amount", DoubleType(), True),
                StructField("tx_time_seconds", IntegerType(), True),
                StructField("tx_time_days", IntegerType(), True),
                StructField("tx_fraud", IntegerType(), True),
                StructField("tx_fraud_scenario", IntegerType(), True),
            ]
        )
        frame = self._spark.read.csv(f"hdfs://{data_dir.as_posix()}*", schema=custom_schema).dropna(how="all")
        return frame

    def upload_data(self, data_frame: Union[SparkDataFrame, pd.DataFrame], output_path: str):
        if isinstance(data_frame, SparkDataFrame):
            self._upload_spark_data(data_frame, output_path)
        elif isinstance(data_frame, pd.DataFrame):
            self._upload_pandas_data(data_frame, output_path)

    def _upload_spark_data(self, data_frame: SparkDataFrame, output_path: str):
        data_frame.write.format("csv").option("header", "true").option("delimiter", ";").save(
            f"s3a://{BUCKET_NAME}/{output_path}"
        )

    def _upload_pandas_data(self, data_frame: pd.DataFrame, output_path: str):
        spark_data_frame = self._spark.createDataFrame(data_frame)
        self.upload_data(spark_data_frame, output_path)

    def __exit__(self, exc_type, exc_value, traceback):
        self._spark.stop()
        return super().__exit__(exc_type, exc_value, traceback)
