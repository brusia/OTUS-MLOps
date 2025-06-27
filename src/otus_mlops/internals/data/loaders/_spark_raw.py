from __future__ import annotations
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


class SparkRawDataLoader(IDataLoader[SparkDataFrame]):
    def __init__(self):
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

    # spark.sparkContext.setLogLevel("ERROR")

    # @override
    # TODO
    def load(
        self, data_dir: str | Path = DEFAULT_DATA_DIR, loading_method: LoadingMethod = LoadingMethod.OneByOne
    ) -> Union[SparkDataFrame, Iterator[SparkDataFrame]]:
        # print(data_dir)
        #   data_path: Path
        #  if isinstance(data_dir, Path) and data_dir.exists():
        #     data_path = data_dir
        # print(data_path)
        # elif isinstance(data_dir, str) and Path(data_dir).exists():
        #     data_path = Path(data_dir)

        #   print(data_path)
        data_path = data_dir
        if loading_method == LoadingMethod.FullDataset:
            return self._load_full(data_path)
        else:  #  LoadingMethod.OneByOne:
            return self._load_one_by_one(data_path)

    def _load_full(self, data_path: Path) -> SparkDataFrame:
        frames: list[SparkDataFrame] = []
        for _, data_frame in enumerate(self._load_one_by_one(data_path)):
            frames.append(data_frame)

        return self._spark.union(*frames)

    # TODO: customized schema for unify the input data format
    def _load_one_by_one(self, data_path: Path) -> Iterator[SparkDataFrame]:
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

        # TODO: not one by one (!)
        frame = self._spark.read.csv(f"hdfs://{data_path.as_posix()}*", schema=custom_schema).dropna(how="all")
        return frame
        # files = self._spark.sparkContext.textFile(f"hdfs://{data_path.as_posix()}*").keys().collect()
        # # print(files)
        # for file_path in tqdm.tqdm(files, desc="Loading data"):
        #     print(file_path)
        #     try:
        #         if Path(file_path).suffix in (CSV_EXTENSION, TXT_EXTENSION):
        #             frame = self._spark.read.csv(file_path, header=True, inferSchema=True)
        #         elif Path(file_path).suffix == PARQUET_EXTENSION:
        #             frame = self._spark.read.json(file_path)
        #         else:
        #             _logger.warning("File format '%s' is not supported.", file_path)
        #             continue

        #         if frame.isEmpty():
        #             _logger.warning("File '%s' does not contain data.", file_path)
        #             continue

        #         _logger.debug("File '%s' was loaded succesfully.", file_path)
        #         yield frame

        #     # TODO: specify exception types.
        #     except Exception:
        #         _logger.exception("File '%s' could not be loaded.", file_path.as_posix())
        #         continue

    def _upload_spark_data(self, data_frame: SparkDataFrame, output_path: str):
        data_frame.write.format("csv").option("header", "true").option("delimiter", ";").save(
            f"s3a://{BUCKET_NAME}/{output_path}"
        )

    def upload_data(self, data_frame: Union[SparkDataFrame, pd.DataFrame], output_path: str):
        if isinstance(data_frame, SparkDataFrame):
            self._upload_spark_data(data_frame, output_path)
        elif isinstance(data_frame, pd.DataFrame):
            self._upload_pandas_data(data_frame, output_path)

    def _upload_pandas_data(self, data_frame: pd.DataFrame, output_path: str):
        spark_data_frame = self._spark.createDataFrame(data_frame)
        self.upload_data(spark_data_frame, output_path)

    def __del__(self):
        self._spark.stop()
