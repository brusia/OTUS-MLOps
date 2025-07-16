from __future__ import annotations
from contextlib import AbstractContextManager
from enum import auto
from strenum import StrEnum
from pathlib import Path
from typing import Final, Iterator, Tuple, Union

# import pyarrow.fs as fs
# import tqdm
import pandas as pd
import findspark
import os
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType
from otus_mlops.internals.interfaces import CSV_EXTENSION, PARQUET_EXTENSION, TXT_EXTENSION, IDataLoader, LoadingMethod
import sys
import logging
from logging import Logger
import pyspark.sql.functions as F

from otus_mlops.internals.interfaces.base import ACCESS_KEY_VARIABLE_NAME, BUCKET_NAME, DEFAULT_DATA_DIR, S3_ENDPOINT_URL, SECRET_KEY_VARIABLE_NAME


_logger: Logger = logging.getLogger(__name__)


class SparkRawDataLoader(IDataLoader[SparkDataFrame], AbstractContextManager):
    def __enter__(self):
        venv_python = os.path.join(os.path.dirname(sys.executable), "python")

        findspark.init()
        self._spark = (
            SparkSession.builder.appName("OTUS-MLOps")
            .config("spark.pyspark.python", "python3")
            .config("spark.pyspark.driver.python", "python3")
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get(ACCESS_KEY_VARIABLE_NAME))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get(SECRET_KEY_VARIABLE_NAME))
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
            .getOrCreate()
        )

        return super().__enter__()

    # TODO: make schema customized
    def load(
        self, data_dir: str | Path = DEFAULT_DATA_DIR, loading_method: LoadingMethod = LoadingMethod.OneByOne
    ) -> Union[Tuple[str,SparkDataFrame], Iterator[Tuple[str,SparkDataFrame]]]:
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
        data_dir = Path(data_dir)
        if not data_dir.exists:
            data_dir = DEFAULT_DATA_DIR
 
        if loading_method == LoadingMethod.OneByOne:
            fs = self._spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(self._spark.sparkContext._jsc.hadoopConfiguration())
            for status in fs.listStatus(self._spark.sparkContext._jvm.org.apache.hadoop.fs.Path(data_dir.as_posix())):
                filename = Path(status.getPath().toString())
    
            # Добавим колонку с датой без времени
            dataset = self._spark.read.csv(f"hdfs://{data_dir.joinpath(filename.name).as_posix()}", schema=custom_schema).dropna(how="all")
            dataset = dataset.withColumn("date_col", F.to_date("tx_datetime"))

            # Получим список уникальных дат
            dates = sorted([row['date_only'] for row in dataset.select("date_only").distinct().collect()])

            # Создаем генератор, который по очереди возвращает DataFrame за каждый день
            for date in dates:
                print(date)
                yield (date, dataset.filter(f"'date_only'= '{date}'"))

        elif loading_method == LoadingMethod.FullDataset:
            return ("full_data", self._spark.read.csv(f"hdfs://{data_dir.as_posix()}*", schema=custom_schema).dropna(how="all"))
        else:
            raise RuntimeError("Loading method must be specify.")

    def upload_data(self, data_frame: Union[SparkDataFrame, pd.DataFrame], output_path: str):
        if isinstance(data_frame, SparkDataFrame):
            self._upload_spark_data(data_frame, output_path)
        elif isinstance(data_frame, pd.DataFrame):
            self._upload_pandas_data(data_frame, output_path)

    def _upload_spark_data(self, data_frame: SparkDataFrame, output_path: str):
        print(data_frame.show(5))
        print(f"s3a://{BUCKET_NAME}/{output_path}")
        data_frame.write.format("parquet").save(
            f"s3a://{BUCKET_NAME}/{output_path}"
        )

    def _upload_pandas_data(self, data_frame: pd.DataFrame, output_path: str):
        spark_data_frame = self._spark.createDataFrame(data_frame)
        self.upload_data(spark_data_frame, output_path)

    def __exit__(self, exc_type, exc_value, traceback):
        self._spark.stop()
        return super().__exit__(exc_type, exc_value, traceback)
