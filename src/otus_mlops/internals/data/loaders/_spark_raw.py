from __future__ import annotations
from enum import auto
from strenum import StrEnum
from pathlib import Path
from typing import Final, Iterator, override

import tqdm
import pandas as pd
import findspark
import os
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from otus_mlops.internals.interfaces.i_data_loader import CSV_EXTENSION, PARQUET_EXTENSION, TXT_EXTENSION, IDataLoader, LoadingMethod
import sys
DEFAULT_DATA_DIR: Final[Path] = Path("/home/ubuntu/data")

import logging


_logger = logging.getLogger(__name__)


class SparkRawDataLoader(IDataLoader[SparkDataFrame]):
    def __init__(self):
        venv_python = os.path.join(os.path.dirname(sys.executable), "python")
        # os.environ["PYARROW_IGNORE_INIT"] = "yes"
        os.environ["PYSPARK_PYTHON"] = venv_python
        os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

        findspark.init()
        self._spark = (
            SparkSession
                .builder
                .appName("OTUS-MLOps")
                
                # .config("spark.sql.shuffle.partitions", "100") 
                .config("spark.pyspark.python", venv_python)
                .config("spark.pyspark.driver.python", venv_python)
                .config("spark.executorEnv.PYSPARK_PYTHON", venv_python)
                .getOrCreate()
        )


    # spark.sparkContext.setLogLevel("ERROR")

    # @override
    def load(self, data_dir: str | Path = "", loading_method: LoadingMethod = LoadingMethod.OneByOne) -> SparkDataFrame | Iterator[SparkDataFrame]:
        data_path: Path
        if isinstance(data_dir, Path) and data_dir.exists():
            data_path = data_dir
        elif isinstance(data_dir, str) and Path(data_dir).exists():
            data_path = Path(data_dir)
        else:
            data_path = DEFAULT_DATA_DIR

        match loading_method:
            case LoadingMethod.FullDataset:
                return self._load_full(data_path)
            case _: #  LoadingMethod.OneByOne:
                return self._load_one_by_one(data_path)


    def _load_full(self, data_path: Path) -> SparkDataFrame:
        frames: list[SparkDataFrame] = []
        for (_, data_frame) in enumerate(self._load_one_by_one(data_path)):
            frames.append(data_frame)

        return self._spark.unionAll(*frames)

    def _load_one_by_one(self, data_path: Path) -> Iterator[SparkDataFrame]:
        for file_path, _, _ in tqdm(data_path.iterdir(), desc="Loading data"):
            try:
                if file_path.suffix in (CSV_EXTENSION, TXT_EXTENSION):
                    frame = self._spark.read.csv(file_path, header=True, inferSchema=True)
                elif file_path.suffix == PARQUET_EXTENSION:
                    frame = self._spark.read.json(file_path)
                else:
                    _logger.warning("File format '%s' is not supported.", file_path.as_posix())
                    continue
                
                if frame.isEmpty():
                    _logger.warning("File '%s' does not contain data.", file_path.as_posix())
                    continue
                
                _logger.debug("File '%s' was loaded succesfully.", file_path)
                yield frame

            # TODO: specify exception types.
            except Exception:
                _logger.exception("File '%s' could not be loaded.", file_path.as_posix())
                continue