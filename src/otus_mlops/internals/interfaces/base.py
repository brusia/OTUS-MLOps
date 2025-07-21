from pathlib import Path
from typing import Dict, Final, TypeVar, Union
from evidently import Dataset
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd

from dataclasses import dataclass


PREPROCESS_DATA_MODEL_PATH: Final[Path] = Path("models/data_preprocess")
DEFAULT_DATA_DIR: Final[Path] = Path("/user/ubuntu/data")

ACCESS_KEY_VARIABLE_NAME: Final[str] = "AWS_ACCESS_KEY_ID"
SECRET_KEY_VARIABLE_NAME: Final[str] = "AWS_SECRET_ACCESS_KEY"

BUCKET_NAME: Final[str] = "brusia-bucket"
S3_ENDPOINT_URL: Final[str] = "https://storage.yandexcloud.net"

NUMERICAL_COLUMNS = [ "transaction_id",
                    "customer_id",
                    "terminal_id",
                    "tx_amount",
                    "tx_time_seconds",
                    "tx_time_days",
                    "tx_fraud",
                    "tx_fraud_scenario"]

# TODO: update fields with data classes
@dataclass
class BaseStatistics:
    base_stats: Dict[str, Dict[str, Union[float, int]]]
    # categorical_stats: pd.DataFrame
    correlations: pd.DataFrame
    # targets: Dict[str, SparkDataFrame]
    # histo: Dict[str, pd.DataFrame]

    # def get_pandas_histo(self) -> pd.DataFrame:
    #     return pd.DataFrame({column: value for column, value in self.histo.items()})


DataFrame = TypeVar("DataFrame", pd.DataFrame, SparkDataFrame)
AnalyserInputData = TypeVar("AnalyserInputData", pd.DataFrame, Dataset, SparkDataFrame)
AnalyserOutputData = TypeVar("AnalyserOutputData", BaseStatistics, None)