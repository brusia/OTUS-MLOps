from typing import Dict, TypeVar, Union
from evidently import Dataset
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd

from dataclasses import dataclass


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