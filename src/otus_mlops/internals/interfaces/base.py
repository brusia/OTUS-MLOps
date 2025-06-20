from typing import TypeVar
from evidently import Dataset
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd

from otus_mlops.internals.data.analysers.base_stats import BaseStatistics

DataFrame = TypeVar("DataFrame", pd.DataFrame, SparkDataFrame)
AnalyserInputData = TypeVar("AnalyserInputData", pd.DataFrame, Dataset)
AnalyserOutputData = TypeVar("AnalyserOutputData", BaseStatistics, None)
