

from typing import TypeVar
from evidently import Dataset
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd

DataFrame = TypeVar("DataFrame", pd.DataFrame, SparkDataFrame)
InputData = TypeVar("InputData", pd.DataFrame, Dataset)