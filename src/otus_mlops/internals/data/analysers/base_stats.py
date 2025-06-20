from pyspark.sql import functions as F

from dataclasses import dataclass
from typing import Dict, Final, List, Any
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, when, lit, countDistinct, count, stddev_pop, evg
from pyspark.sql.types import DoubleType, StringType, NumericType, DateType

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# from pyspark.sql.functions import countDistinct
# from evidently import Calculator
# from evidently.model_quality import data_quality, target_quality
from otus_mlops.internals.interfaces.i_data_analyser import IDataAnalyser

ROUND_LEVEL_NUMERIC: Final[int] = 3


# TODO: update fields with data classes
@dataclass
class BaseStatistics:
    base_stats: Dict[str, Dict[str, float | int]]
    # categorical_stats: pd.DataFrame
    correlations: pd.DataFrame
    # targets: Dict[str, SparkDataFrame]
    histo: Dict[str, pd.DataFrame]

    def get_pandas_histo(self) -> pd.DataFrame:
        return pd.DataFrame({column: value for column, value in self.histo.items()})


class SparkBaseStatisticsDataAnalyser(IDataAnalyser[SparkDataFrame, BaseStatistics]):
    def __init__(self, bins_count: int = 30):
        self._bins = bins_count

    def analyse(self, data_frame: SparkDataFrame) -> BaseStatistics:
        field_names = [
            f.name
            for f in data_frame.schema.fields
            if isinstance(f.dataType, (NumericType, DoubleType))
            or (isinstance(f.dataType, StringType) and f.metadata.get("numeric"))
        ]

        base_stats: Dict[str, Dict[str, float | int]] = {}
        histo: Dict[str, List[float]] = {}

        for field_name in field_names:
            base_stats[field_name] = self._calculate_base_statistics(data_frame, field_names)
            histo[field_name] = self._calculate_distributions(data_frame, field_name)

        # categorical_stats = self._calculate_categorical_stats(data_frame)
        correlations = self._calculate_correlations(data_frame, field_names)

        return BaseStatistics(
            base_stats=base_stats,
            # categorical_stats=categorical_stats,
            correlations=correlations,
            # targets = targets,
            histo=histo,
        )

    def _calculate_base_statistics(self, frame: SparkDataFrame, column_name: str) -> Dict[str, float]:
        stats = frame.agg(
            avg(column_name).alias("mean"),
            stddev_pop(column_name).alias("std"),
            min(column_name).alias("min"),
            max(column_name).alias("max"),
            expr(f"percentile_approx({column_name}, 0.5, 100000)").alias("median"),
            expr(f"percentile_approx({column_name}, 0.25, 100000)").alias("25percentile"),
            expr(f"percentile_approx({column_name}, 0.75, 100000)").alias("75percentile"),
            expr(f"skewness({column_name})").alias("skewness"),
            expr(f"kurtosis({column_name})").alias("kurtosis"),
            (count(col(column_name)) / count("*")).cast("double").alias("missing_ratio"),
        ).first()

        total = frame.count()
        unique = frame.select(column_name).distinct().count()
        duplicates_count = total - unique
        duplicates_ratio = duplicates_count / total if total > 0 else 0.0

        return {
            "mean": float(stats["mean"]),
            "std": float(stats["std"]),
            "min": float(stats["min"]),
            "max": float(stats["max"]),
            "median": float(stats["median"]),
            "25percentile": float(stats["25percentile"]),
            "75percentile": float(stats("75percentile")),
            "skewness": float(stats["skewness"]),
            "kurtosis": float(stats["kurtosis"]),
            "missing_ratio": float(stats["missing_ratio"]),
            "duplicates_ratio": duplicates_ratio,
        }

    def _calculate_correlations(self, data_frame: SparkDataFrame, numeric_cols: List[str]) -> pd.DataFrame:
        filtered_frame = data_frame.fillna(0, subset=numeric_cols)
        assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
        df_vector = assembler.transform(filtered_frame).select("features")

        correlation = Correlation.corr(df_vector, "features", "pearson").first()[0]
        correlation_matrix = np.array(correlation.toArray()).reshape(len(numeric_cols), len(numeric_cols))

        corr_df = pd.DataFrame(correlation_matrix, index=numeric_cols, columns=numeric_cols)

        return corr_df.round(ROUND_LEVEL_NUMERIC)

    def _calculate_distributions(self, data_frame: SparkDataFrame, column_name: str):
        def get_bin_filter(bin_index):
            lower = bin_edges[bin_index]
            upper = bin_edges[bin_index + 1]
            return expr(f"{column_name} >= {lower} AND {column_name} < {upper}")

        stats = data_frame.describe(column_name).collect()
        data_min = float(stats[1][0])
        data_max = float(stats[1][1])

        if data_max > data_min:
            range_size = max(data_max - data_min, 1.0)
        else:
            range_size = 1.0

        self._bins = max(self._bins, int(range_size))

        bin_edges = [data_min + i / self._bins * range_size for i in range(self._bins + 1)]

        counts_inside_bin: List[int] = []
        for i in range(self._bins):
            filter_expr = get_bin_filter(i)
            count = data_frame.filter(filter_expr).count()
            counts_inside_bin.append(count)

        return pd.DataFrame(counts_inside_bin)
