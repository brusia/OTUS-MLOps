from pyspark.sql import functions as F

from typing import Dict, Final, List, Any
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
# from pyspark.sql.functions import col, expr, when, lit, countDistinct, count, stddev_pop, avg
from pyspark.sql.types import DoubleType, StringType, NumericType, DateType

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# from pyspark.sql.functions import countDistinct
# from evidently import Calculator
# from evidently.model_quality import data_quality, target_quality
from otus_mlops.internals.interfaces import IDataAnalyser
from otus_mlops.internals.interfaces.base import BaseStatistics

ROUND_LEVEL_NUMERIC: Final[int] = 3



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
        # histo: Dict[str, List[float]] = {}

        for field_name in field_names:
            base_stats[field_name] = self._calculate_base_statistics(data_frame, field_name)

        correlations = self._calculate_correlations(data_frame, field_names)

        return BaseStatistics(
            base_stats=base_stats,
            correlations=correlations,
        )

    def _calculate_base_statistics(self, frame: SparkDataFrame, column_name: str) -> Dict[str, float]:
        stats = frame.agg(
            F.avg(column_name).alias("mean"),
            F.stddev_pop(column_name).alias("std"),
            F.min(column_name).alias("min"),
            F.max(column_name).alias("max"),
            F.expr(f"percentile_approx({column_name}, 0.5, 100000)").alias("median"),
            F.expr(f"percentile_approx({column_name}, 0.25, 100000)").alias("25percentile"),
            F.expr(f"percentile_approx({column_name}, 0.75, 100000)").alias("75percentile"),
            F.expr(f"skewness({column_name})").alias("skewness"),
            F.expr(f"kurtosis({column_name})").alias("kurtosis"),
            (F.count(F.col(column_name)) / F.count("*")).cast("double").alias("missing_ratio"),
        ).first()

        total = frame.count()
        unique = frame.select(column_name).distinct().count()
        duplicates_count = total - unique
        duplicates_ratio = duplicates_count / total if total > 0 else 0.0

        print(f"stats for column '{column_name}':")
        print(stats)

        return {
            "mean": float(stats["mean"]) if stats["mean"] else None,
            "std": float(stats["std"]) if stats["std"] else None,
            "min": float(stats["min"]) if stats["min"] else None,
            "max": float(stats["max"]) if stats["max"] else None,
            "median": float(stats["median"]) if stats["median"] else None,
            "25percentile": float(stats["25percentile"]) if stats["25percentile"] else None,
            "75percentile": float(stats["75percentile"]) if stats["75percentile"] else None,
            "skewness": float(stats["skewness"]) if stats["skewness"] else None,
            "kurtosis": float(stats["kurtosis"]) if stats["kurtosis"] else None,
            "missing_ratio": float(stats["missing_ratio"]) if stats["missing_ratio"] else None,
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