
from pyspark.sql import functions as F

from dataclasses import dataclass
from typing import Final, List
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, when, lit, countDistinct, count
from pyspark.sql.types import DoubleType, StringType, NumericType, DateType

# from pyspark.ml.stat import Correlation
# from pyspark.ml.feature import VectorAssembler

# from pyspark.sql.functions import countDistinct
# from evidently import Calculator
# from evidently.model_quality import data_quality, target_quality
from otus_mlops.internals.interfaces.i_data_analyser import IDataAnalyser


PARTITIONS_COUNT: Final[int] = 100
ROUND_LEVEL_NUMERIC: Final[int] = 4
ROUND_LEVEL_CATEGORIAL: Final[int] = 3


# TODO: update fields with data classes
@dataclass
class CustomStatistics:
    base_stats: pd.DataFrame
    categorical_stats: pd.DataFrame
    # correlations: pd.DataFrame

# class SparkCustomDataAnalyser(IDataAnalyser[SparkDataFrame, pd.DataFrame]):
class SparkCustomDataAnalyser:
    def analyse(self, data_frame: SparkDataFrame) -> CustomStatistics:
        field_names = [ f.name for f in data_frame.schema.fields
                        if isinstance(f.dataType, (NumericType, DoubleType))
                        or (isinstance(f.dataType, StringType) and f.metadata.get("numeric")) ]
        

        print(data_frame.show(5))
        base_stats = self._calculate_basic_statistics(data_frame, field_names)
        categorical_stats = self._calculate_categorical_stats(data_frame)
        # correlations = self._calculate_correlations(data_frame, numerical_cols)

        # return CustomStatistics(base_stats=base_stats, categorical_stats=categorical_stats, correlations=correlations)
        return CustomStatistics(base_stats=base_stats, categorical_stats=categorical_stats)


    def _calculate_basic_statistics(self, frame: SparkDataFrame, numerical_cols: List[str]) -> pd.DataFrame:

        # basic_stats = frame.select(numerical_cols + [F.count("*").alias("sample_size")])

        # window_spec = Window.orderBy(lit(""))
        
        # mean_values = basic_stats.agg(*(expr(f"avg({col})").cast("double").alias(col) for col in numerical_cols))
        # std_values = basic_stats.agg(*(expr(f"stddev_pop({col})").cast("double").alias(col) for col in numerical_cols))
        # min_values = basic_stats.agg(*(expr(f"min({col})").cast("double").alias(col) for col in numerical_cols))
        # max_values = basic_stats.agg(*(expr(f"max({col})").cast("double").alias(col) for col in numerical_cols))
        # q1_values = basic_stats.agg(*(expr(f"percentile_approx({col}, 0.25)").cast("double").alias(col) for col in numerical_cols))
        # q3_values = basic_stats.agg(*(expr(f"percentile_approx({col}, 0.75)").cast("double").alias(col) for col in numerical_cols))
        
        # # Объединяем результаты
        # numeric_stats = pd.concat([
        #     mean_values.toPandas(),
        #     std_values.toPandas(),
        #     min_values.toPandas(),
        #     max_values.toPandas(),
        #     q1_values.toPandas(),
        #     q3_values.toPandas()
        # ], axis=1)
        
        # numeric_stats.index = ["mean", "std_dev", "min", "max", "25th_percentile", "75th_percentile"]
        # numeric_stats["n"] = basic_stats.select("sample_size").first()[0]

        # return numeric_stats.round(ROUND_LEVEL_NUMERIC)

        numeric_stats = frame.agg(
        *[
            F.count("*").alias("sample_size"),  # Общее количество записей
            *(F.avg(c).cast("double").alias(c) for c in numerical_cols),  # Среднее значение
            *(F.stddev_pop(c).cast("double").alias(f"{c}_std") for c in numerical_cols),  # Стандартное отклонение
            *(F.min(c).cast("double").alias(f"{c}_min") for c in numerical_cols),  # Минимальное значение
            *(F.max(c).cast("double").alias(f"{c}_max") for c in numerical_cols),  # Максимальное значение
            # *(F.percentile_approx(c, 0.25).cast("double").alias(f"{c}_25p") for c in numerical_cols),  # 25-й перцентиль
            # *(F.percentile_approx(c, 0.75).cast("double").alias(f"{c}_75p") for c in numerical_cols)  # 75-й перцентиль
        ])
        return numeric_stats.round(ROUND_LEVEL_NUMERIC)
    

    # def _calculate_correlations(self, data_frame: SparkDataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    #     assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    #     df_vector = assembler.transform(data_frame).select("features")

    #     correlation = Correlation.corr(df_vector, "features", "pearson").first()[0]
    #     correlation_matrix = np.array(correlation.values).squeeze()
        
    #     corr_df = pd.DataFrame(correlation_matrix, 
    #                         index=numeric_cols, 
    #                         columns=numeric_cols)
        
    #     return corr_df.round(ROUND_LEVEL_CATEGORIAL)

    # def _process_target_variable(df, target_col):
    #     result = {
    #         "min": df.selectExpr(f"min({target_col})").first()[0],
    #         "max": df.selectExpr(f"max({target_col})").first()[0],
    #         "mean": df.selectExpr(f"avg({target_col})").first()[0],
    #         "std": df.selectExpr(f"stddev_pop({target_col})").first()[0],
    #         "count_null": df.selectExpr(f"count({target_col})").first()[0] / df.count() * 100,
    #         "distribution": df.selectExpr(f"histogram({target_col}, 50)").first()[0]
    #     }
    #     return result