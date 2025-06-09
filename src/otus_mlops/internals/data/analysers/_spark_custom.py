

from dataclasses import dataclass
from typing import Final
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, when, lit, countDistinct, count
from pyspark.sql.types import DoubleType, StringType

from pyspark.ml.stat import Correlation, CorrelationWrapper
from pyspark.ml.feature import VectorAssembler

from pyspark.sql.functions import countDistinct
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
    correlations: pd.DataFrame

class SparkCustomDataAnalyser(IDataAnalyser[SparkDataFrame]):
    def analyse(self, data_frame: SparkDataFrame) -> CustomStatistics:
        numerical_cols = [f.name for f in data_frame.schema.fields 
                        # if (f.dataType.kind in ['N', 'D'] or 
                        if (f.dataType.json() in ['N', 'D'] or 
                            (isinstance(f.dataType, StringType) and f.metadata.get("numeric")))]
        
        base_stats = self._calculate_basic_statistics(data_frame, numerical_cols)
        categorical_stats = self._calculate_categorical_stats(data_frame)
        correlations = self._calculate_correlations(data_frame, numerical_cols)

        return CustomStatistics(base_stats=base_stats, categorical_stats=categorical_stats, correlations=correlations)


    def _calculate_basic_statistics(self, frame: SparkDataFrame, numerical_cols: list[str]) -> pd.DataFrame:

        basic_stats = frame.select(numerical_cols + [count("*").alias("sample_size")])

        window_spec = Window.orderBy(lit(""))
        
        mean_values = basic_stats.agg(*(expr(f"avg({col})").cast("double").alias(col) for col in numerical_cols))
        std_values = basic_stats.agg(*(expr(f"stddev_pop({col})").cast("double").alias(col) for col in numerical_cols))
        min_values = basic_stats.agg(*(expr(f"min({col})").cast("double").alias(col) for col in numerical_cols))
        max_values = basic_stats.agg(*(expr(f"max({col})").cast("double").alias(col) for col in numerical_cols))
        q1_values = basic_stats.agg(*(expr(f"percentile_approx({col}, 0.25)").cast("double").alias(col) for col in numerical_cols))
        q3_values = basic_stats.agg(*(expr(f"percentile_approx({col}, 0.75)").cast("double").alias(col) for col in numerical_cols))
        
        # Объединяем результаты
        numeric_stats = pd.concat([
            mean_values.toPandas(),
            std_values.toPandas(),
            min_values.toPandas(),
            max_values.toPandas(),
            q1_values.toPandas(),
            q3_values.toPandas()
        ], axis=1)
        
        numeric_stats.index = ["mean", "std_dev", "min", "max", "25th_percentile", "75th_percentile"]
        numeric_stats["n"] = basic_stats.select("sample_size").first()[0]

        return numeric_stats.round(ROUND_LEVEL_NUMERIC)

    def _calculate_categorical_stats(self, frame: SparkDataFrame) -> pd.DataFrame:
        categorical_cols = [f.name for f in frame.schema.fields 
                        if f.dataType.json() == 'S' or 
                            (isinstance(f.dataType, StringType) and not any(x in f.metadata.get("exclude_for_model", []) for x in ['numeric', 'id']))]

        sample_size = min(100000, frame.count())
        
        # TODO: debug
        if sample_size > 2 * PARTITIONS_COUNT:  # frame.sparkSession.sparkContext.getConf().getInt("spark.sql.shuffle.partitions", 100):
            print("Ограничена выборка для категориальных признаков")
            sample_df = frame.sample(False, sample_size/float(frame.count()))
        else:
            sample_df = frame
        
        freq_data = {}
        for col in categorical_cols:
            freq_rdd = sample_df.groupBy(col).agg(count("*").alias("count")).sort(col)
            freq = [(row[0], round(row[1], 2)) for row in freq_data[col]]
            freq_data[col] = freq
            
            top_10 = pd.DataFrame([(k, v) for col, freqs in freq_data.items() 
                                for k, v in freqs[:10]], columns=[col, "percentage"])
        
        return freq_data
    

    def _calculate_correlations(self, data_frame: SparkDataFrame, numeric_cols: list[str]) -> pd.DataFrame:
        assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
        df_vector = assembler.transform(data_frame).select("features")

        correlation = Correlation.corr(df_vector, "features", "pearson").first()[0]
        correlation_matrix = np.array(correlation.values).squeeze()
        
        corr_df = pd.DataFrame(correlation_matrix, 
                            index=numeric_cols, 
                            columns=numeric_cols)
        
        return corr_df.round(ROUND_LEVEL_CATEGORIAL)

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