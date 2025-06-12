from pyspark.sql import functions as F

from dataclasses import dataclass
from typing import Dict, Final, List
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, when, lit, countDistinct, count
from pyspark.sql.types import DoubleType, StringType, NumericType, DateType

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

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
    # categorical_stats: pd.DataFrame
    correlations: pd.DataFrame
    targets: Dict[str, pd.DataFrame]
    histo: Dict[str, list[float]]

class SparkCustomDataAnalyser(IDataAnalyser[SparkDataFrame, pd.DataFrame]):
    def __init__(self, target_columns: List[str] = None, bins_count: int = 10):
        self._targets = target_columns
        self._bins = bins_count

    def analyse(self, data_frame: SparkDataFrame) -> CustomStatistics:
        field_names = [ f.name for f in data_frame.schema.fields
                        if isinstance(f.dataType, (NumericType, DoubleType))
                        or (isinstance(f.dataType, StringType) and f.metadata.get("numeric")) ]
        

        print(data_frame.show(5))
        base_stats = self._calculate_basic_statistics(data_frame, field_names)
        # categorical_stats = self._calculate_categorical_stats(data_frame)
        correlations = self._calculate_correlations(data_frame, field_names)

        targets = {}
        if self._targets:
            for target_column_name in self._targets:
                targets[target_column_name] = self._process_target_variable(data_frame, target_column_name)

        histo = {}
        for field_name in field_names:
            histo[field_name] = self._calculate_distributions(data_frame, field_name)
        return CustomStatistics(base_stats=base_stats,
                                # categorical_stats=categorical_stats,
                                correlations=correlations,
                                targets = targets,
                                histo=histo)


    def _calculate_basic_statistics(self, frame: SparkDataFrame, numerical_cols: List[str]) -> pd.DataFrame:
        numeric_stats = frame.agg(
        *[
            F.count("*").alias("sample_size"),
            *(F.avg(c).cast("double").alias(c) for c in numerical_cols),
            *(F.stddev_pop(c).cast("double").alias(f"{c}_std") for c in numerical_cols),
            *(F.min(c).cast("double").alias(f"{c}_min") for c in numerical_cols),
            *(F.max(c).cast("double").alias(f"{c}_max") for c in numerical_cols),
            *(F.expr(f"percentile_approx({c}, {0.25}, {100000})").cast("double").alias(f"{c}_25p") for c in numerical_cols),
            *(F.expr(f"percentile_approx({c}, {0.75}, {100000})").cast("double").alias(f"{c}_75p") for c in numerical_cols)
        ])
        return numeric_stats
    

    def _calculate_correlations(self, data_frame: SparkDataFrame, numeric_cols: List[str]) -> pd.DataFrame:
        assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
        df_vector = assembler.transform(data_frame).select("features")

        correlation = Correlation.corr(df_vector, "features", "pearson").first()[0]
        correlation_matrix = np.array(correlation.toArray()).reshape(len(numeric_cols), len(numeric_cols))
        
        corr_df = pd.DataFrame(correlation_matrix, 
                            index=numeric_cols, 
                            columns=numeric_cols)
        
        return corr_df.round(ROUND_LEVEL_CATEGORIAL)


    def _calculate_categorical_stats(self, frame: SparkDataFrame) -> pd.DataFrame:
        categorical_cols = [f.name for f in frame.schema.fields
                        if isinstance(f.dataType, StringType)]
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

    def _calculate_distributions(self, data_frame: SparkDataFrame, column_name: str):
        quantiles = data_frame.approxQuantile(column_name, [i/self._bins for i in range(self._bins+1)], 0.01)
        return [quantiles[i] for i in range(len(quantiles)-1)]

    def _process_target_variable(self, frame: SparkDataFrame, target_column_name: str):
        result = {
            "min": frame.selectExpr(f"min({target_column_name})").first()[0],
            "max": frame.selectExpr(f"max({target_column_name})").first()[0],
            "mean": frame.selectExpr(f"avg({target_column_name})").first()[0],
            "std": frame.selectExpr(f"stddev_pop({target_column_name})").first()[0],
            "count_null": frame.selectExpr(f"count({target_column_name})").first()[0] / frame.count() * 100,
            "distribution": frame.selectExpr(f"histogram({target_column_name}, 50)").first()[0]
        }
        return result