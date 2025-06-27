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
        histo: Dict[str, List[float]] = {}

        for field_name in field_names:
            base_stats[field_name] = self._calculate_base_statistics(data_frame, field_name)
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

        print("stats:")
        print(stats)

        return {
            "mean": float(stats["mean"]),
            "std": float(stats["std"]),
            "min": float(stats["min"]),
            "max": float(stats["max"]),
            "median": float(stats["median"]),
            "25percentile": float(stats["25percentile"]),
            "75percentile": float(stats["75percentile"]),
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

    # def _calculate_distributions(self, data_frame: SparkDataFrame, column_name: str):
    #     def get_bin_filter(bin_index):
    #         lower = bin_edges[bin_index]
    #         upper = bin_edges[bin_index + 1]
    #         return F.expr(f"{column_name} >= {lower} AND {column_name} < {upper}")

    #     stats = data_frame.describe(column_name).collect()

    #     print("stats:")
    #     print(stats)

    #     data_min = float(stats[1][0])
    #     data_max = float(stats[1][1])

    #     if data_max > data_min:
    #         range_size = max(data_max - data_min, 1.0)
    #     else:
    #         range_size = 1.0

    #     self._bins = max(self._bins, int(range_size))

    #     bin_edges = [data_min + i / self._bins * range_size for i in range(self._bins + 1)]

    #     counts_inside_bin: List[int] = []
    #     for i in range(self._bins):
    #         filter_expr = get_bin_filter(i)
    #         count = data_frame.filter(filter_expr).count()
    #         counts_inside_bin.append(count)

    #     return pd.DataFrame(counts_inside_bin)

    def _calculate_distributions(self, data_frame: SparkDataFrame, column_name: str):
        # Получаем описательные статистики
        desc = data_frame.describe(column_name)
        desc_rows = desc.collect()
        
        # Название столбца с описателями
        headers = [desc_row[0] for desc_row in desc_rows]

        print("headers")
        print(headers)
        
        other_headers = [item for item in desc_rows[1].asDict().values()]

        print("other headers")
        print(other_headers)

        print("0:")
        print(desc_rows[0])
        print("1:")
        print(desc_rows[1])
        print("2:")
        print(desc_rows[2])
        print("3:")
        print(desc_rows[3])
        # Находим индексы для min и max
        min_idx = headers.index('min')
        max_idx = headers.index('max')

        # Преобразуем соответствующие статистики в числа
        data_min = float(desc_rows[min_idx][column_name])  # Третяя строчка (индекс 2) содержит значения
        data_max = float(desc_rows[max_idx][column_name])

        print(f"data_min: {data_min}")
        print(f"max: {data_max}")

        if data_max > data_min:
            range_size = max(data_max - data_min, 1.0)
        else:
            range_size = 1.0

        self._bins = max(self._bins, int(range_size))
        # 3. Вычисляем границы бинов
        bin_edges = [data_min + i * (data_max - data_min) / self._bins for i in range(self._bins + 1)]

    # Создаем выражение для определения бина для каждого значения
        bin_expr = F.expr("""
            CASE 
                WHEN %s >= %s AND %s < %s THEN %d
                WHEN %s < %s THEN 0  -- Для значений меньше минимального
                WHEN %s >= %s THEN %d + 1000000  -- Для значений больше максимального (сдвигаем индекс)
            END
        """ % (
            column_name, data_min, column_name, 
            bin_edges[0], column_name, data_min, 
            column_name, bin_edges[-2], self._bins
        ))
        
        # Генерируем выражения для каждого бина
        bin_conditions = []
        bin_counts = []
        for i in range(self._bins):
            lower = bin_edges[i]
            upper = bin_edges[i+1]
            bin_conditions.append(
                F.when(F.col(column_name) >= lower & F.col(column_name) < upper, 1).otherwise(0)
            )
            bin_counts.append(f"bin_{i}")
        
        # Создаем временное поле с индексом бина
        data_with_bin_index = data_frame.withColumn("bin_index", bin_expr)
        
        # Группируем и суммируем по каждому бину
        result = data_with_bin_index.groupBy("bin_index").agg(
            F.expr("COUNT(*)").alias("count"),
            F.collect_list("bin_index").alias("bins")
        )
        
        # Определяем функцию для преобразования результатов
        def map_bins_to_counts(bin_index):
            if bin_index < 1000000 or bin_index < 1000000 + num_bins:
                return bin_index - 1
            elif bin_index == 1000000:
                return -1  # Подразумевает: значения меньше минимального
            else:
                return -2  # Подразумевает: значения больше максимального
        
        # Обрабатываем результаты и создаем DataFrame
        processed_counts = []
        for bin_index in sorted(result.select("bins").first()[0]):
            processed_bin = map_bins_to_counts(bin_index)
            if 0 <= processed_bin < num_bins:
                # Берем соответствующее количество
                count = next(row for row in result.selectExpr("count") where row["bins"][0] == bin_index).first()[0]
                processed_counts.append(count)
        
        return pd.DataFrame(processed_counts)

        # bin_edges = [data_min + i / self._bins * range_size for i in range(self._bins + 1)]

        # # Определяем функцию для фильтрации бина
        # def get_bin_filter(bin_index):
        #     lower = bin_edges[bin_index]
        #     upper = bin_edges[bin_index + 1]
        #     return F.expr(f"{column_name} >= {lower} AND {column_name} < {upper}")

        # counts_inside_bin = []
        # for i in range(self._bins):
        #     filter_expr = get_bin_filter(i)
        #     count = data_frame.filter(filter_expr).count()
        #     counts_inside_bin.append(count)

        # return pd.DataFrame(counts_inside_bin)