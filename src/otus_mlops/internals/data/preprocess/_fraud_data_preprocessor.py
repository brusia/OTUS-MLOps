from pyspark.sql import DataFrame as SparkDataFrame
from otus_mlops.internals.interfaces import IDataPreprocessor


class FraudDataProcessor(IDataPreprocessor[SparkDataFrame, SparkDataFrame]):
    def preprocess(self, input_data: SparkDataFrame) -> SparkDataFrame:
        return super().preprocess(input_data)
