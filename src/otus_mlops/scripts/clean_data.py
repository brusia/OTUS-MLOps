from typing import Final
from pyspark.ml.feature import MinMaxScaler, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

from otus_mlops.internals.data.loaders._spark_raw import SparkRawDataLoader
from otus_mlops.internals.data.preprocess._fraud_data_preprocessor import FraudDataProcessor
from otus_mlops.internals.interfaces.base import BUCKET_NAME
from otus_mlops.scripts.analyse_data import NUMERICAL_COLUMNS

OUTPUT_DATA_PATH: Final[str] = "data/processed/data.parquet"
PARTITIONS_COUNT = 10000


def preprocess():
    with SparkRawDataLoader() as data_loader:
        dataset = data_loader.load()
        preprocessor = FraudDataProcessor()

        processed_data = preprocessor.preprocess(dataset)

        processed_data.repartition(PARTITIONS_COUNT).write.parquet(f"s3a://{BUCKET_NAME}/{OUTPUT_DATA_PATH}")


if __name__ == "__main__":
    preprocess()
