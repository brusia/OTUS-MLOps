from pathlib import Path
from typing import Final
import boto3
from pyspark.ml.feature import MinMaxScaler, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

from botocore.exceptions import ClientError
from otus_mlops.internals.data.loaders._spark_raw import SparkRawDataLoader
from otus_mlops.internals.data.preprocess._fraud_data_preprocessor import FraudDataProcessor
from otus_mlops.internals.interfaces.base import BUCKET_NAME, PREPROCESS_DATA_MODEL_PATH, S3_ENDPOINT_URL
from otus_mlops.scripts.analyse_data import NUMERICAL_COLUMNS

OUTPUT_DATA_PATH: Final[str] = "data/processed/data.parquet"
PARTITIONS_COUNT = 10000


def preprocess() -> bool:
    s3_bucket = boto3.resource(service_name="s3", endpoint_url=S3_ENDPOINT_URL).Bucket(BUCKET_NAME)
    for s3_object in s3_bucket.objects.filter(Prefix=PREPROCESS_DATA_MODEL_PATH.as_posix()).all():
        if not s3_object.key.endswith("/"):
            try:
                s3_bucket.download_file(s3_object.key, s3_object.key)
            except ClientError:
                return False

    with SparkRawDataLoader() as data_loader:
        dataset = data_loader.load()
        preprocessor = FraudDataProcessor()

        processed_data = preprocessor.preprocess(dataset)

        processed_data.repartition(PARTITIONS_COUNT).write.parquet(f"s3a://{BUCKET_NAME}/{OUTPUT_DATA_PATH}")

    return True


if __name__ == "__main__":
    preprocess()
