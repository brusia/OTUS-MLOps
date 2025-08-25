import logging
import os
import boto3

from argparse import ArgumentParser
from pathlib import Path
from typing import Final, Union, Any

from pyspark.sql import functions as F


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType

_logger = logging.getLogger(__name__)
BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/raw"

TIME_COLUMN_NAME: Final[str] = "tx_datetime"
OUTPUT_DATA_DIR: Final[str] = "data/processed_with_airlow/"
PARTITIONS_COUNT = 10000
S3_ENDPOINT_URL = "https://storage.yandexcloud.net"


def get_next_file_to_process(s3_client: Any) -> Union[Path, None]:
    input_files = [Path(obj['Key']) for obj in s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=INPUT_DATA_DIR).get('Contents', [])]
    processed_files = sorted([Path(obj['Prefix']).relative_to(OUTPUT_DATA_DIR).as_posix() for obj in s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=OUTPUT_DATA_DIR, Delimiter='/').get('CommonPrefixes', [])])
    last_date_processed = processed_files[-1] if processed_files else None

    next_file_index = list(map(lambda x: x.stem, input_files)).index(last_date_processed) + 1 if last_date_processed else 0
    if next_file_index < len(input_files):
        next_file= input_files[next_file_index]
        return next_file
    else:
        _logger.info("All the data processed. There are no data to process.")
        return None

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--access_key", required=True, help="S3 access key")
    parser.add_argument("--secret_key", required=True, help="S3 secret key")

    args = parser.parse_args()
    access_key = args.access_key
    secret_key = args.secret_key

    if not access_key or not secret_key:
        raise ValueError()

    spark = (
        SparkSession.builder.appName("DataPreprocess")
            .enableHiveSupport()
            .getOrCreate()
    )

    s3_client = boto3.session.Session(aws_access_key_id=access_key,
            aws_secret_access_key=secret_key).client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)

    custom_schema = StructType(
            [
                StructField("transaction_id", IntegerType(), True),
                StructField("tx_datetime", TimestampType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("terminal_id", IntegerType(), True),
                StructField("tx_amount", DoubleType(), True),
                StructField("tx_time_seconds", IntegerType(), True),
                StructField("tx_time_days", IntegerType(), True),
                StructField("tx_fraud", IntegerType(), True),
                StructField("tx_fraud_scenario", IntegerType(), True),
            ]
        )
    
 
    file_to_process = get_next_file_to_process(s3_client)
    if file_to_process:
        dataframe = spark.read.csv(f"s3a://{BUCKET_NAME}/{file_to_process.as_posix()}", schema=custom_schema).dropna(how="all")

        processed_data = dataframe.filter(
                                (F.col("transaction_id") > 0) &
                                (F.col("customer_id") > 0) &
                                (F.col("terminal_id") > 0) &
                                (F.col("tx_amount") > 0)
                            ).dropna()
        
        processed_data.write.mode("overwrite").parquet(f"file://{Path('/tmp').joinpath(OUTPUT_DATA_DIR, file_to_process.stem).as_posix()}")
        
        
        for item in Path("/tmp/").joinpath(OUTPUT_DATA_DIR).rglob("*"):
            print(item.relative_to(Path("/tmp")))
            if Path(item).is_file():
                file_name  =item.relative_to(Path("/tmp"))
                print(file_name)
                s3_client.upload_file(
                    Filename=item.as_posix(),
                    Bucket=BUCKET_NAME,
                    Key=file_name.as_posix()
                )

    spark.stop()