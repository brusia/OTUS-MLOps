import logging
import os
import boto3
import json
import subprocess

from argparse import ArgumentParser
from pathlib import Path
from typing import Final, Union, Any


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType

LAST_DATE_PROCESSED_ENV_NAME: Final[str] = "ENV_LAST_DATE_PROCESSED"
BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/raw"

TIME_COLUMN_NAME: Final[str] = "tx_datetime"
OUTPUT_DATA_PATH: Final[Path] = Path("data/processed_with_airlow/")
PARTITIONS_COUNT = 10000
S3_ENDPOINT_URL = "https://storage.yandexcloud.net"


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

logging.getLogger("py4j").setLevel(logging.DEBUG)
logging.getLogger("org.apache").setLevel(logging.DEBUG)

# TODO. tune processing (date-by-date)
# TODO. check the data uploading
def listing_files_from_s3_to_hdfs(s3_client: Any) -> Union[str, None]:

    files = [Path(obj['Key']) for obj in s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=INPUT_DATA_DIR).get('Contents', [])]
    last_date_processed = os.environ.get(LAST_DATE_PROCESSED_ENV_NAME, None)

    next_file_index = list(map(lambda x: x.stem, files)).index(last_date_processed) + 1 if last_date_processed else 0
    if next_file_index < len(files):
        next_file= files[next_file_index]
        return next_file

    else:
        _logger.info(f"No new files to process after {last_date_processed}")
        return None

#     # command = ["echo", "tmp"]
#     # command = ["hadoop", "distcp" f"s3a://{BUCKET_NAME}/{next_file} user/ubuntu/data/{next_file}"]
#     try:
#     #     # ruff: noqa: S603, S607
#         result = subprocess.Popen(command)
#         result.communicate()
#     except subprocess.CalledProcessError as e:
#         _logger.exception("Failed to copy file from S3 to HDFS")
#         return None
#     except ValueError:
#         _logger.exception("Failed to copy file from S3 to HDFS")
#         return None

#     # res = subprocess.run(["hdfs", "dfs", "-ls", "/user/ubuntu/data"])
#     # print(res.stdout)
#     # _logger.info(res.stdout)
#     # _logger.error(res.stderr)
    
#     # return "any"
#     return f"data/{next_file}"

# def process_data():
#     def _upload_file(s3: Any, file_name: Path, destination: Union[Path, None] = None):
#         s3.upload_file(
#             Filename=file_name.as_posix(),
#             Bucket=BUCKET_NAME,
#             Key=destination.as_posix() if destination else file_name.as_posix(),
#         )

#     _logger.info("Initialize loader.")

#     s3 = boto3.session.Session().client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)

#     with SparkRawDataLoader() as data_loader:
#         _logger.info("Initialize data analysers.")

#         print("load data")
#         ref_data: Union[SparkDataFrame, None] = None
#         ref_data_pandas: Union[pd.DataFrame, None] = None
#         for data_name, dataset in data_loader.load():
#             try:
#                 if ref_data is not None:
#                     _logger.info("Start data preprocessing.")

#                     processed_data = ref_data.filter(
#                             (F.col("transaction_id") > 0) &
#                             (F.col("customer_id") > 0) &
#                             (F.col("terminal_id") > 0) &
#                             (F.col("tx_amount") > 0)
#                         ).dropna()

#                     # pipe_processor = FraudDataProcessor()
#                     # pipe_processor.fit_model(ref_data, NUMERICAL_COLUMNS, data_name)

#                     # for item in Path("/tmp").joinpath(PREPROCESS_DATA_MODEL_PATH).rglob("*"):
#                     #     print(item.relative_to(Path("/tmp")))
#                     #     if Path(item).is_file():
#                     #         _upload_file(s3, item, item.relative_to(Path("/tmp")))

#                     # _logger.info("Data analyse completed.")

#                     # processed_data: SparkDataFrame = pipe_processor.preprocess(dataset)
#                     processed_data.write.parquet(f"file:///tmp/{OUTPUT_DATA_PATH.joinpath(data_name)}")

#                     for item in Path("/tmp").joinpath(OUTPUT_DATA_PATH, data_name).rglob("*"):
#                         print(item.relative_to(Path("/tmp")))
#                         if Path(item).is_file():
#                             _upload_file(s3, item, item.relative_to(Path("/tmp")))

#                     shutil.rmtree(Path("/tmp").joinpath(OUTPUT_DATA_PATH, data_name))
#                     shutil.rmtree(Path("/tmp").joinpath(PREPROCESS_DATA_MODEL_PATH))
#                     shutil.rmtree(REPORTS_PATH.joinpath(data_name))
                
#                 ref_data = dataset
#                 # ref_data_pandas = data_pandas
#             except Exception as ex:
#                 _logger.exception(ex)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--access_key", required=True, help="S3 access key")
    parser.add_argument("--secret_key", required=True, help="S3 secret key")
    args = parser.parse_args()
    # bucket_name = args.bucket_name
    access_key = args.access_key
    secret_key = args.secret_key

    if not access_key or not secret_key:
    # if not os.environ.get("S3_ACCESS_KEY", None):
        raise ValueError()

    spark = (
        SparkSession.builder.appName("SamplePySparkJob")
            # .config("spark.pyspark.python", "python3")
            # .config("spark.pyspark.driver.python", "python3")
            .enableHiveSupport()

            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
            .config("spark.hadoop.fs.s3a.committer.name", "magic")
            .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
            .getOrCreate()
    )

    # custom_schema = StructType(
    #         [
    #             StructField("transaction_id", IntegerType(), True),
    #             StructField("tx_datetime", TimestampType(), True),
    #             StructField("customer_id", IntegerType(), True),
    #             StructField("terminal_id", IntegerType(), True),
    #             StructField("tx_amount", DoubleType(), True),
    #             StructField("tx_time_seconds", IntegerType(), True),
    #             StructField("tx_time_days", IntegerType(), True),
    #             StructField("tx_fraud", IntegerType(), True),
    #             StructField("tx_fraud_scenario", IntegerType(), True),
    #         ]
    #     )

    # dataframe = spark.read.csv(f"s3a://{BUCKET_NAME}/data/raw/2019-10-21.txt", schema=custom_schema).dropna(how="all")

    # processed_data = dataframe.filter(
    #                         (F.col("transaction_id") > 0) &
    #                         (F.col("customer_id") > 0) &
    #                         (F.col("terminal_id") > 0) &
    #                         (F.col("tx_amount") > 0)
    #                     ).dropna()
    # # _logger.warning(dataframe.show())
    data = [("Alice", 30), ("Bob", 25), ("Cathy", 35)]
    dataframe = spark.createDataFrame(data, ["name", "age"])

    dataframe.write.mode("overwrite").parquet(f"file:///tmp/data/processed_airflow/2019-10-21")

    dataframe.write.parquet(f"s3a:///{BUCKET_NAME}/data/processed_airflow/2019-10-21")

    # access_key = os.environ.get("YANDEX_STORAGE_ACCESS_KEY_ID", None)
    # secret_key = os.environ.get("YANDEX_STORAGE_SECRET_ACCESS_KEY", None)

    Path("/tmp/data/processed_airflow/").mkdir(parents=True, exist_ok=True)
    with open("/tmp/data/processed_airflow/my_awesome_data.json", "w+") as f:
        json.dump(data, f)

    s3_client = boto3.session.Session(aws_access_key_id=access_key,
            aws_secret_access_key=secret_key).client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)
    
    # listing_files_from_s3_to_hdfs(s3_client)
    # for item in Path("/tmp/data/processed_airflow").rglob("*"):
    #         # print(item.relative_to(Path("/tmp")))
    #     _logger.info(item.relative_to(Path("/tmp")))
    #     if Path(item).is_file():
    #         # _upload_file(s3, item, item.relative_to(Path("/tmp"))
    #         file_name  =item.relative_to(Path("/tmp"))
    #         # print(file_name)
    #         s3_client.upload_file(
    #         Filename=item,
    #         Bucket=BUCKET_NAME,
    #         Key=file_name.as_posix()
    #     )


    # command = ["echo", "tmp"]
    # command = ["hadoop", "distcp", "data/processed_airflow/*", f"s3a://{BUCKET_NAME}/data/processed_airflow/"]

    # try:
    # #     # ruff: noqa: S603, S607
    #     result = subprocess.Popen(command)
    #     result.communicate()
    # except subprocess.CalledProcessError as e:
    #     _logger.exception("Failed to copy file from S3 to HDFS")
    #     # return 0
    # except ValueError:
    #     _logger.exception("Failed to copy file from S3 to HDFS")
    #     # return 0
    # finally:
    # dataframe.write.mode("overwrite").parquet(f"s3a:///{BUCKET_NAME}/data/processed_airflow/2019-10-21")


    spark.stop()
    # process_data()