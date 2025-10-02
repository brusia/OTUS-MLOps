"""
Script: infer_using_kafka.py
Description: PySpark script for inrefence a fraud detection model and produce results into kafka queue.
"""

from datetime import datetime
import json
import os
import sys
import argparse
from typing import Any, Final, List, Union

from pyspark.sql import SparkSession, Row
from kafka import KafkaConsumer, KafkaProducer
import mlflow
import mlflow.spark
import logging


from pyspark.sql import functions as F

_logger = logging.getLogger(__name__)

BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/processed_with_airlow/"

OUTPUT_MODELS_DIR: Final[str] = "models/fraud_detection_validate/"
DATE_FORMAT: Final[str] = "%Y%m%d"


TARGET_COLUMN_NAMES: List[str] = ["tx_fraud", "tx_fraud_scenario"]



def create_spark_session(s3_config=None):
    """
    Create and configure a Spark session.

    Parameters
    ----------
    s3_config : dict, optional
        Dictionary containing S3 configuration parameters
        (endpoint_url, access_key, secret_key)

    Returns
    -------
    SparkSession
        Configured Spark session
    """
    _logger.debug("Start to create Spark-session")
    try:
        builder = (SparkSession
            .builder
            .appName("FraudDetectionOptimizationModel")
        )

        if s3_config and all(k in s3_config for k in ['endpoint_url', 'access_key', 'secret_key']):
            _logger.debug(f"Conifgure S3 withendpoint_url: {s3_config['endpoint_url']}")
            builder = (builder
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint_url'])
                .config("spark.hadoop.fs.s3a.access.key", s3_config['access_key'])
                .config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key'])
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            )

        _logger.debug("Spark session configured successfully")
        
        return builder
    except Exception as e: 
        _logger.exception("Error with session create")
        raise e


def load_model_from_mlflow(model_name, alias="champion") -> Union[Any, None]:
    try:
        try:
            model_uri = f"models:/{model_name}@{alias}"
            model = mlflow.spark.load_model(model_uri)
            return model
        except Exception as ex:
            mlflow.log_text(f"{ex}", "loading_model_problem.txt")
            client = mlflow.tracking.MlflowClient()
            model_versions = client.get_latest_versions(model_name)
            
            for version in model_versions:
                if hasattr(version, 'tags') and version.tags.get('alias') == alias:
                    model_uri = f"models:/{model_name}/{version.version}"
                    model = mlflow.spark.load_model(model_uri)
                    return model
            
            mlflow.log_text(f"model '{model_name}' with alias '{alias}' was not found", "not_found_exeption.txt")
            return None
            
    except Exception as e:
        mlflow.log_text(f"{e}", "model_loading_exeption.txt")
        return None


def infer():
    """
    Main function to run the inference due kafka.
    """
    parser = argparse.ArgumentParser(description="Fraud Detection Model Training")

    parser.add_argument("--tracking-uri", help="MLflow tracking URI")
    parser.add_argument("--experiment-name", default="fraud_detection", help="MLflow exp name")
    parser.add_argument("--run-name", default=None, help="Name for the MLflow run")

    parser.add_argument("--s3-endpoint-url", help="S3 endpoint URL")
    parser.add_argument("--s3-access-key", help="S3 access key")
    parser.add_argument("--s3-secret-key", help="S3 secret key")

    parser.add_argument("--group-id", default=None, help="Kafka consumet group id")
    parser.add_argument("--bootstrap-server", default="10.0.0.23:9092", help="Kafka bootstrap server url:port")
    parser.add_argument("-n", default=10, type=int, help="Number of messages to process before stop")
    parser.add_argument("--topic", default="transactions", type=str, help="Kafka topic to consume")
    args = parser.parse_args()

    s3_config = None
    if args.s3_endpoint_url and args.s3_access_key and args.s3_secret_key:
        s3_config = {
            'endpoint_url': args.s3_endpoint_url,
            'access_key': args.s3_access_key,
            'secret_key': args.s3_secret_key
        }
        os.environ['AWS_ACCESS_KEY_ID'] = args.s3_access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = args.s3_secret_key
        os.environ['MLFLOW_S3_ENDPOINT_URL'] = args.s3_endpoint_url

    if args.tracking_uri:
        mlflow.set_tracking_uri(args.tracking_uri)

    mlflow.set_experiment(args.experiment_name)

    spark = create_spark_session(s3_config).getOrCreate()

    try:

        consumer = KafkaConsumer(
                bootstrap_servers=args.bootstrap_server,
                group_id=args.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )

        topic = args.topic
        partitions = consumer.partitions_for_topic(topic)

        if partitions is None:
            return

        consumer.subscribe(topics=[args.topic])
        model = load_model_from_mlflow("mlflow-experiment-train_model", alias="champion")
        if not model:
            mlflow.log_text("cannot load model", "kafka__exception.txt")
            sys.exit(1)

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            value_serializer=lambda msg: json.dumps(msg).encode("utf-8"),
            security_protocol='PLAINTEXT',
            request_timeout_ms=30000,
            retries=3,
            api_version=(2, 0, 2),
        )

        start = datetime.now()
        for _ in range(10):
            msg = next(consumer)

            input_data = spark.createDataFrame([Row(**msg.value)])
            res = model.transform(input_data).collect()[0].asDict()
    
            producer.send(
                topic="inference_results",
                value=res,
            )
        stop = datetime.now()
        performance = float(args.n) / (stop - start).total_seconds()

        mlflow.log_text(f"Model performance is '{performance}' entity per second", "kafka_perf.txt")
        producer.flush()
        producer.close()

    except Exception as ex:
        mlflow.log_text(f"{ex.with_traceback()}", "kafka_processing_exeption.txt")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    infer()