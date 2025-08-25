"""
Script: fraud_detection_model.py
Description: PySpark script for training a fraud detection model and logging to MLflow.
"""

from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
import argparse
from typing import Any, Final, List, Union
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import logging
import boto3

from pyspark.sql import functions as F

_logger = logging.getLogger(__name__)

BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/processed_with_airlow/"

OUTPUT_MODELS_DIR: Final[str] = "models/fraud_detection/"
DATE_FORMAT: Final[str] = "%Y%m%d"


TARGET_COLUMN_NAMES: List[str] = ["tx_fraud", "tx_fraud_scenario"]


def get_next_file_to_process(s3_client: Any) -> Union[str, None]:
    input_files = sorted([Path(obj["Prefix"]).relative_to(INPUT_DATA_DIR) for obj in s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=INPUT_DATA_DIR, Delimiter='/').get("CommonPrefixes", {})])
    processed_files = sorted([Path(obj['Prefix']).relative_to(OUTPUT_MODELS_DIR).as_posix() for obj in s3_client.list_objects(Bucket=BUCKET_NAME, 
    Prefix=OUTPUT_MODELS_DIR, Delimiter='/').get('CommonPrefixes', [])])
    last_date_processed = processed_files[-1] if processed_files else None

    next_file_index = list(map(lambda x: x.stem, input_files)).index(last_date_processed) + 1 if last_date_processed else 0
    if next_file_index < len(input_files):
        next_file= input_files[next_file_index]
        return next_file.as_posix()
    else:
        # _logger.info("All the data processed. There are no data to process.")
        return None


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
            .appName("FraudDetectionModel")
        )

        if s3_config and all(k in s3_config for k in ['endpoint_url', 'access_key', 'secret_key']):
            _logger.debug(f"Conifgure S3 withendpoint_url: {s3_config['endpoint_url']}")
            builder = (builder
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                # .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
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


def load_data(spark, input_path):
    """
    Load and prepare the fraud detection dataset.

    Parameters
    ----------
    spark : SparkSession
        Spark session
    input_path : str
        Path to the input data

    Returns
    -------
    tuple
        (train_df, test_df) - Spark DataFrames for training and testing
    """
    _logger.debug("Start loading data from %s", input_path)
    try:
        _logger.debug("Read parquet %s", input_path)

        file_path = f"s3a://{Path(BUCKET_NAME).joinpath(INPUT_DATA_DIR, input_path).as_posix()}"

        df = spark.read.parquet(file_path)

        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
       
        return train_df, test_df
    except Exception as e:
        _logger.exception("Error while loading data.")
        raise e


def prepare_features(train_df, test_df):
    """
    Prepare features for model training.

    Parameters
    ----------
    train_df : DataFrame
        Training DataFrame
    test_df : DataFrame
        Testing DataFrame

    Returns
    -------
    tuple
        (train_df, test_df, feature_cols) - Prepared DataFrames and feature column names
    """
    _logger.debug("Prepare features")
    try:
        _logger.debug("Check columns types")
        dtypes = dict(train_df.dtypes)

        feature_cols = ["tx_amount"]
        
                        # [col for col in train_df.columns
                        # if col not in TARGET_COLUMN_NAMES and dtypes[col] != 'string']

        for col in train_df.columns:
            null_count = train_df.filter(train_df[col].isNull()).count()
            if null_count > 0:
                _logger.warning("Column '%s' contains '%d' null values", col, null_count)

        return train_df, test_df, feature_cols
    except Exception as e:
        _logger.exception("Error with features prepare.")
        raise e


def train_model(train_df, test_df, feature_cols, model_type="rf", run_name="fraud_detection_model"):
    """
    Train a fraud detection model and log metrics to MLflow.

    Parameters
    ----------
    train_df : DataFrame
        Training DataFrame
    test_df : DataFrame
        Testing DataFrame
    feature_cols : list
        List of feature column names
    model_type : str
        Model type to train ('rf' for Random Forest, 'lr' for Logistic Regression)
    run_name : str
        Name for the MLflow run

    Returns
    -------
    tuple
        (best_model, metrics) - Best model and its performance metrics
    """
    try:
        # Create feature vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )

        # Select model based on type
        classifier = RandomForestClassifier(
            labelCol="tx_fraud",
            featuresCol="features",
            numTrees=10,
            maxDepth=5
        )
        param_grid = (ParamGridBuilder()
            .addGrid(classifier.numTrees, [10, 20])
            .addGrid(classifier.maxDepth, [5, 10])
            .build()
        )

        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, classifier])

        # Create evaluators
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="tx_fraud",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="tx_fraud",
            predictionCol="prediction",
            metricName="accuracy"
        )
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="tx_fraud",
            predictionCol="prediction",
            metricName="f1"
        )

        # Create cross-validator
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator_auc,
            numFolds=3
        )

        # Start MLflow run
        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
            print(f"MLflow Run ID: {run_id}")

            # Log model parameters
            mlflow.log_param("numTrees_options", [10, 20])
            mlflow.log_param("maxDepth_options", [5, 10])

            # Train the model
            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel

            # Make predictions on test data
            predictions = best_model.transform(test_df)

            # Calculate metrics
            auc = evaluator_auc.evaluate(predictions)
            accuracy = evaluator_acc.evaluate(predictions)
            f1 = evaluator_f1.evaluate(predictions)

            # Log metrics
            mlflow.log_metric("auc", auc)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1", f1)

            # Log best model parameters
            rf_model = best_model.stages[-1]
            try:
                num_trees = rf_model.getNumTrees
                max_depth = rf_model.getMaxDepth()
                print(f"DEBUG: numTrees={num_trees}, maxDepth={max_depth}")
                mlflow.log_param("best_numTrees", num_trees)
                mlflow.log_param("best_maxDepth", max_depth)
            except Exception as e:
                pass

            # Log the model
            mlflow.spark.log_model(best_model, "model")

            # Print metrics
            print(f"AUC: {auc}")
            print(f"Accuracy: {accuracy}")
            print(f"F1 Score: {f1}")

            metrics = {
                "run_id": run_id,
                "auc": auc,
                "accuracy": accuracy,
                "f1": f1
            }

            return best_model, metrics
    except Exception as e:
        mlflow.log_text(f"Traceback: {traceback.format_exc()}", "exception_while_training.txt")
        raise


def save_model(model, data_path, s3_client):
    """
    Save the trained model to the specified path.

    Parameters
    ----------
    model : PipelineModel
        Trained model
    data_path: str
        The part of path where the model will be saved
    s3_client : boto3.Client
        client to save model on S3 bucket
    """

    try:
        model.write().overwrite().save(f"s3a://{Path(BUCKET_NAME).joinpath(OUTPUT_MODELS_DIR, data_path, f'model_{datetime.now().strftime(DATE_FORMAT)}')}")

    except Exception as e:
        _logger.exception("Error with saving model.")
        mlflow.log_text(f"{e.with_traceback()}", "exeption_while_saving_model.txt")
        raise e


def get_best_model_metrics(experiment_name):
    client = MlflowClient()

    try:
        experiment = client.get_experiment_by_name(experiment_name)
        if not experiment:
            return None
    except Exception as e:
        _logger.exception("Error with getting exeriment.")
        mlflow.log_text(f"{e.with_traceback()}", "exeption_while_gettings_experiment_for_best_model.txt")
        return None

    try:
        model_name = f"{experiment_name}_model"

        try:
            registered_model = client.get_registered_model(model_name)
        except Exception as e:
            return None

        model_versions = client.get_latest_versions(model_name)
        champion_version = None

        for version in model_versions:
            if hasattr(version, 'aliases') and "champion" in version.aliases:
                champion_version = version
                break
            elif hasattr(version, 'tags') and version.tags.get('alias') == "champion":
                champion_version = version
                break

        if not champion_version:
            return None

        champion_run_id = champion_version.run_id

        run = client.get_run(champion_run_id)
        metrics = {
            "run_id": champion_run_id,
            "auc": run.data.metrics["auc"],
            "accuracy": run.data.metrics["accuracy"],
            "f1": run.data.metrics["f1"]
        }

        return metrics
    except Exception as e:
        _logger.exception("Error while getting best model.")
        mlflow.log_text(f"{e.with_traceback()}", "exeption_while_getting_best_model.txt")
        return None


def compare_and_register_model(new_metrics, experiment_name):
    client = MlflowClient()
    best_metrics = get_best_model_metrics(experiment_name)
    model_name = f"{experiment_name}_model"

    try:
        client.get_registered_model(model_name)
    except Exception as e:
        client.create_registered_model(model_name)

    run_id = new_metrics["run_id"]
    model_uri = f"runs:/{run_id}/model"
    model_details = mlflow.register_model(model_uri, model_name)
    new_version = model_details.version

    should_promote = False

    if not best_metrics:
        should_promote = True
    else:
        if new_metrics["auc"] > best_metrics["auc"]:
            should_promote = True
            improvement = (new_metrics["auc"] - best_metrics["auc"]) / best_metrics["auc"] * 100

    if should_promote:
        try:
            if hasattr(client, 'set_registered_model_alias'):
                client.set_registered_model_alias(model_name, "champion", new_version)
            else:
                client.set_model_version_tag(model_name, new_version, "alias", "champion")
        except Exception as e:
            client.set_model_version_tag(model_name, new_version, "alias", "champion")

        return True

    try:
        if hasattr(client, 'set_registered_model_alias'):
            client.set_registered_model_alias(model_name, "challenger", new_version)
        else:
            client.set_model_version_tag(model_name, new_version, "alias", "challenger")
    except Exception as e:
        client.set_model_version_tag(model_name, new_version, "alias", "challenger")

    return False


def main():
    """
    Main function to run the fraud detection model training.
    """
    parser = argparse.ArgumentParser(description="Fraud Detection Model Training")
    parser.add_argument("--model-type", default="rf", help="Model type (rf or lr)")

    parser.add_argument("--tracking-uri", help="MLflow tracking URI")
    parser.add_argument("--experiment-name", default="fraud_detection", help="MLflow exp name")
    parser.add_argument("--auto-register", action="store_true", help="Automatically register")
    parser.add_argument("--run-name", default=None, help="Name for the MLflow run")

    os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

    parser.add_argument("--s3-endpoint-url", help="S3 endpoint URL")
    parser.add_argument("--s3-access-key", help="S3 access key")
    parser.add_argument("--s3-secret-key", help="S3 secret key")

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
        s3_client = boto3.session.Session(aws_access_key_id=args.s3_access_key,
            aws_secret_access_key=args.s3_secret_key).client(service_name="s3", endpoint_url=args.s3_endpoint_url)
        
        data_path = get_next_file_to_process(s3_client=s3_client)
        train_df, test_df = load_data(spark, data_path)
        train_df, test_df, feature_cols = prepare_features(train_df, test_df)

        run_name = args.run_name or f"fraud_detection_{args.model_type}_{data_path}"
        model, metrics = train_model(train_df, test_df, feature_cols, args.model_type, run_name)

        save_model(model, data_path, s3_client)

        if args.auto_register:
            compare_and_register_model(metrics, args.experiment_name)

    except Exception as ex:
        mlflow.log_text(f"{ex.with_traceback()}", "model_train_exeption.txt")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()