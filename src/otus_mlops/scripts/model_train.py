"""
Script: fraud_detection_model.py
Description: PySpark script for training a fraud detection model and logging to MLflow.
"""

from datetime import datetime
import os
from pathlib import Path
import sys
import traceback
import argparse
from typing import Any, Final, List, Union
import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import logging
import boto3

from otus_mlops.helpers.common import create_spark_session, load_data, prepare_features

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
        mlflow.spark.save_model(model, f"s3a://{Path(BUCKET_NAME).joinpath(OUTPUT_MODELS_DIR, data_path, f'model_{datetime.now().strftime(DATE_FORMAT)}')}")

    except Exception as e:
        _logger.exception("Error with saving model.")
        mlflow.log_text(f"{e.with_traceback()}", "exeption_while_saving_model.txt")
        raise e


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

        mlflow.log_text("success", "success.txt")
        # model, metrics = train_model(train_df, test_df, feature_cols, args.model_type, run_name)
# 
        # save_model(model, data_path, s3_client)

        # if args.auto_register:
            # compare_and_register_model(metrics, args.experiment_name)

    except Exception as ex:
        mlflow.log_text(f"{ex.with_traceback()}", "model_train_exeption.txt")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()