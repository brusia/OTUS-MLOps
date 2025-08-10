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
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import logging
import boto3

_logger = logging.getLogger(__name__)

BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/processed_with_airlow"
OUTPUT_MODELS_DIR: Final[str] = "models/fraud_detection"
DATE_FORMAT: Final[str] = "%Y%m%d"


TARGET_COLUMN_NAMES: List[str] = ["tx_fraud", "tx_fraud_scenario"]


def get_next_file_to_process(s3_client: Any) -> Union[Path, None]:
    input_files = [Path(obj['Key']) for obj in s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=INPUT_DATA_DIR, Delimiter='/').get('CommonPrefixes', [])]
    processed_files = sorted([Path(obj['Prefix']).relative_to(OUTPUT_MODELS_DIR).as_posix() for obj in s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=OUTPUT_MODELS_DIR, Delimiter='/').get('CommonPrefixes', [])])
    last_date_processed = processed_files[-1] if processed_files else None

    next_file_index = list(map(lambda x: x.stem, input_files)).index(last_date_processed) + 1 if last_date_processed else 0
    if next_file_index < len(input_files):
        next_file= input_files[next_file_index]
        return next_file
    else:
        _logger.info("All the data processed. There are no data to process.")
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
        # Создаем базовый Builder
        builder = (SparkSession
            .builder
            .appName("FraudDetectionModel")
        )

        # Если передана конфигурация S3, добавляем настройки
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
        raise


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
        # Load the data
        _logger.debug("Read parquet %s", input_path)
        df = spark.read.parquet(input_path, header=True, inferSchema=True)

        # # Print schema and basic statistics
        _logger.debug("Dataset Schema:")
        _logger.debug(df.printSchema())
        
        _logger.debug("Total records: %s", df.count())
        # Split the data into training and testing sets
        _logger.debug("Splitting dataset into train and test sets")
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        _logger.debug("Training set size: %s", train_df.count())
        _logger.debug("Testing set size: %s", test_df.count())

        return train_df, test_df
    except Exception as e:
        _logger.exception("Error while loading data.")
        raise


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

        feature_cols = [col for col in train_df.columns
                        if col not in TARGET_COLUMN_NAMES and dtypes[col] != 'string']

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
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )

        classifier = RandomForestClassifier(
            labelCol="fraud",
            featuresCol="features",
            numTrees=10,
            maxDepth=5
        )
        param_grid = (ParamGridBuilder()
            .addGrid(classifier.numTrees, [10, 20])
            .addGrid(classifier.maxDepth, [5, 10])
            .build()
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, classifier])

        # тут можно сделать несколько оценциков для каждой целевой переменной (fraud, fraud_scenario)
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="fraud",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="fraud",
            predictionCol="prediction",
            metricName="accuracy"
        )
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="fraud",
            predictionCol="prediction",
            metricName="f1"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator_auc,
            numFolds=3
        )

        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
           
            mlflow.log_param("numTrees_options", [10, 20])
            mlflow.log_param("maxDepth_options", [5, 10])

            cv_model = cv.fit(train_df)
            best_model = cv_model.bestModel
            
            predictions = best_model.transform(test_df)

            auc = evaluator_auc.evaluate(predictions)
            accuracy = evaluator_acc.evaluate(predictions)
            f1 = evaluator_f1.evaluate(predictions)

            mlflow.log_metric("auc", auc)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1", f1)

            rf_model = best_model.stages[-1]
            try:
                num_trees = rf_model.getNumTrees
                max_depth = rf_model.getMaxDepth()

                mlflow.log_param("best_numTrees", num_trees)
                mlflow.log_param("best_maxDepth", max_depth)
            except Exception as e:
                _logger.exception("Model parameters are incorrect.")
                for attr in dir(rf_model):
                    if not attr.startswith('_'):
                        _logger.error("Ploblem with attribute '%s'", attr)

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
        _logger.exception("Error with model train")
        raise e


def save_model(model, output_path):
    """
    Save the trained model to the specified path.

    Parameters
    ----------
    model : PipelineModel
        Trained model
    output_path : str
        Path to save the model
    """
    try:
        model.write().overwrite().save(output_path)
    except Exception as e:
        _logger.exception("Error with saving model.")
        raise e


def get_best_model_metrics(experiment_name):
    """
    Получает метрики лучшей модели из MLflow с алиасом 'champion'

    Parameters
    ----------
    experiment_name : str
        Имя эксперимента MLflow

    Returns
    -------
    dict
        Метрики лучшей модели или None, если модели нет
    """
    client = MlflowClient()

    try:
        experiment = client.get_experiment_by_name(experiment_name)
        if not experiment:
            return None
    except Exception as e:
        _logger.exception("Error with getting exeriment.")
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
        return None


def compare_and_register_model(new_metrics, experiment_name):
    """
    Сравнивает новую модель с лучшей в MLflow и регистрирует, если она лучше

    Parameters
    ----------
    new_metrics : dict
        Метрики новой модели
    experiment_name : str
        Имя эксперимента MLflow

    Returns
    -------
    bool
        True, если новая модель была зарегистрирована как лучшая
    """
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

        run_name = args.run_name or f"fraud_detection_{args.model_type}_{os.path.basename(args.input)}"
        model, metrics = train_model(train_df, test_df, feature_cols, args.model_type, run_name)

        save_model(model, f"s3a://{Path(BUCKET_NAME).joinpath(OUTPUT_MODELS_DIR, data_path, f'model_{datetime.now().strftime(DATE_FORMAT)}')}")

        if args.auto_register:
            compare_and_register_model(metrics, args.experiment_name)

    except Exception:
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()