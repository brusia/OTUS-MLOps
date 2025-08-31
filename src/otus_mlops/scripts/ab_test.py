"""
Script: fraud_detection_model.py
Description: PySpark script for training a fraud detection model and logging to MLflow.
"""

from datetime import datetime
import os
from pathlib import Path
import sys
import argparse
from typing import Any, Final, List, Union

import numpy as np
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import logging
import boto3

from functools import partial
from hyperopt import fmin, tpe, hp, SparkTrials, STATUS_OK, Trials
from pyspark.sql import functions as F

_logger = logging.getLogger(__name__)

BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/processed_with_airlow/"

OUTPUT_MODELS_DIR: Final[str] = "models/fraud_detection_validate/"
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



def calculate_class_weights(df, label_col="tx_fraud"):
    class_counts = df.groupBy(label_col).count().collect()
    
    count_dict = {row[label_col]: row['count'] for row in class_counts}
    
    total = sum(count_dict.values())
    
    weights = {class_label: total / (len(count_dict) * count) 
            for class_label, count in count_dict.items()}
    
    return weights


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

        class_weights = calculate_class_weights(df, "tx_fraud")

        df_weighted = df.withColumn(
            "weight", 
            F.when(F.col("tx_fraud") == 1, class_weights[1]).otherwise(class_weights[0])
        )

        train_df, test_df = df_weighted.randomSplit([0.8, 0.2], seed=42)
       
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

        feature_cols = ["tx_amount", "weight"]

        for col in train_df.columns:
            null_count = train_df.filter(train_df[col].isNull()).count()
            if null_count > 0:
                _logger.warning("Column '%s' contains '%d' null values", col, null_count)

        return train_df, test_df, feature_cols
    except Exception as e:
        _logger.exception("Error with features prepare.")
        raise e


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


def compare_and_register_model(new_metrics, experiment_name, model_name):
    client = MlflowClient()
    best_metrics = get_best_model_metrics(experiment_name)
    # model_name = f"{experiment_name}_model"

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


def load_model_from_mlflow(model_name, alias="champion"):
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



def evaluate_model(model, test_data):
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(
            labelCol="tx_fraud",
            weightCol="weight", 
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
    
    auc = evaluator.evaluate(predictions)

    tp = predictions.filter((F.col("prediction") == 1) & (F.col("tx_fraud") == 1)).count()
    fp = predictions.filter((F.col("prediction") == 1) & (F.col("tx_fraud") == 0)).count()
    fn = predictions.filter((F.col("prediction") == 0) & (F.col("tx_fraud") == 1)).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    metrics = {
        "auc": auc,
        "precision": precision,
        "recall": recall,
        "f1": f1_score,
        "TP": tp,
        "FP": fp,
        "FN": fn
    }

    return metrics




def optimize_hyperparameters(spark, df_train, df_test, feature_cols):
    df_train.createOrReplaceTempView("temp_train_data")
    df_test.createOrReplaceTempView("temp_test_data")

    def objective(params):
        train_data = spark.table("temp_train_data")
        test_data = spark.table("temp_test_data")
            
        try:
            params['numTrees'] = int(params['numTrees'])
            params['maxDepth'] = int(params['maxDepth'])
            params['maxBins'] = int(params['maxBins'])
            params['minInstancesPerNode'] = int(params['minInstancesPerNode'])
            
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")

            scaler = StandardScaler(
                inputCol="features_raw",
                outputCol="features",
                withStd=True,
                withMean=True
            )

            rf = RandomForestClassifier(
                featuresCol="features",
                labelCol="tx_fraud",
                weightCol="weight", 
                numTrees=params['numTrees'],
                maxDepth=params['maxDepth'],
                maxBins=params['maxBins'],
                minInstancesPerNode=params['minInstancesPerNode'],
                subsamplingRate=params['subsamplingRate'],
                featureSubsetStrategy=params['featureSubsetStrategy'],
                impurity=params['impurity'],
                seed=42
            )
            
            pipeline = Pipeline(stages=[assembler, scaler, rf])

            model = pipeline.fit(train_data)

            predictions = model.transform(test_data)

            evaluator = BinaryClassificationEvaluator(
                labelCol="tx_fraud",
                weightCol="weight", 
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC"
            )
            
            auc = evaluator.evaluate(predictions)
            
            tp = predictions.filter((F.col("prediction") == 1) & (F.col("tx_fraud") == 1)).count()
            fp = predictions.filter((F.col("prediction") == 1) & (F.col("tx_fraud") == 0)).count()
            fn = predictions.filter((F.col("prediction") == 0) & (F.col("tx_fraud") == 1)).count()
            
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            

            if recall > 0.8:
                combined_metric = 0.7 * auc + 0.3 * precision
            else:
                combined_metric = 0.7 * auc
            
            loss = 1 - combined_metric
            
            return {
                'loss': loss,
                'status': STATUS_OK,
                'auc': auc,
                'precision': precision,
                'recall': recall,
                'f1': f1_score,
                'params': params
            }
        
        except Exception as e:
            print(f"Error in trial: {e}")
            return {'loss': 1.0, 'status': STATUS_OK, 'info': f"{e}"}


    search_space = {
        'numTrees': hp.quniform('numTrees', 50, 200, 10),
        'maxDepth': hp.quniform('maxDepth', 5, 15, 1),
        'maxBins': hp.quniform('maxBins', 20, 50, 5),
        'minInstancesPerNode': hp.quniform('minInstancesPerNode', 1, 10, 1),
        'minInstancesPerNode': hp.quniform('minInstancesPerNode', 1, 10, 1),
        'subsamplingRate': hp.uniform('subsamplingRate', 0.6, 1.0),
        'featureSubsetStrategy': hp.choice('featureSubsetStrategy', ['auto', 'sqrt', 'log2']),
        'impurity': hp.choice('impurity', ['gini', 'entropy'])
    }

    spark_trials = SparkTrials()
    # mlflow.log_text(f"trials: {spark_trials}", "optimize_2.txt")
    # Запуск оптимизации
    print("Запуск оптимизации гиперпараметров...")
    best = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,
        max_evals=15,
        trials=spark_trials,
        verbose=True
    )

    spark.catalog.dropTempView("temp_train_data")
    spark.catalog.dropTempView("temp_test_data")
    results = []

    for trial in spark_trials.trials:
        if 'result' in trial and 'auc' in trial['result']:
            results.append({
                'params': trial['result']['params'],
                'auc': trial['result']['auc'],
                'precision': trial['result']['precision'],
                'recall': trial['result']['recall'],
                'loss': trial['result']['loss']
            })

    results.sort(key=lambda x: x['auc'], reverse=True)

    # Преобразуем лучшие параметры
    best_params = {
        'numTrees': int(best['numTrees']),
        'maxDepth': int(best['maxDepth']),
        'maxBins': int(best['maxBins']),
        'minInstancesPerNode': int(best['minInstancesPerNode']),
        'subsamplingRate': best['subsamplingRate'],
        'featureSubsetStrategy': ['auto', 'sqrt', 'log2'][best['featureSubsetStrategy']],
        'impurity': ['gini', 'entropy'][best['impurity']]
    }

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    final_rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="tx_fraud",
        weightCol="weight", 
        **best_params,
        seed=42
    )
    final_pipeline = Pipeline(stages=[assembler, scaler, final_rf])

    final_model = final_pipeline.fit(df_train)
    metrics = evaluate_model(final_model, df_test)

    return final_model, best_params, metrics



def save_model_to_mlflow(model, model_name, metrics, params, data_path, output_model_dir, run_name, register_model=False, description=""):
    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        print(f"MLflow Run ID: {run_id}")

        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        
        mlflow.spark.log_model(model, model_name)
        mlflow.log_params(params)

        model.write().overwrite().save(f"s3a://{Path(BUCKET_NAME).joinpath(OUTPUT_MODELS_DIR, data_path, f'model_{datetime.now().strftime(DATE_FORMAT)}')}")
        
        run_id = run.info.run_id
        model_uri = f"runs:/{run_id}/model"
    
    model_info = {
        "run_id": run_id,
        "model_uri": model_uri
    }

    if register_model:
        client = mlflow.tracking.MlflowClient()
        
        try:
            client.get_registered_model(model_name)
        except Exception:
            client.create_registered_model(model_name)
        
        model_details = mlflow.register_model(model_uri, model_name)
        
        if description:
            client.update_model_version(
                name=model_name,
                version=model_details.version,
                description=description
            )
        
        model_info["version"] = model_details.version
        model_info["model_details"] = model_details
    
    return model_info


def compare_models(baseline_predictions, candidate_predictions, metric_func, bootstrap_iterations, ci, sample_fraction):
    baseline_ci = bootstrap_ci(baseline_predictions, metric_func, bootstrap_iterations, ci, sample_fraction)
    candidate_ci = bootstrap_ci(candidate_predictions, metric_func, bootstrap_iterations, ci, sample_fraction)
    
    baseline_point_estimate = metric_func(baseline_predictions)
    candidate_point_estimate = metric_func(candidate_predictions)

    # TODO: add logs for MLFlow
    print(f"Baseline model: {baseline_point_estimate:.4f}, 95% CI = [{baseline_ci[0]:.4f}, {baseline_ci[1]:.4f}]")
    print(f"Candidate model: {candidate_point_estimate:.4f}, 95% CU = [{candidate_ci[0]:.4f}, {candidate_ci[1]:.4f}]")      

    if baseline_ci[1] < candidate_ci[0]:
        improvement = (candidate_point_estimate - baseline_point_estimate) / baseline_point_estimate * 100
        return True
    elif candidate_ci[1] < baseline_ci[0]:
        deterioration = (baseline_point_estimate - candidate_point_estimate) / candidate_point_estimate * 100
        return False
    else:
        return None


def ab_test_models(
        baseline_model,
        candidate_model,
        test_data,
        run_name,
        bootstrap_iterations=100,
        sample_fraction=0.01
    ):
    baseline_predictions = baseline_model.transform(test_data)
    candidate_predictions = candidate_model.transform(test_data)

    evaluator_auc = BinaryClassificationEvaluator(
            labelCol="tx_fraud",
            rawPredictionCol="rawPrediction",
            weightCol="weight", 
            metricName="areaUnderROC"
        )
    evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="tx_fraud",
            weightCol="weight", 
            predictionCol="prediction",
            metricName="accuracy"
        )
    evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="tx_fraud",
            weightCol="weight", 
            predictionCol="prediction",
            metricName="f1"
        )
    
    metrics_to_compare = [
        (evaluator_auc, "AUC"),
        (evaluator_acc, "Accuracy"),
        (evaluator_f1, "F1-score")
    ]


    results = {}
    for evaluator, metric_name in metrics_to_compare:
        results[metric_name] = compare_models(
            baseline_predictions, 
            candidate_predictions, 
            evaluator.evaluate, 
            bootstrap_iterations,
            95,
            sample_fraction
        )

    improvements = sum(1 for v in results.values() if v is True)
    deteriorations = sum(1 for v in results.values() if v is False)
    inconclusive = sum(1 for v in results.values() if v is None)

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        print(f"MLflow Run ID: {run_id}")

        mlflow.log_metric("improvements", improvements)
        mlflow.log_metric("deteriorations", deteriorations)
        mlflow.log_metric("inconclusive", inconclusive)


    should_deploy = False
    if improvements > deteriorations and improvements > 0:
        should_deploy=True
    elif deteriorations > improvements:
        print("Production model is better.")
    else:
        print("Needs to analyse deeper.")

    return should_deploy

def bootstrap_ci(predictions, metric_func, n_bootstraps=1000, ci=95, sample_fraction=0.1):
    bootstrapped_metrics = []
    
    for _ in range(n_bootstraps):
        sample = predictions.sample(withReplacement=True, fraction=sample_fraction)
        
        metric_value = metric_func(sample)
        bootstrapped_metrics.append(metric_value)
    
    lower = np.percentile(bootstrapped_metrics, (100 - ci) / 2)
    upper = np.percentile(bootstrapped_metrics, 100 - (100 - ci) / 2)
    
    return lower, upper


def main():
    """
    Main function to run the fraud detection model training.
    """
    parser = argparse.ArgumentParser(description="Fraud Detection Model Training")
    parser.add_argument("--model-type", default="rf", help="Model type (rf or lr)")

    parser.add_argument("--tracking-uri", help="MLflow tracking URI")
    parser.add_argument("--experiment-name", default="fraud_detection", help="MLflow exp name")
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

        production_model = load_model_from_mlflow("mlflow-experiment-train_model", alias="champion")

        if production_model is None:
            return None

        candidate_model, best_params, metrics = optimize_hyperparameters(spark, train_df, test_df, feature_cols)

        model_name = "optimized_model_candidate"
        save_model_to_mlflow(candidate_model, model_name, metrics, best_params, data_path, Path(OUTPUT_MODELS_DIR).joinpath("candidates").as_posix(), run_name, True)

        if ab_test_models(production_model, candidate_model, test_df, run_name, 10, 0.01):
            compare_and_register_model(metrics, args.experiment_name, model_name)

    except Exception as ex:
        mlflow.log_text(f"{ex.with_traceback()}", "model_train_exeption.txt")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()