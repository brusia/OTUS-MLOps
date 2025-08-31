"""
Общие функции для работы с MLflow и данными
"""
from pathlib import Path
from typing import Any, Final, Union
import pandas as pd
import numpy as np
import mlflow
# import mlflow.sklearn
# from sklearn.feature_extraction.text import CountVectorizer
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.pipeline import Pipeline
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import precision_recall_fscore_support, f1_score, precision_score, recall_score, roc_auc_score
# from scipy.stats import ttest_ind
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import logging

_logger = logging.getLogger(__name__)

# warnings.simplefilter(action='ignore', category=(FutureWarning, UserWarning))

BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/processed_with_airlow/"

OUTPUT_MODELS_DIR: Final[str] = "models/fraud_detection_validate/"
DATE_FORMAT: Final[str] = "%Y%m%d"


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
    # _logger.debug("Start to create Spark-session")
    try:
        builder = (SparkSession
            .builder
            .appName("FraudDetectionOptimizeModel")
        )

        if s3_config and all(k in s3_config for k in ['endpoint_url', 'access_key', 'secret_key']):
            # _logger.debug(f"Conifgure S3 withendpoint_url: {s3_config['endpoint_url']}")
            builder = (builder
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint_url'])
                .config("spark.hadoop.fs.s3a.access.key", s3_config['access_key'])
                .config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key'])
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            )

        # _logger.debug("Spark session configured successfully")
        
        return builder
    except Exception as e: 
        # _logger.exception("Error with session create")
        raise e
    

def evaluate_model(model, test_data):
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(
            labelCol="tx_fraud",
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


def save_model_to_mlflow(model, model_name, metrics, params, data_path, output_model_dir, register_model=False, description=""):
    """
    Сохраняет модель в MLflow
    
    Parameters
    ----------
    model : sklearn estimator
        Обученная модель
    model_name : str
        Имя модели для регистрации
    metrics : dict
        Словарь с метриками
    register_model : bool, default=False
        Регистрировать ли модель в Model Registry
    description : str, default=""
        Описание модели
        
    Returns
    -------
    model_info : dict
        Информация о сохраненной модели (run_id, model_uri)
    """
    with mlflow.start_run() as run:
        # Логируем метрики
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        
        # Сохраняем модель без автоматической регистрации
        mlflow.spark.log_model(model, "model")
        mlflow.log_params(params)
        mlflow.spark.save_model(model, f"s3://{Path(BUCKET_NAME).joinpath(output_model_dir, data_path).as_posix()}")
        
        run_id = run.info.run_id
        model_uri = f"runs:/{run_id}/model"
    
    model_info = {
        "run_id": run_id,
        "model_uri": model_uri
    }
    
    # Регистрируем модель в Model Registry если требуется
    if register_model:
        client = mlflow.tracking.MlflowClient()
        
        # Создаем или получаем зарегистрированную модель
        try:
            client.get_registered_model(model_name)
        except Exception:
            client.create_registered_model(model_name)
        
        # Регистрируем новую версию
        model_details = mlflow.register_model(model_uri, model_name)
        
        # Обновляем описание
        if description:
            client.update_model_version(
                name=model_name,
                version=model_details.version,
                description=description
            )
        
        model_info["version"] = model_details.version
        model_info["model_details"] = model_details
    
    return model_info


def load_model_from_mlflow(model_name, alias="champion"):
    """
    Загружает модель из MLflow Model Registry по алиасу
    
    Parameters
    ----------
    model_name : str
        Имя модели в реестре
    alias : str, default="champion"
        Алиас модели для загрузки
        
    Returns
    -------
    model : sklearn estimator or None
        Загруженная модель или None в случае ошибки
    """
    client = mlflow.tracking.MlflowClient()
    
    try:
        # Пытаемся получить модель по алиасу
        try:
            model_uri = f"models:/{model_name}@{alias}"
            model = mlflow.sklearn.load_model(model_uri)
            print(f"Модель '{model_name}' с алиасом '{alias}' успешно загружена")
            return model
        except Exception:
            # Fallback: ищем модель по тегу alias
            print(f"Поиск модели '{model_name}' по тегу alias='{alias}'")
            model_versions = client.get_latest_versions(model_name)
            
            for version in model_versions:
                if hasattr(version, 'tags') and version.tags.get('alias') == alias:
                    model_uri = f"models:/{model_name}/{version.version}"
                    model = mlflow.sklearn.load_model(model_uri)
                    print(f"Модель '{model_name}' версии {version.version} с тегом alias='{alias}' загружена")
                    return model
            
            print(f"Модель '{model_name}' с алиасом '{alias}' не найдена")
            return None
            
    except Exception as e:
        print(f"Ошибка при загрузке модели: {e}")
        return None


def set_model_alias(model_name, version, alias, description=""):
    """
    Устанавливает алиас для модели
    
    Parameters
    ----------
    model_name : str
        Имя модели
    version : str
        Версия модели
    alias : str
        Алиас для модели (например, "champion", "challenger")
    description : str, default=""
        Описание изменения
    """
    client = mlflow.tracking.MlflowClient()
    
    try:
        # Проверяем доступность метода set_registered_model_alias
        if hasattr(client, 'set_registered_model_alias'):
            client.set_registered_model_alias(
                name=model_name,
                alias=alias,
                version=version
            )
        else:
            # Для старых версий MLflow используем тег
            client.set_model_version_tag(model_name, version, "alias", alias)
    except Exception as e:
        print(f"Ошибка установки алиаса '{alias}': {e}")
        # Fallback через теги
        client.set_model_version_tag(model_name, version, "alias", alias)
    
    if description:
        client.update_model_version(
            name=model_name,
            version=version,
            description=description
        )
    
    print(f"Модель {model_name} версии {version} получила алиас '{alias}'")


def transition_model_stage(model_name, version, stage, description=""):
    """
    Функция для обратной совместимости - переводит стадии в алиасы
    
    Parameters
    ----------
    model_name : str
        Имя модели
    version : str
        Версия модели
    stage : str
        Стадия модели (Production, Staging, Archived)
    description : str, default=""
        Описание перехода
    """
    # Преобразуем стадии в алиасы для обратной совместимости
    stage_to_alias = {
        "Production": "champion",
        "Staging": "challenger", 
        "Archived": "archived"
    }
    
    alias = stage_to_alias.get(stage, stage.lower())
    set_model_alias(model_name, version, alias, description)



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
    # _logger.debug("Start loading data from %s", input_path)
    try:
        # _logger.debug("Read parquet %s", input_path)

        file_path = f"s3a://{Path(BUCKET_NAME).joinpath(INPUT_DATA_DIR, input_path).as_posix()}"

        df = spark.read.parquet(file_path)

        df = df.withColumn("class_weight", F.when(F.col("tx_fraud") == 1, 10.0).otherwise(1.0))
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
       
        return train_df, test_df
    except Exception as e:
        # _logger.exception("Error while loading data.")
        raise e

def get_best_model_metrics(experiment_name):
    client = mlflow.MlflowClient()

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
    client = mlflow.MlflowClient()
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
    # _logger.debug("Prepare features")
    try:
        # _logger.debug("Check columns types")
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
        # _logger.exception("Error with features prepare.")
        raise e