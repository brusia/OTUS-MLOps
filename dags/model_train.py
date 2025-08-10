"""
DAG: data_pipeline
Description: DAG for processing data with Dataproc and PySpark.
"""
from typing import Union, Final
# from airflow.decorators import task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
    InitializationAction
)

YC_ZONE = Variable.get("YC_ZONE")
YC_CLOUD_ID = "brusiacloud"
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")


YC_TOKEN = Variable.get("YA_TOKEN")

S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")

S3_DP_LOGS_BUCKET = S3_BUCKET_NAME + "/logs/airflow/"

DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")
INPUT_DATA_DIR: Final[str] = "data/raw"

MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_NAME = "test-mlflow-experiment-train"

S3_INPUT_DATA_BUCKET = f"s3a://{S3_BUCKET_NAME}/test/input_data"
# S3_OUTPUT_MODEL_BUCKET = f"s3a://{S3_BUCKET_NAME}/test/models"
S3_SRC_BUCKET = f"s3a://{S3_BUCKET_NAME}/src"
S3_DP_LOGS_BUCKET = f"s3a://{S3_BUCKET_NAME}/test/logs/airflow_logs/"
S3_VENV_ARCHIVE = f"s3a://{S3_BUCKET_NAME}/src/venvs/venv.tar.gz"


envs = {"S3_ENDPOINT_URL": S3_ENDPOINT_URL, "S3_BUCKET_NAME": S3_BUCKET_NAME, "S3_ACCESS_KEY": S3_ACCESS_KEY,
"S3_SECRET_KEY": S3_SECRET_KEY, "YC_TOKEN": YC_TOKEN, "YC_ZONE": YC_ZONE, "YC_SUBNET_ID": YC_SUBNET_ID, "YC_FOLDER_ID": YC_FOLDER_ID,
"YC_SSH_PUBLIC_KEY": YC_SSH_PUBLIC_KEY, "YC_CLOUD_ID": YC_CLOUD_ID, "DP_SECURITY_GROUP_ID": DP_SECURITY_GROUP_ID, "DP_SA_ID": DP_SA_ID,
"S3_DP_LOGS_BUCKET": S3_DP_LOGS_BUCKET }



def get_cluster_id_from_xcom(**kwargs):
    task_instance = kwargs['ti']
    cluster_id = task_instance.xcom_pull(key='cluster_id', task_ids='create_cluster')
    if not cluster_id:
        raise ValueError("Key 'cluster_id' was not found in XCom!")

    return cluster_id


with DAG(
    dag_id="testing-train-model",
    start_date=datetime(year=2025, month=8, day=3),
    # schedule=timedelta(days=1),
    catchup=False
) as dag:
    delete_cluster_using_bash = BashOperator(
        task_id="delete_cluster_with_cluster_name_using_bash",
        bash_command="airflow/script/delete_cluster.sh",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    cluster_name = "tmp-spark"
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        folder_id=YC_FOLDER_ID,
        cluster_name=cluster_name,
        cluster_description="Temp Spark Cluster",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_DP_LOGS_BUCKET,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.0",

        # masternode
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=20,

        # datanodes
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=2,

        # computenodes
        computenode_count=0,

        # software
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        # initialization_actions = [ 
        #     InitializationAction(uri = f"s3a://{S3_BUCKET_NAME}/scripts/prepare_cluster.sh",
        #         timeout=700,
        #         args=[]
        #         # args=[S3_ACCESS_KEY, S3_SECRET_KEY]
        #         )
        #     ]
    )


    get_cluster_info = PythonOperator(
        task_id='get_cluster_info',
        python_callable=get_cluster_id_from_xcom,
        trigger_rule=TriggerRule.ALL_DONE
    )

    spark_processing = DataprocCreatePysparkJobOperator(
        task_id="cluster-pyspark-task",
        cluster_id="{{ ti.xcom_pull(task_ids='get_cluster_info') }}",
        main_python_file_uri=f"s3a://{S3_BUCKET_NAME}/src/model_train.py",

        args=[
            "--tracking-uri", MLFLOW_TRACKING_URI,
            "--experiment-name", MLFLOW_EXPERIMENT_NAME,
            "--auto-register",
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
            "--run-name", f"training_{datetime.now().strftime('%Y%m%d_%H%M')}"
        ],
        properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.dist.archives': f'{S3_VENV_ARCHIVE}#.venv',
            'spark.yarn.appMasterEnv.PYSPARK_PYTHON': './.venv/bin/python3',
            'spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON': './.venv/bin/python3',
        },

        trigger_rule=TriggerRule.ALL_DONE
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_id="{{ ti.xcom_pull(task_ids='get_cluster_info') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> get_cluster_info >> spark_processing >> delete_spark_cluster >> delete_cluster_using_bash
