"""
DAG: data_pipeline
Description: DAG for processing data with Dataproc and PySpark.
"""

import uuid
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator
)

# # Общие переменные для вашего облака
YC_ZONE = Variable.get("YC_ZONE")
YC_CLOUD_ID = "brusiacloud"
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

# Переменные для подключения к Object Storage

YC_TOKEN = Variable.get("YA_TOKEN")

S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_INPUT_DATA_BUCKET = S3_BUCKET_NAME + "/airflow/"     # YC S3 bucket for input data
S3_SRC_BUCKET = S3_BUCKET_NAME[:]                      # YC S3 bucket for pyspark source files
S3_DP_LOGS_BUCKET = S3_BUCKET_NAME + "/airflow_logs/"   # YC S3 bucket for Data Proc logs

# # Переменные необходимые для создания Dataproc кластера
# DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
# DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
# DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

envs = {"S3_ENDPOINT_URL": S3_ENDPOINT_URL, "S3_BUCKET_NAME": S3_BUCKET_NAME, "S3_ACCESS_KEY": S3_ACCESS_KEY,
"S3_SECRET_KEY": S3_SECRET_KEY, "YC_TOKEN": YC_TOKEN, "YC_ZONE": YC_ZONE, "YC_SUBNET_ID": YC_SUBNET_ID, "YC_FOLDER_ID": YC_FOLDER_ID,
"YC_SSH_PUBLIC_KEY": YC_SSH_PUBLIC_KEY, "YC_CLOUD_ID": YC_CLOUD_ID}


# # Получение cluster_id из XCom
# def get_cluster_id(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster', key='cluster_id')
#     return cluster_id


# @task.bash(env=envs)
# def remove_cluster_bash():
#     return "airflow/script/my_script.sh"


def get_cluster_id_from_xcom(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(key='cluster_id', task_ids='create_cluster')
    if not cluster_id:
        raise ValueError("Key 'cluster_id' was not found in XCom!")
    return cluster_id


# Настройки DAG
with DAG(
    dag_id="data_pipeline",
    start_date=datetime(year=2025, month=6, day=10),
    # schedule_interval=timedelta(minutes=60),
    catchup=False
) as dag:
    # setup hadoop installation
    
    # Задача для создания подключений
    # python_step = PythonOperator(
    #     task_id="setup_connections",
    #     python_callable=run_python_func,
    # )

    # bash_step = BashOperator(
    #     task_id="run_after_loop",
    #     bash_command="echo 'This is echo command here.'\necho '${S3_BUCKET_NAME}'",
    # )

    delete_cluster_using_bash = BashOperator(
        task_id="delete_cluster_with_cluster_name_using_bash",
        bash_command="airflow/script/my_script.sh",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # bash_from_python_script = bash_from_python()

    # bash_from_local = s3_ls()
    # 1 этап: создание Dataproc клаcтера
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
    )


    get_cluster_id_task = PythonOperator(
        task_id='get_cluster_id',
        python_callable=get_cluster_id_from_xcom,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    spark_processing = DataprocCreatePysparkJobOperator(
        task_id="cluster-pyspark-task",
        cluster_id="{{ ti.xcom_pull(task_ids='get_cluster_id') }}",
        main_python_file_uri=f"s3a://{S3_SRC_BUCKET}/src/pyspark_script.py",
        args=["--bucket", S3_BUCKET_NAME],
        trigger_rule=TriggerRule.ALL_DONE
    )


    # 3 этап: удаление Dataproc кластера
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_id="{{ ti.xcom_pull(task_ids='get_cluster_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
        # dag=dag,
    )


    # # Удаление кластера с динамическим cluster_id
    # delete_spark_cluster = DataprocDeleteClusterOperator(
    #     task_id='delete_cluster',
    #     # zone=YC_ZONE,
    #     # project_id='your-project-id',
    #     # region='your-region',
    #     cluster_id="c9qdbuquqccs2bvfd3dj"
    # )
    # # Формирование DAG из указанных выше этапов

    # python_step >> bash_step #  >> create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
    # show_zone_task = show_zone()
    # create_spark_cluster >> delete_spark_cluster >> remove_cluster_bash

    # create_spark_cluster >>
    create_spark_cluster >> get_cluster_id_task >> spark_processing >> delete_spark_cluster >> delete_cluster_using_bash
