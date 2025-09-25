"""
Main pipeline for the project
"""

import os
from pathlib import Path

import boto3
import numpy as np
import pandas as pd

from skl2onnx import to_onnx

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

RANDOM_STATE = 42
TEST_SIZE = 0.2

S3_ENDPOINT_URL = "https://storage.yandexcloud.net"
BUCKET_NAME = "brusia-bucket"
MODEL_PATH_REMOTE = Path("models").joinpath("converted")
MODEL_PATH_LOCAL = Path("/tmp").joinpath("models")
DATA_PATH = Path("data/processed_with_airlow/2019-08-22/part-00000-822589fe-c7bb-4214-9fed-8a66e136b730-c000.snappy.parquet")

BINARY_MODEL_NAME = "fraud_detection_binary.onnx"
MULTICLASS_MODEL_NAME = "fraud_detection_multi.onnx"


def main() -> None:
    """
    Main pipeline for the project
    """

    def _fit_and_upload_model(X_train: pd.DataFrame, y_train: pd.DataFrame, n_estimators: int = 3, model_name: str = "model.onnx"):
        model = RandomForestClassifier(n_estimators = n_estimators)
        model.fit(X_train, y_train)

        models_dir = Path("/tmp/models")
        models_dir.mkdir(parents=True, exist_ok=True)

        onnx_model = to_onnx(model, X_train)

        with open(models_dir.joinpath(model_name), "wb") as f:
            f.write(onnx_model.SerializeToString())

        s3_client.upload_file(
                        Filename=models_dir.joinpath(model_name).as_posix(),
                        Bucket=BUCKET_NAME,
                        Key=MODEL_PATH_REMOTE.joinpath(model_name).as_posix()
                    )

    local_data_path = Path("/tmp").joinpath(DATA_PATH.name)
    s3_client = boto3.session.Session(aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", None),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", None)).client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)
    
    s3_client.download_file(BUCKET_NAME, DATA_PATH.as_posix(), local_data_path)
    data_frame = pd.read_parquet(local_data_path)

    data_frame["tx_datetime"] = data_frame["tx_datetime"].astype("int64")

    train, _ = train_test_split(
        data_frame, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    _fit_and_upload_model(train[["tx_amount"]], train["tx_fraud"], 10, BINARY_MODEL_NAME)
    _fit_and_upload_model(train[["tx_amount"]], train["tx_fraud_scenario"], 10, MULTICLASS_MODEL_NAME)


if __name__ == "__main__":
    main()