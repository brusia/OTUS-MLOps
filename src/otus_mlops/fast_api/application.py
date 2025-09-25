"""
Application
"""

from enum import Enum, auto
import os
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
import numpy as np
from pydantic import BaseModel
import pandas as pd
import logging
import boto3
import onnxruntime

_logger = logging.getLogger(__name__)

S3_ENDPOINT_URL = "https://storage.yandexcloud.net"
BUCKET_NAME = "brusia-bucket"
MODEL_PATH_REMOTE = Path("models").joinpath("converted")
MODEL_PATH_LOCAL = Path("/tmp/models")
BINARY_MODEL_NAME = "fraud_detection_binary.onnx"
MULTICLASS_MODEL_NAME = "fraud_detection_multi.onnx"

s3_client = boto3.session.Session(aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", None),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", None)).client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)


class PredictionType(Enum):
    Binary = auto()
    Multiclass = auto()

def get_model(prediction_type: PredictionType):
    """
    Function to load a model from disk.

    Parameters
    ----------
    model_path : str
        The path to the model to load.

    Returns
    -------
    BaseEstimator
        The loaded model.
    """
    try:
        model_name = BINARY_MODEL_NAME if prediction_type == PredictionType.Binary else MULTICLASS_MODEL_NAME
        s3_client.download_file(BUCKET_NAME, MODEL_PATH_REMOTE.joinpath(model_name).as_posix(), MODEL_PATH_LOCAL.joinpath(model_name).as_posix())

        model = onnxruntime.InferenceSession(MODEL_PATH_LOCAL.joinpath(model_name))

        return model
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Model not found at {MODEL_PATH_LOCAL}") from e

_logger.info("Loading model")
MODEL_PATH_LOCAL.mkdir(parents=True, exist_ok=True)
binary_model = get_model(PredictionType.Binary)
multiclass_model = get_model(PredictionType.Multiclass)
_logger.info("Model loaded")


app = FastAPI()

class FraudFeatures(BaseModel):
    """Fraud features"""
    transaction_id: int
    customer_id: int
    terminal_id: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int


def make_prediction(features: FraudFeatures, prediction_type: PredictionType = PredictionType.Binary, output_value_dict: dict[int, str] = {0: "simple_transaction", 1: "FRAUD"}) -> Dict[str, Any]:
    _logger.info(f"Making prediction for: {features}")
    try:
        data = pd.DataFrame([features.model_dump()])
        model = binary_model if prediction_type == PredictionType.Binary else multiclass_model

        input_name = model.get_inputs()[0].name
        label_name = model.get_outputs()[0].name

        print(f"Input name: {input_name}")
        print(f"Labels name: {label_name}")

        res = model.run(None, {input_name: np.array([[features.tx_amount]], dtype=np.double)})

        probabilities_dict = res[1][0]

        predicted_class = max(probabilities_dict, key=probabilities_dict.get)
        max_probability = probabilities_dict[predicted_class]

        pred_class = output_value_dict[predicted_class]
    except Exception as e:
        _logger.error(f"Prediction error: {e}")
        raise HTTPException(
            status_code=500, 
            detail="An error occurred during prediction"
        )
    
    return {"prediction": pred_class, "class_probability": max_probability}

@app.get("/")
def health_check() -> dict:
    """Health check"""
    return {"status": "ok"}


@app.post("/predict")
def predict_fraud(features: FraudFeatures) -> Dict[str, Any]:
    """Make a prediction by model"""

    output_value_dict = { 0: "simple_transaction", 1: "FRAUD" }
    return make_prediction(features=features, prediction_type=PredictionType.Binary, output_value_dict=output_value_dict)


@app.post("/predict_scenario")
def prediction_fraud_scenario(features: FraudFeatures) -> Dict[str, Any]:
    """Make a prediction for multiclasses"""

    output_value_dict ={0: "simple_transaction", 1: "scenario_1", 2: "scenario_2", 3: "scenario_3"}
    return make_prediction(features=features, prediction_type=PredictionType.Binary, output_value_dict=output_value_dict)