from pathlib import Path
from typing import Dict, Final, List, Tuple, Union
from pyspark.sql import DataFrame as SparkDataFrame
from otus_mlops.internals.interfaces import IDataPreprocessor
from pyspark.ml.feature import MinMaxScaler, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

from otus_mlops.internals.interfaces.base import NUMERICAL_COLUMNS



class FraudDataProcessor(IDataPreprocessor[SparkDataFrame, SparkDataFrame]):
    def __init__(self, model_path: Union[Path, None] = None):
        self._model_path = model_path if model_path else PREPROCESS_DATA_MODEL_PATH
        self._model = Pipeline.read().load(model_path) if model_path and model_path.exists() else None

    def fit_model(self, input_data: SparkDataFrame, numeric_columns: List[str]) -> None:
        """
        Method is used for fit Pipeline imputer / Rescaler model

        :param input_data: reference data
        :type input_data: SparkDataFrame
        """

        assembler = VectorAssembler(
            inputCols=NUMERICAL_COLUMNS,
            outputCol="features"
        )

        # todo: customize scaler choosing: MinMax, Standard, etc.
        scaler = MinMaxScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )

        self._pipeline = Pipeline(stages=[assembler, scaler])

        
        model = self._pipeline.fit(input_data)
        model.write().overwrite().save(self._model_path)
        
    def preprocess(self, input_data: SparkDataFrame) -> SparkDataFrame:
        if not self._model:
            raise RuntimeError("Pipelime model was not loaded. Please, check parameters and trye again.")

        return self._model.transform(input_data)