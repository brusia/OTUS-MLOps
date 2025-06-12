

from typing import Final
from pyspark.sql import DataFrame as SparkDataFrame
import pandas as pd
# TODO: lazy import
from pyspark.rdd import RDD
from otus_mlops.internals.interfaces.i_data_preprocessor import IDataPreprocessor
import logging

SAMPLES_COUNT: Final[int] = 100000

_logger = logging.getLogger(__name__)

class EvidentlyProcessor(IDataPreprocessor[SparkDataFrame, pd.DataFrame]):

    def preprocess(self, input_data: SparkDataFrame) -> pd.DataFrame:
        sample_size = min(SAMPLES_COUNT, input_data.count())
        _logger.debug("Samples count is '%s', total was  '%s'", sample_size, input_data.count())
        
        if sample_size < SAMPLES_COUNT:
            _logger.warning("Samples count is too small. At least %s recommended.", SAMPLES_COUNT)
        
        if sample_size < input_data.count():
            if hasattr(input_data, 'sample'):
                sample_df = input_data.sample(False, sample_size/float(input_data.count()), 42)
            else:
                _logger.error("Method sample is unavailable for this type DataFrame")
                sample_df = input_data.limit(sample_size)
        else:
            sample_df = input_data

        numeric_cols = [f.name for f in sample_df.schema.fields
                        if (isinstance(f.dataType, (NumericType, DoubleType) or
                        (isinstance(f.dataType, StringType) and f.metadata.get("numeric"))))]
        categorical_cols = [f.name for f in sample_df.schema.fields 
                            if (isinstance(f.dataType, StringType) and not any(x in f.metadata.get("exclude_for_model", []) for x in ['numeric', 'id']))]
        
        all_cols = list(set(numeric_cols + categorical_cols + ([target_col] if target_col else [])))
        all_cols = [col for col in all_cols if col is not None and col in sample_df.columns]

        preprocessed_data = sample_df.select(all_cols).toPandas()
        
        for col in all_cols:
            if col not in preprocessed_data.columns:
                preprocessed_data[col] = None
            
            if col in numeric_cols and preprocessed_data[col].dtype == 'object':
                preprocessed_data[col] = preprocessed_data[col].astype(float)
            elif col in categorical_cols and preprocessed_data[col].dtype == 'float':
                preprocessed_data[col] = preprocessed_data[col].astype(str)
        
        return preprocessed_data