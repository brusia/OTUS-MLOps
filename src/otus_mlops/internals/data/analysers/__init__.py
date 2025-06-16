from ._evidently import EvidentlyDataAnalyser, REPORT_NAME as EVIDENTLY_REPORT_NAME
from ._spark_custom import SparkCustomDataAnalyser
from ._tf import TFDataAnalyser  # TBA.

__all__ = ["EvidentlyDataAnalyser", "SparkCustomDataAnalyser", "TFDataAnalyser"]