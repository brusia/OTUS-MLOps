from pathlib import Path
from otus_mlops.internals.interfaces.i_data_analyser import REPORTS_PATH, IDataAnalyser


# TBD.
class TFDataAnalyser(IDataAnalyser):
    def __init__(self, report_dir: str = ""):
        self._output_path = Path(report_dir) if report_dir else REPORTS_PATH
