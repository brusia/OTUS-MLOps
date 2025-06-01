from pathlib import Path
from internals.interfaces.i_data_analyser import REPORT_PATH, IDataAnalyser

class TFDataAnalyser(IDataAnalyser):
    def __init__(self, report_dir: str = ""):
        self._output_path = Path(report_dir) if report_dir else REPORT_PATH
