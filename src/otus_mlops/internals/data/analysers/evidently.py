from abc import ABC
from pathlib import Path
from typing import Final, Union  # , override
import pandas as pd

from evidently import Dataset, Report  # type: ignore[import-untyped, import-not-found]
from evidently.presets import DataSummaryPreset  # type: ignore[import-untyped, import-not-found]

from otus_mlops.internals.interfaces import REPORTS_PATH, IDataAnalyser

from evidently.metrics import (
    DriftedColumnsCount,
)


REPORT_EXT: Final[str] = ".html"
SPLIT_PARTS_REPORT_NAME: Final[str] = "split_parts_report"
WHOLE_DATA_REPORT_NAME: Final[str] = "whole_dataset"

class EvidentlyDataAnalyser(IDataAnalyser[Dataset, None]):
    def __init__(self, report_dir: str = ""):
        self._output_path = Path(report_dir) if report_dir else REPORTS_PATH

    def analyse(self, dataset: Dataset, ref: Union[Dataset, None] = None, data_name: str = "") -> None:
        metrics = [DataSummaryPreset()]
        if ref:
            metrics.append(DriftedColumnsCount())

        report = Report(
            metrics=metrics,
            include_tests="True",
        )

        snapshot = report.run(dataset, ref)

        report_name = SPLIT_PARTS_REPORT_NAME if ref else WHOLE_DATA_REPORT_NAME
        report_path = self._output_path.joinpath(data_name, "evidently", Path(report_name).with_suffix(REPORT_EXT))
        report_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot.save_html(report_path.as_posix())
