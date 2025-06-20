from abc import ABC
from pathlib import Path
from typing import Final, Union  # , override
import pandas as pd

# from evidently import Calculator
from evidently.ui.workspace import RemoteWorkspace  # type: ignore[import-untyped, import-not-found]
from evidently import DataDefinition, Dataset, Report  # type: ignore[import-untyped, import-not-found]
from evidently.presets import DataSummaryPreset  # type: ignore[import-untyped, import-not-found]

from otus_mlops.internals.interfaces import REPORT_PATH, IDataAnalyser

from evidently.metrics import (
    DriftedColumnsCount,
)


REPORT_EXT: Final[str] = ".html"


class EvidentlyDataAnalyser(IDataAnalyser[Dataset, None]):
    def __init__(self, report_dir: str = ""):
        self._output_path = Path(report_dir) if report_dir else REPORT_PATH

    def analyse(self, dataset: Dataset, ref: Union[Dataset, None] = None) -> None:
        report = Report(
            metrics=[DataSummaryPreset(), DriftedColumnsCount()],
            include_tests="True",
        )

        snapshot = report.run(dataset, ref)

        report_name = "split_parts_report" if ref else "whole_dataset"
        report_path = REPORT_PATH.absolute().joinpath(Path(report_name).with_suffix(REPORT_EXT))
        report_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot.save_html(report_path.as_posix())
