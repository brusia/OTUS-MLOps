

from abc import ABC
from pathlib import Path
from typing import Final, override
from evidently.ui.workspace import RemoteWorkspace  # type: ignore[import-not-found]
from evidently import DataDefinition, Dataset, Report  # type: ignore[import-not-found]
from evidently.presets import DataSummaryPreset  # type: ignore[import-not-found]

from internals.interfaces.i_data_analyser import REPORT_PATH, IDataAnalyser

from evidently.metrics import (
    DatasetSummaryMetric,
    DataQualityPreset,
    ColumnDriftMetric,
    ColumnDistributionMetric,
    ColumnQuantileMetric,
    ColumnValueRangeMetric
)

# import pandas as pd

REPORT_NAME: Final[str] = "evidently_report.html"

class EvidentlyDataAnalyser(IDataAnalyser):
    def __init__(self, report_dir: str = ""):
        self._output_path = Path(report_dir) if report_dir else REPORT_PATH

    # @override
    def analyse(self, dataset: Dataset) -> None:
        # schema = DataDefinition(
        # numerical_columns=["education-num", "age", "capital-gain", "hours-per-week", "capital-loss"],
        # categorical_columns=["education", "occupation", "native-country", "workclass", "marital-status"],
        # )

        # data_prod = Dataset.from_pandas(pd.DataFrame(adult_prod), data_definition=schema)
        # data_ref = Dataset.from_pandas(pd.DataFrame(adult_ref), data_definition=schema)

        report = Report(metrics=[DataSummaryPreset() ], include_tests="True")

        snapshot = report.run(dataset)

        snapshot.save_html(REPORT_PATH.absolute.joinpath(REPORT_NAME).as_posix())
        # self._workspace.add_run(project_name, snapshot)
