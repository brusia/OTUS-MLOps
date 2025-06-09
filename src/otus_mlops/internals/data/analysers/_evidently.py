

from abc import ABC
from pathlib import Path
from typing import Final, override
import pandas as pd
# from evidently import Calculator
from evidently.ui.workspace import RemoteWorkspace  # type: ignore[import-untyped, import-not-found]
from evidently import DataDefinition, Dataset, Report  # type: ignore[import-untyped, import-not-found]
from evidently.presets import DataSummaryPreset  # type: ignore[import-untyped, import-not-found]

from otus_mlops.internals.interfaces.i_data_analyser import REPORT_PATH, IDataAnalyser

from evidently.metrics import (
    CategoryCount,
    DriftedColumnsCount,
    InListValueCount,
    InRangeValueCount,
    MaxValue,
    MeanValue,
    MedianValue,
    MinValue,
    MissingValueCount,
    OutListValueCount,
    OutRangeValueCount,
    QuantileValue,
    StdValue,
    UniqueValueCount,
    ValueDrift,
    AlmostConstantColumnsCount,
    AlmostDuplicatedColumnsCount,
    ColumnCount,
    ConstantColumnsCount,
    DatasetMissingValueCount,
    DuplicatedColumnsCount,
    DuplicatedRowCount,
    EmptyColumnsCount,
    EmptyRowsCount,
    RowCount
)


REPORT_NAME: Final[str] = "evidently_report.html"

class EvidentlyDataAnalyser(IDataAnalyser[Dataset, pd.DataFrame]):
    def __init__(self, report_dir: str = ""):
        self._output_path = Path(report_dir) if report_dir else REPORT_PATH

    # @override
    def analyse(self, dataset: Dataset) -> None:
        report = Report(metrics=[DataSummaryPreset(), DriftedColumnsCount(),
            CategoryCount(), DriftedColumnsCount(), InListValueCount(), InRangeValueCount(),
            MaxValue(), MeanValue(), MedianValue(), MinValue(), MissingValueCount(), OutListValueCount(),
            OutRangeValueCount(), QuantileValue(), StdValue(), UniqueValueCount(), ValueDrift(),
            AlmostConstantColumnsCount(), AlmostDuplicatedColumnsCount(), ColumnCount(),
            ConstantColumnsCount(), DatasetMissingValueCount(), DuplicatedColumnsCount(),
            DuplicatedRowCount(), EmptyColumnsCount(), EmptyRowsCount(), RowCount()], include_tests="True")

        snapshot = report.run(dataset)

        snapshot.save_html(REPORT_PATH.absolute.joinpath(REPORT_NAME).as_posix())