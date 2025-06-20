from abc import ABC
from pathlib import Path
from typing import Final, Union  # , override
import pandas as pd
from otus_mlops.internals.interfaces import REPORT_PATH, IDataAnalyser
import ruptures
from ruptures import Pelt
from strenum import StrEnum
from matplotlib import pyplot

IMAGE_EXT: Final[str] = ".png"
PENALTY_VALUE: Final[int] = 3


class SegmentModelType(StrEnum):
    LeastAbsolute = "l1"
    LeastSquare = "l2"
    RadialBasisFunction = "rbf"


class RupturesDataAnalyser(IDataAnalyser[pd.DataFrame, None]):
    def __init__(self, report_dir: str = ""):
        self._output_path = Path(report_dir) if report_dir else REPORT_PATH

    def analyse(
        self,
        data_frame: pd.DataFrame,
        feature_name: str,
        model: SegmentModelType = SegmentModelType.RadialBasisFunction,
    ) -> None:
        algo = Pelt(model=model.value).fit(data_frame)
        result = algo.predict(pen=PENALTY_VALUE)

        ruptures.display(data_frame, result, result, computed_chg_pts_linewidth=1)
        pyplot.savefig(REPORT_PATH.joinpath("ruptures", Path(feature_name).joinpath(IMAGE_EXT)))
