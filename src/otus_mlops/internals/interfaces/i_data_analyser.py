from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Final, Generic

from otus_mlops.internals.interfaces.base import AnalyserInputData, AnalyserOutputData


REPORT_PATH: Final[Path] = Path("reports")


class IDataAnalyser(ABC, Generic[AnalyserInputData, AnalyserOutputData]):
    @abstractmethod
    def analyse(self, data_frame: AnalyserInputData) -> AnalyserOutputData:
        raise NotImplementedError
