
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Final, Generic

from otus_mlops.internals.interfaces._base import InputData, DataFrame


REPORT_PATH: Final[Path] = Path("reports")

class IDataAnalyser(ABC, Generic[InputData, DataFrame]):
    @abstractmethod
    def analyse(self, data_frame: DataFrame) -> Any:
        raise NotImplementedError
