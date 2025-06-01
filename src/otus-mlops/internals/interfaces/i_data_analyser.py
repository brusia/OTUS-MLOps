
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Final


REPORT_PATH: Final[Path] = Path("reports")

class IDataAnalyser(ABC):
    @abstractmethod
    def analyse(self) -> None:
        raise NotImplementedError
