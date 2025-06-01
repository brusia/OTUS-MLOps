from abc import ABC, abstractmethod
from typing import Final, Iterator

import pandas as pd


CSV_EXTENSION: Final[str] = ".csv"

class IDataLoader(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def load(self) -> pd.DataFrame | Iterator[pd.DataFrame]:
        raise NotImplementedError