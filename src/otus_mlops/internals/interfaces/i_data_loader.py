from abc import ABC, abstractmethod
from typing import Final, Iterator
from enum import StrEnum, auto

import pandas as pd


CSV_EXTENSION: Final[str] = ".csv"


class LoadingMethod(StrEnum):
    FullDataset = auto()
    OneByOne = auto()


class IDataLoader(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def load(self) -> pd.DataFrame | Iterator[pd.DataFrame]:
        raise NotImplementedError