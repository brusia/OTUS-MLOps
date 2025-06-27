from abc import ABC, abstractmethod
from pathlib import Path
from typing import Final, Generic, Iterator, TypeVar, Union
from enum import auto
from strenum import StrEnum

from otus_mlops.internals.interfaces.base import DataFrame



CSV_EXTENSION: Final[str] = ".csv"
TXT_EXTENSION: Final[str] = ".txt"
PARQUET_EXTENSION: Final[str] = ".parquet"

class LoadingMethod(StrEnum):
    FullDataset = auto()
    OneByOne = auto()


class IDataLoader(ABC, Generic[DataFrame]):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def load(self, data_dir: Union[str, Path], loading_method: LoadingMethod = LoadingMethod.OneByOne) -> Union[DataFrame, Iterator[DataFrame]]:
        raise NotImplementedError