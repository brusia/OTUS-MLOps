from abc import ABC, abstractmethod
from pathlib import Path
from typing import Final, Generic, Iterator, TypeVar, Union
from enum import auto
from strenum import StrEnum


class IRemoteClient(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def download_data(self, source_path: Union[str, Path], desctination_path: Union[str, Path]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def upload_data(self, source_path: Union[str, Path], desctination_path: Union[str, Path]) -> bool:
        raise NotImplementedError