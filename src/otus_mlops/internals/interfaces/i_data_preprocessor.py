

from abc import ABC, abstractmethod
from typing import Generic

from otus_mlops.internals.interfaces.base import AnalyserInputData, DataFrame


class IDataPreprocessor(ABC, Generic[AnalyserInputData, DataFrame]):
# class IDataPreprocessor(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def preprocess(self, input_data: AnalyserInputData) -> DataFrame:
        raise NotImplementedError