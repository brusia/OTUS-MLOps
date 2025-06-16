

from abc import ABC, abstractmethod
from typing import Generic

from otus_mlops.internals.interfaces._base import InputData


# class IDataPreprocessor(ABC, Generic[InputData, OutputInputData]):
class IDataPreprocessor(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def preprocess(self, input_data: InputData) -> InputData:
        raise NotImplementedError