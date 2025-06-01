from __future__ import annotations
from enum import StrEnum, auto
from pathlib import Path
from typing import Final, Iterator, override

import pandas as pd
from interfaces.i_data_loader import CSV_EXTENSION, IDataLoader

DEFAULT_DATA_DIR: Final[Path] = Path("/home/ubuntu/data")


class LoadingMethod(StrEnum):
    FullDataset = auto()
    OneByOne = auto()


class FraudRawDataLoader(IDataLoader):
    def __init__(self):
        super.__init__(self)

    @override
    def load(self, data_dir: str | Path = "", loading_method: LoadingMethod = LoadingMethod.OneByOne) -> pd.DataFrame | Iterator[pd.DataFrame]:
        # data_path: Path
        # if data_dir and isinstance(str, data_dir):
        data_path: Path = Path(data_dir) if isinstance(str, data_dir) and data_dir else data_dir

        if not data_path.exists():
            data_path = DEFAULT_DATA_DIR

        match loading_method:
            case LoadingMethod.FullDataset:
                return self._load_full(data_path)
            case LoadingMethod.OneByOne:
                return self._load_one_by_one(data_path)


    def _load_full(self, data_path: Path) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for data_frame in self._load_one_by_one(data_path):
            frames.append(data_frame)

        return pd.concat(frames)

    def _load_one_by_one(self, data_path: Path) -> Iterator[pd.DataFrame]:
         for file in data_path.iterdir():
            if file.suffix == CSV_EXTENSION:
                yield pd.read_csv(file)