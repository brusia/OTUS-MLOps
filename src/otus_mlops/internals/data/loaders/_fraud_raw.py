from __future__ import annotations
from enum import StrEnum, auto
from pathlib import Path
from typing import Final, Iterator, override
from pyarrow import fs
import pyarrow.csv as pv

import pandas as pd
from otus_mlops.internals.interfaces.i_data_loader import CSV_EXTENSION, IDataLoader, LoadingMethod

DEFAULT_DATA_DIR: Final[Path] = Path("/home/ubuntu/data")


class FraudRawDataLoader(IDataLoader):
    def __init__(self):
        pass
        # super.__init__(self)

    @override
    def load(self, data_dir: str | Path = "", loading_method: LoadingMethod = LoadingMethod.OneByOne) -> pd.DataFrame:
        # data_path: Path
        # if data_dir and isinstance(str, data_dir):
        # data_path: Path = Path(data_dir) if isinstance(data_dir, str) and data_dir != "" else data_dir if isinstance(data_dir, Path) and data_dir.exists() else DEFAULT_DATA_DIR

        # hdfs = fs.HadoopFileSystem(host="MASTERNODE", port=9000, user="ubuntu")

        # Путь к CSV-файлу
        hdfs_path = "/user/ubuntu/data/2022-05-08.txt"

        # Чтение CSV
        table = pv.read_csv(hdfs.open_input_file(hdfs_path))
        df = table.to_pandas()

        return df
        # if not data_path.exists():
        # #     data_path = DEFAULT_DATA_DIR

        # match loading_method:
        #     case LoadingMethod.FullDataset:
        #         return self._load_full(data_path)
        #     case LoadingMethod.OneByOne:
        #         return self._load_one_by_one(data_path)
            # case _:
            #     return pd.DataFrame()


    def _load_full(self, data_path: Path) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for data_frame in self._load_one_by_one(data_path):
            frames.append(data_frame)

        return pd.concat(frames)

    def _load_one_by_one(self, data_path: Path) -> Iterator[pd.DataFrame]:
         for file in data_path.iterdir():
            if file.suffix == CSV_EXTENSION:
                yield pd.read_csv(file)