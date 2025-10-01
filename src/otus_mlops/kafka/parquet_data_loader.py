
import os
import boto3
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Final, Union, Generator

import pandas as pd


BUCKET_NAME: Final[str] = "brusia-bucket"
INPUT_DATA_DIR: Final[str] = "data/processed_with_airlow/"
S3_ENDPOINT_URL="https://storage.yandexcloud.net"


class ParquetDataLoader:
    def __init__(self):
        self._s3_client = boto3.session.Session(aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", None),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", None)).client(service_name="s3", endpoint_url=S3_ENDPOINT_URL)

        self._input_folders = iter(sorted([Path(obj["Prefix"]).relative_to(INPUT_DATA_DIR) for obj in self._s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=INPUT_DATA_DIR, Delimiter='/').get("CommonPrefixes", {})]))

        self._current_folder = next(self._input_folders, None)

        self._input_files = iter(sorted([Path(obj["Key"]).relative_to(Path(INPUT_DATA_DIR).joinpath(self._current_folder).as_posix()) for obj in self._s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=Path(INPUT_DATA_DIR).joinpath(self._current_folder).as_posix()).get("Contents", {}) if Path(obj["Key"]).suffix == ".parquet"]))

        self._current_data_frame = self._load_file().iterrows()

    def _next_file(self) -> Union[Path, None]:
        next_file = next(self._input_files)
        if next_file is StopIteration:
            self._current_folder = next(self._input_folders)
            if not self._current_folder:
                return None
            
            self._input_files = iter(sorted([Path(obj["Key"]).relative_to(Path(INPUT_DATA_DIR).joinpath(self._current_folder).as_posix()) for obj in self._s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=Path(INPUT_DATA_DIR).joinpath(self._current_folder).as_posix()).get("Contents", {}) if Path(obj["Key"]).suffix == ".parquet"]))
            next_file = next(self._input_files)
        return next_file

    def _load_file(self) -> pd.DataFrame:
        next_file = self._next_file()
        if not next_file:
            raise RuntimeError("No more data to process.")

        with TemporaryDirectory() as temp_dir:
            local_data_path = Path(temp_dir).joinpath(next_file)
            self._s3_client.download_file(BUCKET_NAME, Path(INPUT_DATA_DIR).joinpath(self._current_folder, next_file).as_posix(), local_data_path)
            data_frame = pd.read_parquet(Path(temp_dir).joinpath(next_file).as_posix())
        
        data_frame["tx_datetime"] = data_frame["tx_datetime"].astype("int64")

        return data_frame

    def get_next_transaction(self) -> Generator[Dict[str, Any], Any, None]:
        next_row = next(self._current_data_frame)
        if next_row is StopIteration:
            self._current_data_frame = self._load_file().iterrows()
            next_row = next(self._current_data_frame)

        yield next_row[1].to_dict()



if __name__ == "__main__":
    loader = ParquetDataLoader()
    for item in loader.get_next_transaction():
        print(item)