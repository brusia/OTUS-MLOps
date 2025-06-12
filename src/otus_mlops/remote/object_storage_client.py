

import os
from pathlib import Path
from typing import Final
from urllib.parse import urlparse
import boto3
import logging
from src.otus_mlops.internals.interfaces import IRemoteClient

S3_ENDPOINT_URL: Final[str] = "https://storage.yandexcloud.net"
_logger = logging.getLogger(__name__)

class ObjectStorageClient(IRemoteClient):
    def __init__(self):
        self._resource = boto3.resource(service_name="s3", endpoint_url=self.S3_ENDPOINT_URL)

    def download_data(self, ) -> bool:
        pass

    @classmethod
    def get_bucket_and_prefix(cls, remote_uri: str) -> tuple[str, str]:
        """
        Parse input url, formated 's3://<bucket-name>/<prefix>'

        :param remote_uri: input uri to parse
        :type remote_uri: str

        :returns: parsed values for bucket_name and prefix (key for file/dir on s3)
        :rtype: tuple[str, str]
        """
        parsed_url = urlparse(remote_uri)
        relative_remote_path = Path(parsed_url.path.lstrip("/"))
        bucket_name = relative_remote_path.parts[0]
        prefix = relative_remote_path.relative_to(bucket_name).as_posix()

        return bucket_name, prefix

    def _download_file(self, remote_path: str, local_dir: Path) -> bool:
        bucket_name, prefix = ObjectStorageClient.get_bucket_and_prefix(remote_uri=remote_path)

        s3_model_object = self._resource.Object(bucket_name=bucket_name, key=prefix)

        local_path = Path(local_dir).joinpath(Path(prefix).name)
        local_path.parent.mkdir(exist_ok=True, parents=True)
        try:
            s3_model_object.download_file(Filename=local_path.as_posix())
        except Exception:
            _logger.exception("Handle exception while downloading '%s': ", local_path.as_posix())
            return False

        return True

    def _download_dir(self, remote_url: str, local_dir: Path) -> bool:
        bucket_name, prefix = ObjectStorageClient.get_bucket_and_prefix(remote_uri=remote_url)
        s3_bucket = self._resource.Bucket(bucket_name)

        output_dir = Path(local_dir)
        for s3_object in s3_bucket.objects.filter(Prefix=prefix).all():
            rel_path = Path(s3_object.key).relative_to(prefix)
            output_dir.joinpath(rel_path).parent.mkdir(parents=True, exist_ok=True)

            if not s3_object.key.endswith("/"):
                try:
                    s3_bucket.download_file(s3_object.key, str(output_dir.joinpath(rel_path)))
                except Exception:
                    _logger.exception("Handle exception while downloading '%s': ", s3_object.key)
                    return False
        return True

    def _upload_file(self, local_file: Path, remote_uri: str) -> bool:
        bucket_name, prefix = ObjectStorageClient.get_bucket_and_prefix(remote_uri=remote_uri)
        file_name = Path(local_file).name
        key = "/".join([prefix, file_name])
        try:
            self._resource.Bucket(bucket_name).upload_file(str(local_file), key)
        except Exception:
            _logger.exception("Handle exception while uploading file '%s': ", local_file.as_posix())
            return False

        return True

    def _upload_dir(self, local_dir: Path, remote_uri: str) -> bool:
        bucket_name, prefix = ObjectStorageClient.get_bucket_and_prefix(remote_uri=remote_uri)
        for root, _, files in os.walk(local_dir):
            for file in files:
                local_file = Path("/".join([root, file]))
                remote_file = local_file.relative_to(local_dir)
                key = "/".join([prefix, remote_file.as_posix()])
                try:
                    self._resource.Bucket(bucket_name).upload_file(str(local_file), key)
                except Exception:
                    _logger.exception("Handle exception while uploading folder'%s': ", local_file.as_posix())
                    return False
        return True