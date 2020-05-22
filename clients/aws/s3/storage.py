from typing import Tuple, List, Optional

from clients.engines.storage import StorageClient
from clients.aws.s3 import S3Client
from engines.metastore.models.table import Table, InvalidTable
from engines.storage.models.path import Path


class S3StorageClient(StorageClient):

    def __init__(self, config: dict):
        self.region = config.get("aws_region")
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")
        self.client = S3Client(self.get_config())

    def path(self, path: str):
        return self.client.path(path)

    def get_path(self, path: Optional[str]) -> Optional[Path]:
        if path:
            return self.client.get_path(path)
        else:
            return None

    def infer_table(self, path: str, options = {}) -> Tuple[Optional[Table], List[InvalidTable]]:
        return self.client.infer_table(path, options)

    def get_config(self):
        return {
            'region': self.region
        }



