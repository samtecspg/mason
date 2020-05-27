from typing import Tuple, List, Optional, Union

from clients.engines.storage import StorageClient
from clients.s3 import S3Client
from engines.metastore.models.table import Table, InvalidTable
from engines.storage.models.path import Path


class S3StorageClient(StorageClient):

    def __init__(self, config: dict):
        self.client = S3Client(config)

    def path(self, path: str) -> Path:
        return self.client.path(path)

    def get_path(self, path: Optional[str]) -> Optional[Path]:
        if path:
            return self.client.get_path(path)
        else:
            return None

    def infer_table(self, path: str,  name: Optional[str] = None, options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        return self.client.infer_table(self.path(path).path_str, name, options)




