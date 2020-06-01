from typing import List, Optional, Union

from mason.clients.engines.storage import StorageClient
from mason.clients.s3 import S3Client
from mason.engines.metastore.models.table import Table, InvalidTable
from mason.engines.storage.models.path import Path


class S3StorageClient(StorageClient):

    def __init__(self, config: dict):
        self.client = S3Client(config)

    def path(self, path: Optional[str]) -> Optional[Path]:
        if path:
            return self.client.path(path)
        else:
            return None

    def infer_table(self, path: str,  name: Optional[str] = None, options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        return self.client.infer_table((self.path(path) or Path("")).path_str, name, options)




