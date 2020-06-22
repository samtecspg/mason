from typing import List, Optional, Union, Tuple

from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.clients.s3.s3_client import S3Client
from mason.engines.metastore.models.table import Table, InvalidTables
from mason.engines.storage.models.path import Path


class S3StorageClient(StorageClient):

    def __init__(self, config: dict):
        self.client = S3Client(config)

    def path(self, path: str) -> Path:
        return self.client.path(path)

    def infer_table(self, path: str,  name: Optional[str] = None, options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        return self.client.infer_table((self.path(path) or Path("")).path_str, name, options, response)




