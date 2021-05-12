from typing import Tuple, List, Optional, Union

from fsspec import open
from fsspec.spec import AbstractBufferedFile

from mason.clients.engines.storage import StorageClient
from mason.clients.local.local_client import LocalClient
from mason.clients.response import Response
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path, parse_path, InvalidPath


class LocalStorageClient(StorageClient):

    def __init__(self, client: LocalClient):
        self.client: LocalClient = client

    def get_path(self, path: str) -> Union[Path, InvalidPath]:
        return parse_path(path, "file") 

    def save_to(self, inpath: Path, outpath: Path, response: Response) -> Response:
        raise NotImplementedError("not implemented")

    def expand_path(self, path: Path, response: Response = Response(), sample_size: int = 3) -> Tuple[List[Path], Response]:
        return [path], response
    
    def open(self, path: Path) -> AbstractBufferedFile:
        return open(path.full_path()).open()

    def infer_table(self, path: Path, table_name: Optional[str], options: dict={}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        raise NotImplementedError("Client infer_table not implemented")



