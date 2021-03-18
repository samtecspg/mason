from typing import Tuple, List, Optional, Union

from fsspec import open
# from fsspec.core import OpenFile
from fsspec.spec import AbstractBufferedFile

from mason.clients.engines.storage import StorageClient
from mason.clients.local.local_client import LocalClient
from mason.clients.response import Response
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.table import Table
from mason.engines.metastore.models.table.tables import infer
from mason.engines.storage.models.path import Path, construct

class LocalStorageClient(StorageClient):

    def __init__(self, client: LocalClient):
        self.client: LocalClient = client

    def path(self, path: str) -> Path:
        return construct([path], "file")

    def table_path(self, database_name: str, table_name: str) -> Path:
        return construct(["/", database_name, table_name], "file")

    def infer_table(self, path: Path, table_name: Optional[str], options: dict = {}, response=Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        return infer(path, self, table_name, options, response)
        
    def save_to(self, inpath: str, outpath: str, response: Response) -> Response:
        raise NotImplementedError("not implemented")

    def expand_path(self, path: Path, response: Response = Response(), sample_size: int = 3) -> Tuple[List[Path], Response]:
        return [path], response
    
    def open(self, path: Path) -> AbstractBufferedFile:
        return open(path.full_path()).open()
        


