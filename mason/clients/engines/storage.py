from typing import Tuple, List, Optional, Union
from abc import abstractmethod

from fsspec.spec import AbstractBufferedFile

from mason.clients.base import Client
from mason.clients.response import Response
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path

class StorageClient:
    
    @abstractmethod
    def __init__(self, client: Client):
        self.client = client
        
    @abstractmethod
    def open(self, path: Path) -> AbstractBufferedFile:
        raise NotImplementedError("Client open not implemented")

    @abstractmethod
    def path(self, path: str) -> Path:
        raise NotImplementedError("Client path not implemented")
    
    @abstractmethod
    def table_path(self, database_name: str, table_name: str) -> Path:
        raise NotImplementedError("Client table_path not implemented")

    @abstractmethod
    def save_to(self, inpath: str, outpath: str, response: Response) -> Response:
        raise NotImplementedError("Client save_to not implemented")

    @abstractmethod
    def infer_table(self, path: Path, table_name: Optional[str], options: dict={}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        raise NotImplementedError("Client infer_table not implemented")

    def get_name(self, path: Path, name: Optional[str] = None) -> str:
        full_path = path.full_path()
        if not name or name == "":
            return full_path.rstrip("/").split("/")[-1]
        else:
            return name

    @abstractmethod
    def expand_path(self, path: Path, response: Response = Response(), sample_size: int = 3) -> Tuple[List[Path], Response]:
        raise NotImplementedError("Client expand_path not implemented")

        
