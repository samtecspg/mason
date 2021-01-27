from typing import Optional, Union, Tuple
from abc import abstractmethod

from mason.clients.client import Client
from mason.clients.response import Response
from mason.engines.metastore.models.table import Table, InvalidTables
from mason.engines.storage.models.path import Path

class StorageClient(Client):

    @abstractmethod
    def path(self, path: str) -> Path:
        raise NotImplementedError("Client path not implemented")

    @abstractmethod
    def save_to(self, inpath: str, outpath: str, response: Response) -> Response:
        raise NotImplementedError("Client save_to not implemented")

    @abstractmethod
    def infer_table(self, path: str, name: Optional[str], options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table,InvalidTables], Response]:
        raise NotImplementedError("Client infer_table not implemented")

