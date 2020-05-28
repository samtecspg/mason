from typing import List, Optional, Union
from abc import abstractmethod

from mason.clients.client import Client
from mason.engines.metastore.models.table import Table, InvalidTable
from mason.engines.storage.models.path import Path


class StorageClient(Client):

    @abstractmethod
    def path(self, path: Optional[str]) -> Optional[Path]:
        raise NotImplementedError("Client method not implemented")
        return Path()

    @abstractmethod
    def infer_table(self, path: str, name: Optional[str], options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        raise NotImplementedError("test")
        return InvalidTable("infer_table not implemented")


