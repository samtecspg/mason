from typing import List, Optional, Union

from clients import Client
from abc import abstractmethod

from engines.metastore.models.table import Table, InvalidTable
from engines.storage.models.path import Path


class StorageClient(Client):

    @abstractmethod
    def path(self, path: str) -> str:
        raise NotImplementedError("Client method not implemented")

    @abstractmethod
    def get_path(self, path: str) -> Path:
        raise NotImplementedError("Client method not implemented")
        return Path()

    @abstractmethod
    def infer_table(self, path: str, name: Optional[str], options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        raise NotImplementedError("test")
        return InvalidTable("infer_table not implemented")


