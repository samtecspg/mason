from typing import List, Tuple, Optional

from clients import Client
from abc import abstractmethod

from engines.metastore.models.table import Table, InvalidTable
from engines.storage.models.path import Path


class StorageClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific metastore client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'S3StorageClient' with abstract attribute 'path' (for example)

    @abstractmethod
    def path(self, path: str) -> str:
        raise NotImplementedError("Client method not implemented")

    @abstractmethod
    def get_path(self, path: str) -> Path:
        raise NotImplementedError("Client method not implemented")
        return Path()

    @abstractmethod
    def infer_table(self, path: str, options: dict = {}) -> Tuple[Optional[Table], List[InvalidTable]]:
        return InvalidTable("Not implemented")


class EmptyStorageClient(StorageClient):

    def path(self, path: str) -> str:
        raise NotImplementedError("Client method not implemented")
        return ""

    def get_path(self, path: str) -> Path:
        raise NotImplementedError("Client method not implemented")
        return Path()

    def infer_table(self, path: str, options: dict = {}) -> Tuple[List[Table], List[InvalidTable]]:
        return InvalidTable("Not implemented")

