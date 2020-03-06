
from clients import Client
from abc import abstractmethod

class StorageClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific metastore client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'S3StorageClient' with abstract attribute 'path' (for example)

    @abstractmethod
    def path(self, path: str) -> str:
        raise NotImplementedError("Client method not implemented")

class EmptyStorageClient(Client):

    def path(self, path: str):
        raise NotImplementedError("Client method not implemented")

