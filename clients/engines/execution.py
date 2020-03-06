
from clients import Client
from abc import abstractmethod
from clients.response import Response
from abc import abstractmethod

class StorageClient(Client):

    @abstractmethod
    def any(self, respnse: Response):
        raise NotImplementedError("Client method not implemented")

class EmptyExecutionClient(Client):
    def any(self, respnse: Response):
        raise NotImplementedError("Client method not implemented")

