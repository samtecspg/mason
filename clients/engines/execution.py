
from clients import Client
from clients.response import Response
from abc import abstractmethod

class StorageClient(Client):

    @abstractmethod
    def any(self, response: Response):
        raise NotImplementedError("Client method not implemented")

