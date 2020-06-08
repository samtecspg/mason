from typing import Union

from mason.clients.engines.storage import InvalidStorageClient, StorageClient
from mason.clients.engines.valid_client import EmptyClient
from mason.engines.engine import Engine

class StorageEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("storage", config)
        self.client: Union[StorageClient, EmptyClient, InvalidStorageClient] = self.get_client()

    def get_client(self):
        from mason.clients.engines.valid_client import ValidClient
        client = self.validate()
        if isinstance(client, ValidClient):
            if client.client_name == "s3":
                from mason.clients.s3.storage import S3StorageClient
                return S3StorageClient(client.config)
            else:
                return InvalidStorageClient(f"Client type not supported {client.client_name}")
        elif isinstance(client, EmptyClient):
            return client
        else:
            return InvalidStorageClient(client.reason)





