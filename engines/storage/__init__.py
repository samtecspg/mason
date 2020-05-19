from clients.engines.invalid_client import InvalidClient
from clients.engines.valid_client import ValidClient
from engines.engine import Engine
from clients.aws.s3.storage import S3StorageClient
from engines.storage.models.path import Path


class StorageEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("storage", config)
        self.client = self.get_client()

    def get_client(self):
        client = self.validate()
        if isinstance(client, ValidClient):
            if client.client_name == "s3":
                return S3StorageClient(client.config)
            else:
                return InvalidClient(f"Client type not supported {client.client_name}")
        else:
            return client





