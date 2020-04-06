from engines import Engine
from clients.s3.storage import S3StorageClient
from clients.engines.storage import EmptyStorageClient
from util.logger import logger

class StorageEngine(Engine):

    def __init__(self, config: dict, valid: bool = True):
        super().__init__("storage", config)
        if valid and self.valid:
            self.client = self.get_client(self.client_name, self.config_doc)
        else:
            self.client = EmptyStorageClient()


    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "s3":
            return S3StorageClient(config_doc)
        else:
            return EmptyStorageClient()
