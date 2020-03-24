from engines import Engine
from clients.s3.storage import S3StorageClient

class StorageEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("storage", config)
        self.client = self.get_client(self.client_name, self.config_doc)

    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "s3":
            return S3StorageClient(config_doc)
