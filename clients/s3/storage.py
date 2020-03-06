from clients.engines.storage import StorageClient
from clients.s3 import S3Client

class S3StorageClient(StorageClient):

    def __init__(self, config: dict):
        self.region = config.get("region")
        self.client = S3Client(self.get_config())

    def path(self, path: str):
        return self.client.path(path)

    def get_config(self):
        return {
            'region': self.region
        }



