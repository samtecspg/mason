from clients.engines.storage import StorageClient
from clients.aws.s3 import S3Client

class S3StorageClient(StorageClient):

    def __init__(self, config: dict):
        self.region = config.get("aws_region")
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")
        self.client = S3Client(self.get_config())

    def path(self, path: str):
        return self.client.path(path)

    def get_path(self, path: str):
        return self.client.get_path(path)

    def get_config(self):
        return {
            'region': self.region
        }



