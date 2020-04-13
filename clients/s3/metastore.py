from clients.engines.metastore import MetastoreClient
from clients.response import Response
from clients.s3 import S3Client
from engines.metastore.models.credentials.aws import AWSCredentials
from urllib.parse import urlparse
import os

class S3MetastoreClient(MetastoreClient):

    def __init__(self, config: dict):
        self.region = config.get("region")
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")
        self.client = S3Client(self.get_config())

    def list_tables(self, database_name: str, response: Response) -> Response:
        response = self.client.list_tables(database_name, response)
        return response

    def get_table(self, database_name: str, table_name: str, response: Response) -> Response:
        response = self.client.get_table(database_name, table_name, response)
        return response

    def credentials(self):
        return AWSCredentials(self.access_key, self.secret_key)

    def full_path(self, path: str) -> str:
        return "s3a://" + path

    def parse_path(self, path: str) -> (str, str):
        parsed = urlparse(self.full_path(path), allow_fragments=False)
        key = parsed.path.lstrip("/")
        path = os.path.split(key)[0]
        bucket = parsed.netloc
        return bucket, path

    def get_config(self):
        return {
            'region': self.region,
            'access_key': self.access_key,
            'secret_key': self.secret_key
        }

