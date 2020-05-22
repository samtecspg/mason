from clients.aws.aws_client import AWSClient
from clients.engines.metastore import MetastoreClient
from clients.response import Response
from clients.aws.s3 import S3Client
from urllib.parse import urlparse
from typing import Tuple, List, Union

from engines.metastore.models.database import Database, InvalidDatabase
from engines.metastore.models.schemas import Schema

class S3MetastoreClient(MetastoreClient, AWSClient):

    def __init__(self, config: dict):
        self.region = config.get("aws_region")
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")
        self.client = S3Client(self.get_config())

    def get_database(self, database_name: str) -> Union[Database, InvalidDatabase]:
        return InvalidDatabase("S3 Client get_database not implemented")

    def list_tables(self, database_name: str, response: Response) -> Response:
        response = self.client.list_tables(database_name, response)
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict = {}) -> Tuple[List[Schema], Response]:
        return self.client.get_table(database_name, table_name, response, options)

    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return ("", "")

    def full_path(self, path: str) -> str:
        return "s3a://" + path

    def parse_path(self, path: str) -> Tuple[str, str]:
        parsed = urlparse(self.full_path(path), allow_fragments=False)
        key = parsed.path.lstrip("/")
        bucket = parsed.netloc
        return bucket, key

    def get_config(self):
        return {
            'region': self.region,
            'access_key': self.access_key,
            'secret_key': self.secret_key
        }

