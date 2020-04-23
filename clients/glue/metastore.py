from engines.metastore.models.credentials.aws import AWSCredentials

from clients.engines.metastore import MetastoreClient
from clients.response import Response
from clients.glue import GlueClient
from engines.metastore.models.credentials import MetastoreCredentials
from typing import Tuple, List

from engines.metastore.models.schemas import MetastoreSchema


class GlueMetastoreClient(MetastoreClient):

    def __init__(self, config: dict):
        self.aws_region = config.get("aws_region") or ""
        self.aws_role_arn = config.get("aws_role_arn") or ""
        self.access_key = config.get("access_key") or ""
        self.secret_key = config.get("secret_key") or ""
        self.client = GlueClient(self.get_config())

    def list_tables(self, database_name: str, response: Response) -> Response:
        response = self.client.list_tables(database_name, response)
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict = {}) -> Tuple[List[MetastoreSchema], Response]:
        schema, response = self.client.get_table(database_name, table_name, response)
        return [schema], response

    def get_config(self):
        return {
            'aws_region': self.aws_region,
            'aws_role_arn': self.aws_role_arn,
            'access_key': self.access_key,
            'secret_key': self.secret_key
        }

    def credentials(self) -> MetastoreCredentials:
        return AWSCredentials(self.access_key, self.secret_key)

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Client not implemented")
        return ""

    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Client not implemented")
        return ("", "")

