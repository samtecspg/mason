from engines.metastore.models.credentials.aws import AWSCredentials

from clients.engines.metastore import MetastoreClient
from clients.response import Response
from clients.glue import GlueClient
from engines.metastore.models.credentials import MetastoreCredentials
from typing import Tuple, List

from engines.metastore.models.schemas import MetastoreSchema


class GlueMetastoreClient(MetastoreClient):

    def __init__(self, config: dict):
        self.client = GlueClient(config)

    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        response = self.client.delete_table(database_name, table_name, response)
        return response

    def list_tables(self, database_name: str, response: Response) -> Response:
        response = self.client.list_tables(database_name, response)
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict = {}) -> Tuple[List[MetastoreSchema], Response]:
        schema, response = self.client.get_table(database_name, table_name, response)
        return [schema], response

    def credentials(self) -> MetastoreCredentials:
        return AWSCredentials(self.client.access_key, self.client.secret_key)

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Client not implemented")
        return ""

    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Client not implemented")
        return ("", "")

