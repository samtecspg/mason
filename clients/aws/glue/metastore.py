from clients.aws.aws_client import AWSClient

from clients.engines.metastore import MetastoreClient
from clients.response import Response
from clients.aws.glue import GlueClient
from typing import Tuple, List, Union

from engines.metastore.models.database import Database, InvalidDatabase
from engines.metastore.models.schemas import Schema

class GlueMetastoreClient(MetastoreClient, AWSClient):

    def __init__(self, config: dict):
        self.client = GlueClient(config)

    def get_database(self, database_name: str) -> Union[Database, InvalidDatabase]:
        return self.client.get_database(database_name)

    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        response = self.client.delete_table(database_name, table_name, response)
        return response

    def list_tables(self, database_name: str, response: Response) -> Response:
        response = self.client.list_tables(database_name, response)
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict = {}) -> Tuple[List[Schema], Response]:
        schema, response = self.client.get_table(database_name, table_name, response)
        return [schema], response


    def full_path(self, path: str) -> str:
        raise NotImplementedError("Client not implemented")
        return ""

    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Client not implemented")
        return ("", "")

