from clients.aws_client import AWSClient

from clients.engines.metastore import MetastoreClient
from clients.response import Response
from clients.glue import GlueClient
from typing import Union, Optional

from engines.metastore.models.database import Database, InvalidDatabase
from engines.metastore.models.table import Table, InvalidTable


class GlueMetastoreClient(MetastoreClient):

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

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None) -> Union[Table, InvalidTable]:
        return self.client.get_table(database_name, table_name)

