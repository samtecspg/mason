from clients.aws import AWSClient
from clients.aws.athena import AthenaClient
from engines.metastore.models.credentials.aws import AWSCredentials
from clients.engines.metastore import MetastoreClient
from clients.response import Response
from engines.metastore.models.credentials import MetastoreCredentials
from typing import Tuple, List

from engines.metastore.models.database import Database
from engines.metastore.models.schemas import MetastoreSchema
from engines.metastore.models.schemas.metastore_schema import EmptyMetastoreSchema

class AthenaMetastoreClient(MetastoreClient, AWSClient):

    def __init__(self, config: dict):
        self.client = AthenaClient(config)

    def get_database(self, database_name: str) -> Database:
        raise NotImplementedError("Client not implemented")
        return Database()

    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    def list_tables(self, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict = {}) -> Tuple[List[MetastoreSchema], Response]:
        raise NotImplementedError("Client not implemented")
        return [EmptyMetastoreSchema()], response


    def full_path(self, path: str) -> str:
        raise NotImplementedError("Client not implemented")
        return ""

    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Client not implemented")
        return ("", "")


