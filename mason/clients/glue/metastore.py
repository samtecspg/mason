from typing import Union, Optional, List, Tuple

from mason.clients.engines.metastore import MetastoreClient
from mason.clients.engines.valid_client import ValidClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table, InvalidTables
from mason.engines.storage.models.path import Path

class GlueMetastoreClient(MetastoreClient, ValidClient):

    def __init__(self, config: dict):
        self.client = GlueClient(config)

    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Union[Database, InvalidDatabase], Response]:
        return self.client.get_database(database_name)

    def delete_table(self, database_name: str, table_name: str, response: Optional[Response] = None) -> Response:
        return self.client.delete_table(database_name, table_name, response)

    def list_tables(self, database_name: str, response: Response) -> Response:
        response = self.client.list_tables(database_name, response)
        return response

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        return self.client.get_table(database_name, table_name, response)

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return InvalidCredentials("Client 'credentials' not implemented")

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return InvalidDDLStatement("Client not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return InvalidJob("Client 'execute_ddl' not implemented"), response or Response()



