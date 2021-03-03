from typing import Union, Optional, Tuple

from returns.result import Result

from mason.clients.engines.metastore import MetastoreClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table, InvalidTables, TableList
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.storage.models.path import Path

class GlueMetastoreClient(MetastoreClient):
    
    def __init__(self, client: GlueClient):
        self.client: GlueClient = client

    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Result[Database, InvalidDatabase], Response]:
        return self.client.get_database(database_name, response)
    
    def summarize_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        response.add_error("Client summarize_table not implemented")
        return InvalidTables([]), response

    def delete_table(self, database_name: str, table_name: str, response: Optional[Response] = None) -> Response:
        return self.client.delete_table(database_name, table_name, response)

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        return self.client.list_tables(database_name, response)

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        return self.client.get_table(database_name, table_name, response)

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return InvalidCredentials("Client 'credentials' not implemented")

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return InvalidDDLStatement("Client not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return InvalidJob("Client 'execute_ddl' not implemented"), response or Response()



