from typing import Union, Optional, Tuple

from returns.result import Result

from mason.clients.athena.athena_client import AthenaClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.response import Response
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase, DatabaseList
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table.invalid_table import InvalidTables, InvalidTable
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.metastore.models.table.table import TableList, Table
from mason.engines.storage.models.path import Path, parse_table_path, InvalidPath


class GlueMetastoreClient(MetastoreClient):
    
    def __init__(self, client: GlueClient):
        self.client: GlueClient = client
        self.athena_client = AthenaClient(self.client.access_key, self.client.secret_key, self.client.aws_region)

    def get_databases(self, response: Response = Response()) -> Tuple[DatabaseList, Response]:
        return self.client.get_databases(response)
    
    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Result[Database, InvalidDatabase], Response]:
        return self.client.get_database(database_name, response)
    
    def summarize_table(self, table: Table, options: dict = {}, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        raise NotImplementedError("Client summarize_table not implemented")

    def delete_table(self, table_path: str, response: Response = Response()) -> Response:
        path = parse_table_path(table_path, "glue")
        if isinstance(path, InvalidPath):
            response.add_error(f"Invalid Path: ${path.reason}")
            return response
        else:
            return self.client.delete_table(path, response)

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        return self.client.list_tables(database_name, response)

    def get_table(self, table_path: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        path = self.parse_table_path(table_path, "glue")

        if isinstance(path, Path):
            return self.client.get_table(path, response)
        else:
            return InvalidTables([InvalidTable(f"Invalid Path: {path.reason}")]), response

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return self.client.credentials()

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return self.athena_client.generate_table_ddl(table, path, database)

