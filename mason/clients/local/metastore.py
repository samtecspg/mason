from typing import Tuple, Union, Optional
from returns.result import Result

from mason.clients.engines.metastore import MetastoreClient
from mason.clients.local.local_client import LocalClient
from mason.clients.local.storage import LocalStorageClient
from mason.clients.response import Response

from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase, DatabaseList
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table.invalid_table import InvalidTables, InvalidTable
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.metastore.models.table.table import TableList, Table
from mason.engines.metastore.models.table.tables import infer, summarize
from mason.engines.storage.models.path import Path, InvalidPath


class LocalMetastoreClient(MetastoreClient):

    def __init__(self, client: LocalClient):
        self.client: LocalClient = client

    def summarize_table(self, table: Table, options: dict = {}, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        storage = LocalStorageClient(self.client)
        return summarize(table, storage, options, response)

    def delete_table(self, table_path: str, response: Response = Response()) -> Response:
        raise NotImplementedError("delete_table not implemented")

    def get_databases(self, response: Response = Response()) -> Tuple[DatabaseList, Response]:
        raise NotImplementedError("Local client get_databases not implemented")

    def get_database(self, database_name: str, response: Optional[Response] = Response()) -> Tuple[Result[Database, InvalidDatabase], Response]:
        raise NotImplementedError("get_database not implemented")

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        raise NotImplementedError("list_tables not implemented")

    def get_table(self, table_path: str, options: dict = {}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        path: Union[Path, InvalidPath] = self.parse_path(table_path, "file")
        if isinstance(path, Path):
            return infer(path, LocalStorageClient(self.client), None, options, response)
        else:
            return InvalidTables([InvalidTable(f"Bad Path: {path.reason}")]), response

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        raise NotImplementedError("credentials not implemented")

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return InvalidDDLStatement("Client not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return InvalidJob("Client 'execute_ddl' not implemented"), response or Response()
    