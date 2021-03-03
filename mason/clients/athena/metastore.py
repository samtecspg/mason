from typing import Optional, Union, List, Tuple

from returns.result import Result

from mason.clients.athena.athena_client import AthenaClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table, InvalidTables, TableList
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.storage.models.path import Path

class AthenaMetastoreClient(MetastoreClient):

    def __init__(self, client: AthenaClient):
        super().__init__(client)
        self.client: AthenaClient = client

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return self.client.generate_table_ddl(table, path, database)

    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Result[Database, InvalidDatabase], Response]:
        return self.client.get_database(database_name, response)
    
    def summarize_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        response.add_error("Client summarize_table not implemented")
        return InvalidTables([]), response

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        raise NotImplementedError("Athena Client list_tables not implemented")

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        raise NotImplementedError("Athena Client get_table not implemented")

    def delete_table(self, database_name: str, table_name: str, response: Optional[Response] = None) -> Response:
        raise NotImplementedError("Athena Client delete_table not implemented")

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Athena Client full_path not implemented")

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return InvalidCredentials("Client 'credentials' not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return self.client.execute_ddl(ddl, database, response)

    def list_partitions(self, table: Table, response: Optional[Response] = None) ->  Tuple[List[Path], Response]:
        # SELECT "$path" FROM "my_database"."my_table" WHERE year=2019;
        raise NotImplementedError("Athena Client list_partitions not implemented")


