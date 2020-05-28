from typing import Optional, Union, List, Tuple

from mason.clients.athena import AthenaClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table, InvalidTable
from mason.engines.storage.models.path import Path


class AthenaMetastoreClient(MetastoreClient):

    def __init__(self, config: dict):
        self.client = AthenaClient(config)

    def generate_table_ddl(self, table: Table, output_path: Optional[Path] = None) -> Union[DDLStatement, InvalidDDLStatement]:
        return self.client.generate_table_ddl(table, output_path)

    def get_database(self, database_name: str) -> Union[Database, InvalidDatabase]:
        return self.client.get_database(database_name)

    def list_tables(self, database_name: str, response: Response) -> Response:
        response.add_error("Athena client list_tables not Implemented")
        return response

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        return InvalidTable("Client get_table not implemented")

    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        response.add_error("Athena client delete_table not Implemented")
        return response

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Athena Client full_path not implemented")
        return ""

    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Athena Client parse_path not implemented")
        return ("", "")

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return InvalidCredentials("Client 'credentials' not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database) -> Union[ExecutedJob, InvalidJob]:
        return self.client.execute_ddl(ddl, database)


