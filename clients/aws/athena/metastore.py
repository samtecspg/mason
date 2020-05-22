from engines.execution.models.jobs import ExecutedJob, InvalidJob
from engines.execution.models.jobs.query_job import QueryJob
from engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials

from clients.aws.athena import AthenaClient
from clients.aws.aws_client import AWSClient

from clients.engines.metastore import MetastoreClient
from clients.response import Response
from typing import Tuple, List, Union, Optional

from engines.metastore.models.database import Database, InvalidDatabase
from engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from engines.metastore.models.schemas import Schema, emptySchema
from engines.metastore.models.table import Table
from engines.storage.models.path import Path


class AthenaMetastoreClient(MetastoreClient, AWSClient):

    def __init__(self, config: dict):
        self.client = AthenaClient(config)

    def generate_table_ddl(self, table: Table, output_path: Optional[Path] = None) -> Union[DDLStatement, InvalidDDLStatement]:
        return self.client.generate_table_ddl(table, output_path)

    def get_database(self, database_name: str) -> Union[Database, InvalidDatabase]:
        return self.client.get_database(database_name)

    def list_tables(self, database_name: str, response: Response) -> Response:
        response.add_error("Athena client list_tables not Implemented")
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict) -> Tuple[List[Schema], Response]:
        response.add_error("Athena client get_table not Implemented")
        return [], response

    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        response.add_error("Athena client delete_table not Implemented")
        return response

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Athena Client full_path not implemented")
        return ""

    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Athena Client parse_path not implemented")
        return ("", "")

    def execute_ddl(self, ddl: DDLStatement, database: Database) -> Union[ExecutedJob, InvalidJob] :
        return self.client.execute_ddl(ddl, database)


