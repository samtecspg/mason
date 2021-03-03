from urllib.parse import urlparse
from typing import Tuple, Union, Optional

from returns.result import Result

from mason.clients.engines.metastore import MetastoreClient
from mason.clients.response import Response
from mason.clients.s3.s3_client import S3Client
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table, InvalidTables, TableList
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.storage.models.path import Path

class S3MetastoreClient(MetastoreClient):

    def __init__(self, client: S3Client):
        self.client: S3Client = client

    def summarize_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        return self.client.summarize_table(database_name, table_name, options, response)

    def delete_table(self, database_name: str, table_name: str, response: Optional[Response] = None) -> Response:
        resp = (response or Response()).add_error("Client delete_table not implemented")
        return resp

    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Result[Database, InvalidDatabase], Response]:
        tables, response =  self.list_tables(database_name, response or Response())
        database = tables.map(lambda a: Database("s3_table", a)).alt(lambda b: InvalidDatabase(b.error or b.message()))
        return database, response

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        return self.client.list_tables(database_name, response)

    def get_table(self, database_name: str, table_name: str, options: dict = {}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        return self.client.get_table(database_name, table_name, options, response)

    def full_path(self, path: str) -> str:
        return "s3a://" + path

    def parse_path(self, path: str) -> Tuple[str, str]:
        parsed = urlparse(self.full_path(path), allow_fragments=False)
        key = parsed.path.lstrip("/")
        bucket = parsed.netloc
        return bucket, key

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return self.client.credentials()

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return InvalidDDLStatement("Client not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return InvalidJob("Client 'execute_ddl' not implemented"), response or Response()
