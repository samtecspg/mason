from urllib.parse import urlparse
from typing import Tuple, Union, Optional, List

from returns.result import Result, Success, Failure

from mason.clients.engines.metastore import MetastoreClient
from mason.clients.response import Response
from mason.clients.s3.s3_client import S3Client
from mason.clients.s3.storage import S3StorageClient
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase, DatabaseList
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table.invalid_table import InvalidTables, InvalidTable
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.metastore.models.table.table import Table, TableList
from mason.engines.metastore.models.table.tables import infer, summarize
from mason.engines.storage.models.path import Path, InvalidPath
from mason.util.list import sequence

class S3MetastoreClient(MetastoreClient):

    def __init__(self, client: S3Client):
        self.client: S3Client = client

    def summarize_table(self, table: Table, options: dict = {}, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        storage = S3StorageClient(self.client)
        return summarize(table, storage, options, response)

    def delete_table(self, table_path: str, response: Response = Response()) -> Response:
        raise NotImplementedError("Client delete table not implemented")

    def get_databases(self, response: Response = Response()) -> Tuple[DatabaseList, Response]:
        raise NotImplementedError("S3 client get_databases not implemented")

    def get_database(self, database_name: str, response: Optional[Response] = Response()) -> Tuple[Result[Database, InvalidDatabase], Response]:
        tables, response =  self.list_tables(database_name, response or Response())
        database = tables.map(lambda a: Database("s3_table", a)).alt(lambda b: InvalidDatabase(b.error or b.message()))
        return database, response

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        tables, response = self.client.list_objects(database_name, response)
        result: Result[dict, InvalidTables] = tables.alt(lambda e: InvalidTables([], e))

        def parse_response(result: dict, response: Response) -> Result[TableList, InvalidTables]:
            contents: Optional[List[dict]] = result.get("Contents")
            prefixes: Optional[List[dict]] = result.get("CommonPrefixes")

            if contents:
                tables: List[Union[Table, InvalidTables]] = []
                for c in contents:
                    key: Optional[str] = c.get("Key")
                    if key:
                        table_path = "/".join([database_name.split("/")[0], key])
                        table, response = self.get_table(table_path , response=response)
                        tables.append(table)
                valid, invalid = sequence(tables, Table, InvalidTables)
                if len(valid) > 0:
                    return Success(TableList(valid))
                else:
                    invalid_tables: List[InvalidTable] = []
                    for i in invalid:
                        invalid_tables += (i.invalid_tables)

                    return Failure(InvalidTables(invalid_tables, f"No valid tables at {database_name}"))
            elif prefixes:
                for p in prefixes:
                    response.add_data(p)
                return Failure(InvalidTables([], f"No valid tables at {database_name}.  Try appending '/' or specify deeper key."))
            else:
                return Failure(InvalidTables([], "No Data returned from AWS"))

        # TODO:  response is not pure here
        final = result.bind(lambda r: parse_response(r, response))

        return final, response

    def get_table(self, table_path: str, options: dict = {}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        storage = S3StorageClient(self.client)
        path: Union[Path, InvalidPath] = self.parse_table_path(table_path, "s3")
        
        return infer(path, storage, None, options, response)

    def full_path(self, path: str) -> str:
        return "s3a://" + path

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return self.client.credentials()

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return InvalidDDLStatement("Client not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return InvalidJob("Client 'execute_ddl' not implemented"), response or Response()
