from typing import Optional, Union, List, Tuple

from returns.result import Result

from mason.clients.athena.athena_client import AthenaClient
from mason.clients.engines.metastore import MetastoreClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.glue.metastore import GlueMetastoreClient
from mason.clients.response import Response
from mason.engines.metastore.models.credentials import InvalidCredentials
from mason.engines.metastore.models.credentials.aws import AWSCredentials
from mason.engines.metastore.models.database import Database, InvalidDatabase, DatabaseList
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.metastore.models.table.table import Table, TableList
from mason.engines.storage.models.path import Path

class AthenaMetastoreClient(MetastoreClient):

    def __init__(self, client: AthenaClient):
        super().__init__(client)
        self.client: AthenaClient = client
        #  Glue is the metastore for athena
        self.glue_client = GlueClient(access_key=self.client.access_key, secret_key=self.client.secret_key, aws_region=self.client.aws_region)

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        return self.client.generate_table_ddl(table, path, database)

    def get_database(self, database_name: str, response: Optional[Response] = Response()) -> Tuple[Result[Database, InvalidDatabase], Response]:
        return self.glue_client.get_database(database_name, response)

    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]:
        return self.glue_client.list_tables(database_name, response)

    def get_table(self, table_path: str, options: Optional[dict] = None, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        metastore = GlueMetastoreClient(self.glue_client)
        return metastore.get_table(table_path, options, response)

    def delete_table(self, table_path: str, response: Response = Response()) -> Response:
        glue_metastore = GlueMetastoreClient(self.glue_client)
        return glue_metastore.delete_table(table_path, response)

    def get_databases(self, response: Response = Response()) -> Tuple[DatabaseList, Response]:
        raise NotImplementedError("Athena client get_databases not implemented")

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Athena Client full_path not implemented")

    def credentials(self) -> Union[AWSCredentials, InvalidCredentials]:
        return self.client.credentials()

    def list_partitions(self, table: Table, response: Optional[Response] = None) ->  Tuple[List[Path], Response]:
        # SELECT "$path" FROM "my_database"."my_table" WHERE year=2019;
        raise NotImplementedError("Athena Client list_partitions not implemented")
    
    def summarize_table(self, table: Table, options: dict = {}, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        raise NotImplementedError("Client summarize_table not implemented")


