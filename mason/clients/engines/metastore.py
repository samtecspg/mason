
from abc import abstractmethod
from typing import Union, Optional, Tuple, List

from returns.result import Result

from mason.clients.base import Client
from mason.clients.response import Response
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.database import InvalidDatabase, Database, DatabaseList
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table.table import Table, TableList
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.summary import TableSummary
from mason.engines.storage.models.path import Path

class MetastoreClient:
    
    @abstractmethod
    def __init__(self, client: Client):
        self.client = client

    @abstractmethod
    def get_databases(self,response: Optional[Response] = None) -> Tuple[DatabaseList, Response]:
        raise NotImplementedError

    @abstractmethod
    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Result[Database, InvalidDatabase], Response]:
        raise NotImplementedError

    @abstractmethod
    def list_tables(self, database_name: str, response: Response) -> Tuple[Result[TableList, InvalidTables], Response]: 
        raise NotImplementedError("Client list_tables not implemented")

    @abstractmethod
    def get_table(self, database_name: str, table_name: str, options: dict = {}, response: Response = Response()) -> Tuple[Union[Table, InvalidTables], Response]:
        raise NotImplementedError("Client get_table not implemented")

    @abstractmethod
    def summarize_table(self, table: Table, options: dict = {}, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
        raise NotImplementedError("Client summarize_table not implemented")

    @abstractmethod
    def delete_table(self, database_name: str, table_name: str, response: Optional[Response] = None) -> Response:
        raise NotImplementedError("Client delete_table not implemented")

    @abstractmethod
    def credentials(self) -> Union[MetastoreCredentials, InvalidCredentials]:
        raise NotImplementedError("Client credentials not implemented")

    @abstractmethod
    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        raise NotImplementedError("Client generate_table_ddl not implemented")

