
from abc import abstractmethod
from typing import List, Union, Optional, Tuple

from mason.clients.client import Client
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.database import InvalidDatabase, Database
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table, InvalidTable, InvalidTables
from mason.engines.storage.models.path import Path


class MetastoreClient(Client):

    @abstractmethod
    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Union[Database, InvalidDatabase], Response]:
        raise NotImplementedError("Client get_database not implemented")

    @abstractmethod
    def list_tables(self, database_name: str, response: Response) -> Response: 
        raise NotImplementedError("Client list_tables not implemented")

    @abstractmethod
    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None, response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        raise NotImplementedError("Client get_table not implemented")

    @abstractmethod
    def delete_table(self, database_name: str, table_name: str, response: Optional[Response] = None) -> Response:
        raise NotImplementedError("Client delete_table not implemented")

    @abstractmethod
    def credentials(self) -> Union[MetastoreCredentials, InvalidCredentials]:
        raise NotImplementedError("Client credentials not implemented")

    @abstractmethod
    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        raise NotImplementedError("Client generate_table_ddl not implemented")

    @abstractmethod
    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        raise NotImplementedError("Client execute_ddl not implemented")



class InvalidMetastoreClient(InvalidClient, MetastoreClient):
    
    def __init__(self, reason: str):
        super().__init__(reason)

    def get_database(self, database_name: str, response: Optional[Response] = None) -> Tuple[Union[Database, InvalidDatabase], Response]:
        raise NotImplementedError("Client get_database not implemented")

    def list_tables(self, database_name: str,
                    response: Response) -> Response:  # TODO: Make return Union[List[Tables], List[InvalidTables]]
        raise NotImplementedError("Client list_tables not implemented")

    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None,
                  response: Optional[Response] = None) -> Tuple[Union[Table, InvalidTables], Response]:
        raise NotImplementedError("Client get_table not implemented")

    def delete_table(self, database_name: str, table_name: str, response: Optional[Response] = None) -> Response:
        raise NotImplementedError("Client delete_table not implemented")

    def credentials(self) -> Union[MetastoreCredentials, InvalidCredentials]:
        raise NotImplementedError("Client credentials not implemented")

    def generate_table_ddl(self, table: Table, path: Path, database: Database) -> Union[DDLStatement, InvalidDDLStatement]:
        raise NotImplementedError("Client generate_table_ddl not implemented")

    def execute_ddl(self, ddl: DDLStatement, database: Database, response: Optional[Response] = None) -> Tuple[
        Union[ExecutedJob, InvalidJob], Response]:
        raise NotImplementedError("Client execute_ddl not implemented")
