
from abc import abstractmethod
from typing import List, Union, Optional

from mason.clients.client import Client
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.database import InvalidDatabase, Database
from mason.engines.metastore.models.ddl import DDLStatement, InvalidDDLStatement
from mason.engines.metastore.models.table import Table, InvalidTable
from mason.engines.storage.models.path import Path


class MetastoreClient(Client):

    @abstractmethod
    def get_database(self, database_name: str) -> Union[Database, InvalidDatabase]:
        return InvalidDatabase("Client get_database not implemented")

    @abstractmethod
    def list_tables(self, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    @abstractmethod
    def get_table(self, database_name: str, table_name: str, options: Optional[dict] = None) -> Union[Table, InvalidTable, List[InvalidTable]]:
        return InvalidTable("Client get_table not implemented")

    @abstractmethod
    def delete_table(self, database_name: str, table_name: str, response: Response) -> Response:
        raise NotImplementedError("Client delete_table not implemented")
        return response

    @abstractmethod
    def credentials(self) -> Union[MetastoreCredentials, InvalidCredentials]:
        return InvalidCredentials("Client not implemented")

    @abstractmethod
    def generate_table_ddl(self, table: Table, output_path: Optional[Path] = None) -> Union[DDLStatement, InvalidDDLStatement]:
        return InvalidDDLStatement("Client not implemented")

    @abstractmethod
    def execute_ddl(self, ddl: DDLStatement, database: Database) -> Union[ExecutedJob, InvalidJob]:
        return ExecutedJob(Job("generic"))


