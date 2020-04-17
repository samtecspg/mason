
from clients.response import Response
from clients import Client
from abc import abstractmethod
from engines.metastore.models.credentials import MetastoreCredentials
from typing import Tuple, List

from engines.metastore.models.schemas import MetastoreSchema, emptySchema


class MetastoreClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific metastore client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'GlueMetastoreClient' with abstract attribute 'get_table' (for example)

    @abstractmethod
    def list_tables(self, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    @abstractmethod
    def get_table(self, database_name: str, table_name: str, response: Response, options: dict) -> Tuple[List[MetastoreSchema], Response]:
        raise NotImplementedError("Client not implemented")
        return emptySchema(), response

    @abstractmethod
    def credentials(self) -> MetastoreCredentials:
        raise NotImplementedError("Client not implemented")
        return MetastoreCredentials()

    @abstractmethod
    def full_path(self, path: str) -> str:
        raise NotImplementedError("Client not implemented")
        return ""

    @abstractmethod
    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Client not implemented")
        return ("", "")


class EmptyMetastoreClient(MetastoreClient):

    def list_tables(self, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    def get_table(self, database_name: str, table_name: str, response: Response, options: dict) -> Tuple[List[MetastoreSchema], Response]:
        raise NotImplementedError("Client not implemented")
        return emptySchema(), response

    def credentials(self) -> MetastoreCredentials:
        raise NotImplementedError("Client not implemented")
        return MetastoreCredentials()

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Client not implemented")
        return ""

    def parse_path(self, path: str) -> Tuple[str, str]:
        raise NotImplementedError("Client not implemented")
        return ("", "")

