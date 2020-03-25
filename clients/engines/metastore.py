
from clients.response import Response
from clients import Client
from abc import abstractmethod

class MetastoreClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific metastore client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'GlueMetastoreClient' with abstract attribute 'get_table' (for example)

    @abstractmethod
    def list_tables(self, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    @abstractmethod
    def get_table(self, database_name: str, table_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response



class EmptyMetastoreClient(MetastoreClient):

    def list_tables(self, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

    def get_table(self, database_name: str, table_name: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

