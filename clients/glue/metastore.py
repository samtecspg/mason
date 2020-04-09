
from clients.engines.metastore import MetastoreClient
from clients.response import Response
from clients.glue import GlueClient
from engines.metastore.models.credentials import MetastoreCredentials


class GlueMetastoreClient(MetastoreClient):

    def __init__(self, config: dict):
        self.region = config.get("region")
        self.aws_role_arn = config.get("aws_role_arn")
        self.client = GlueClient(self.get_config())

    def list_tables(self, database_name: str, response: Response) -> Response:
        response = self.client.list_tables(database_name, response)
        return response

    def get_table(self, database_name: str, table_name: str, response: Response) -> Response:
        response = self.client.get_table(database_name, table_name, response)
        return response

    def get_config(self):
        return {
            'region': self.region,
            'aws_role_arn': self.aws_role_arn
        }

    def credentials(self) -> MetastoreCredentials:
        raise NotImplementedError("Client not implemented")
        return MetastoreCredentials()

    def full_path(self, path: str) -> str:
        raise NotImplementedError("Client not implemented")
        return ""

