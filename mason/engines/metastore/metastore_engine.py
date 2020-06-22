from typing import Union

from mason.clients.engines.valid_client import ValidClient, EmptyClient

from mason.clients.engines.metastore import InvalidMetastoreClient, MetastoreClient
from mason.engines.engine import Engine

class MetastoreEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("metastore", config)
        self.client: Union[MetastoreClient, InvalidMetastoreClient, EmptyClient] = self.get_client()

    def get_client(self) -> Union[MetastoreClient, InvalidMetastoreClient, EmptyClient]:
        client = self.validate()
        if isinstance(client, ValidClient):
            if self.client_name == "glue":
                from mason.clients.glue.metastore import GlueMetastoreClient
                return GlueMetastoreClient(client.config)
            elif self.client_name == "s3":
                from mason.clients.s3.metastore import S3MetastoreClient
                return S3MetastoreClient(client.config)
            elif self.client_name == "athena":
                from mason.clients.athena.metastore import AthenaMetastoreClient
                return AthenaMetastoreClient(client.config)
            else:
                return InvalidMetastoreClient(f"Client type not supported: {client.client_name}")
        elif isinstance(client, EmptyClient):
            return client
        else:
            return InvalidMetastoreClient(f"Invalid Client. {client.reason}")

