
from clients.glue.metastore import GlueMetastoreClient
from clients.s3.metastore import S3MetastoreClient
from clients.engines.metastore import EmptyMetastoreClient
from engines import Engine

class MetastoreEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("metastore", config)
        self.client = self.get_client(self.client_name, self.config_doc)

    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "glue":
            return GlueMetastoreClient(config_doc)
        elif client_name == "s3":
            return S3MetastoreClient(config_doc)
        else:
            return EmptyMetastoreClient()

