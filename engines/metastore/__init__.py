
from clients.glue.metastore import GlueMetastoreClient
from clients.s3.metastore import S3MetastoreClient
from clients.engines.metastore import EmptyMetastoreClient
from engines import Engine
from util.logger import logger

class MetastoreEngine(Engine):

    def __init__(self, config: dict, valid: bool = True):
        super().__init__("metastore", config)
        if valid and self.valid:
            self.client = self.get_client(self.client_name, self.config_doc)
        else:
            self.client = EmptyMetastoreClient()

    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "glue":
            return GlueMetastoreClient(config_doc)
        elif client_name == "s3":
            return S3MetastoreClient(config_doc)
        else:
            return EmptyMetastoreClient()


