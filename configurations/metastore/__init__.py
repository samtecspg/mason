from configurations.metastore.glue.client import GlueMetastoreClient
from typing import Optional

class MetastoreConfig(object):

    def __init__(self, metastore_config: dict):
        client: Optional[str] = metastore_config.get("client")
        args: dict = metastore_config.get("configuration", {})

        #  TODO: Automate
        if client == "glue":
            region: Optional[str] = args.get("region", None)
            self.client = GlueMetastoreClient(region)  # TODO: Validate glue client schema

        self.client_name = client

    def to_dict(self):
        return {
            'client': self.client_name,
            'configuration': self.client.__dict__
        }

