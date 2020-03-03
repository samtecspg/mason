from clients import Client
from clients.response import Response

class MetastoreConfig(object):

    def __init__(self, config: dict):
        self.client_name = config.get("metastore_client", "")
        self.client = Client().get(self.client_name, config.get("clients", {}))

    def to_dict(self):
        return {
            'client': self.client_name,
            'configuration': self.client.__dict__
        }

