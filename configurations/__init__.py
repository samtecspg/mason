from configurations.metastore import MetastoreConfig

from util.yaml import parse_yaml


class Config(object):

    def __init__(self, location: str = "config.yaml"):
        print(f"Reading configuration at {location}:")
        config_doc = parse_yaml(location)
        # TODO: Validate config object structure
        metastore_config_doc = config_doc.get("metastore", {})
        metastore_config = MetastoreConfig(metastore_config_doc)

        self.metastore_config = MetastoreConfig(metastore_config_doc)
        self.storage_config = None
        self.execution_config = None
        self.scheduler_config = None

        self.print()

    def metastore_client(self):
        self.metastore_config.client

    def storage_client(self):
        self.storage_config.client

    def execution_client(self):
        self.execution_config.client

    def execution_client(self):
        self.execution_config.client


    def print(self):
        print({
            'metastore_config': self.metastore_config.to_dict()
        })
