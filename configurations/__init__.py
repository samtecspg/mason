from configurations.metastore import MetastoreConfig
from configurations.storage import StorageConfig
from configurations.scheduler import SchedulerConfig
from util.printer import pprint, banner

from util.yaml import parse_yaml


class Config(object):

    def __init__(self, location: str = "config.yaml"):
        banner(f"Reading configuration at {location}:")
        config_doc = parse_yaml(location)
        # TODO: Validate config object structure
        self.metastore_config = MetastoreConfig(config_doc)
        self.storage_config = StorageConfig(config_doc)
        self.scheduler_config = SchedulerConfig(config_doc)
        self.execution_config = None

        self.print()

    def print(self):
        pprint({
            'metastore_config': self.metastore_config.to_dict(),
            'storage_config': self.storage_config.to_dict(),
            'scheduler_config': self.scheduler_config.to_dict()
        })
