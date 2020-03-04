from configurations.metastore import MetastoreConfig
from configurations.storage import StorageConfig
from configurations.scheduler import SchedulerConfig
from configurations.execution import ExecutionConfig
from util.printer import banner

from util.yaml import parse_yaml
from util import environment as env
from util.json import print_json_1level

class Config(object):

    def __init__(self):
        config_home = env.CONFIG_HOME
        banner(f"Reading configuration at {config_home}:")
        config_doc = parse_yaml(config_home)
        # TODO: Validate config object structure
        self.metastore_config = MetastoreConfig(config_doc)
        self.storage_config = StorageConfig(config_doc)
        self.scheduler_config = SchedulerConfig(config_doc)
        self.execution_config = ExecutionConfig(config_doc)

        self.print()

    def print(self):
        print_json_1level({
            'metastore_config': self.metastore_config.to_dict(),
            'storage_config': self.storage_config.to_dict(),
            'scheduler_config': self.scheduler_config.to_dict(),
            'execution_config': self.execution_config.to_dict()
        })
