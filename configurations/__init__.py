from util.printer import banner

from util.yaml import parse_yaml
from util import environment as env
from util.json import print_json_1level

from util.logger import logger
from engines.metastore import MetastoreEngine
from engines.scheduler import SchedulerEngine
from engines.storage import StorageEngine
from engines.execution import ExecutionEngine

class Config:

    def __init__(self):
        config_home = env.CONFIG_HOME
        logger.debug(f"Reading configuration at {config_home}:")
        config_doc = parse_yaml(config_home)
        self.metastore = MetastoreEngine(config_doc)
        self.scheduler = SchedulerEngine(config_doc)
        self.storage = StorageEngine(config_doc)
        self.execution = ExecutionEngine(config_doc)

        self.config = {
            'metastore': self.metastore.to_dict(),
            'scheduler': self.scheduler.to_dict(),
            'storage': self.storage.to_dict(),
            'execution': self.execution.to_dict()
        }


        banner("Configuration")
        self.print()


    def print(self):
        print_json_1level(self.config)

