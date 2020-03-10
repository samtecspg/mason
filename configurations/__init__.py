from util.printer import banner

from util.yaml import parse_yaml
from util.json import print_json_1level

from util.logger import logger
from engines.metastore import MetastoreEngine
from engines.scheduler import SchedulerEngine
from engines.storage import StorageEngine
from engines.execution import ExecutionEngine
from util.environment import MasonEnvironment
from typing import Optional

class Config:

    def __init__(self, env: MasonEnvironment, config_doc: Optional[dict] = None):

        if not config_doc:
            config_home = env.config_home
            logger.debug(f"Reading configuration at {config_home}:")
            yaml_config_doc: dict = parse_yaml(config_home) or {}

        cd: dict = config_doc or yaml_config_doc

        self.env = env
        self.metastore = MetastoreEngine(cd)
        self.scheduler = SchedulerEngine(cd)
        self.storage = StorageEngine(cd)
        self.execution = ExecutionEngine(cd)

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

