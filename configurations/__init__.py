from util.printer import banner

from util.yaml import parse_yaml
from util.json import print_json_1level

from util.logger import logger
from engines.metastore import MetastoreEngine
from engines.scheduler import SchedulerEngine
from engines.storage import StorageEngine
from engines.execution import ExecutionEngine
from util.environment import MasonEnvironment
from typing import List
from util.json_schema import validate_schema
import os

def get_all(env: MasonEnvironment):
    logger.debug(f"Reading configurations at {env.config_home}")

    configs: List[Config] = []
    for subdir, dirs, files in os.walk(env.config_home):
        for file in files:
            yaml_config_doc: dict = parse_yaml(env.config_home + file) or {}
            configs.append(Config(env, yaml_config_doc))

    return configs

class Config:

    def __init__(self, env: MasonEnvironment, config: dict):
        valid = validate_schema(config, env.config_schema)

        if valid:
            self.metastore = MetastoreEngine(config)
            self.scheduler = SchedulerEngine(config)
            self.storage = StorageEngine(config)
            self.execution = ExecutionEngine(config)

            self.engines = {
                'metastore': self.metastore.to_dict(),
                'scheduler': self.scheduler.to_dict(),
                'storage': self.storage.to_dict(),
                'execution': self.execution.to_dict()
            }

        else:
            logger.error(f"\nInvalid config schema: {config}\n")
            self.engines = {}

        banner("Configuration")
        logger.info(f"Configuration: {self.engines}")

    def has_execution(self):
        self.execution and self.execution.client

    # def print(self):
        # print_json_1level(self.engines)

    # # TODO:  Make validation more specific to engine type
    # def client_names(self) -> Set[str]:
    #     if not self.config == {}:
    #         return set(flatten_string([
    #             self.metastore.client_name,
    #             self.scheduler.client_name,
    #             self.storage.client_name,
    #             self.execution.client_name,
    #         ]))
    #     else: return set([])

