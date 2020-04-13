from util.printer import banner

from util.yaml import parse_yaml
from util.session import set_session_config, get_session_config

from util.logger import logger
from util.list import get
from engines.metastore import MetastoreEngine
from engines.scheduler import SchedulerEngine
from engines.storage import StorageEngine
from engines.execution import ExecutionEngine
from util.environment import MasonEnvironment
from typing import List, Union, Optional
from util.json_schema import validate_schema
from os import walk
from tabulate import tabulate
from clients.response import Response

class Config:

    def __init__(self, env: MasonEnvironment, config: dict):
        valid = validate_schema(config, env.config_schema)

        self.metastore = MetastoreEngine(config, valid)
        self.scheduler = SchedulerEngine(config, valid)
        self.storage = StorageEngine(config, valid)
        self.execution = ExecutionEngine(config, valid)
        self.valid = valid and self.metastore.valid and self.scheduler.valid and self.storage.valid and self.execution.valid

        self.engines = {
            'metastore': self.metastore.to_dict(),
            'scheduler': self.scheduler.to_dict(),
            'storage': self.storage.to_dict(),
            'execution': self.execution.to_dict()
        }

        if valid:
            logger.debug(f"Valid Configuration: {self.engines}")
        else:
            logger.error(f"\nInvalid config schema: {config}\n")

    def extended_info(self, config_id: int, current: bool = False):
        ei = []

        if not self.metastore.client_name == "":
            ei.append(
                [
                    "",
                    "metastore",
                    self.metastore.client_name,
                    self.metastore.config_doc,
                ]
            )

        if not self.scheduler.client_name == "":
            ei.append(
                [
                    "",
                    "scheduler",
                    self.scheduler.client_name,
                    self.scheduler.config_doc,
                ]
            )

        if not self.storage.client_name == "":
            ei.append(
                [
                    "",
                    "storage",
                    self.storage.client_name,
                    self.storage.config_doc,
                ]
            )

        if not self.execution.client_name == "":
            ei.append(
                [
                    "",
                    "execution",
                    self.execution.client_name,
                    self.execution.config_doc,
                ]
            )

        def with_id_item(l, current: bool):
            if current == True:
                cid = "*  " + str(config_id)
            else:
                cid = ".  " + str(config_id)
            return [ cid if l.index(x) == 0 else x for x in l]

        with_id = [ with_id_item(x, current) if ei.index(x) == 0 else x for x in ei]

        return with_id

def get_all(env: MasonEnvironment) -> List[Config]:
    logger.debug(f"Reading configurations at {env.config_home}")

    configs: List[Config] = []
    for subdir, dirs, files in walk(env.config_home):
        for file in files:
            if '.yaml' in file:
                yaml_config_doc: dict = parse_yaml(env.config_home + file) or {}
                configs.append(Config(env, yaml_config_doc))

    return configs


def tabulate_configs(configs: List[Config], env: MasonEnvironment, log_level: str = "info") -> Optional[Config]:
    config_id = get_session_config(env)
    current_config = None

    extended_info: List[List[Union[str, dict, int]]] = []
    for i, c in enumerate(configs):
        if i == config_id:
            current = True
            current_config = c
        else:
            current = False

        for ei in c.extended_info(i, current):
            extended_info.append(ei)

    banner("Configurations", log_level)
    logger.at_level(log_level, tabulate(extended_info, headers=["Config ID", "Engine", "Client", "Configuration"]))
    logger.at_level(log_level)
    logger.at_level(log_level, "* = Current Configuration")

    return current_config

def set_current_config(env: MasonEnvironment, config_id: int, response: Response):
    configs = get_all(env)
    config: Optional[Config] = get(configs, config_id)
    if config:
        response.add_info(f"Setting current config to {config_id}")
        set_session_config(env, config_id)
        tabulate_configs(configs, env)
    else:
        response.add_error(f"Config {config_id} not found")

def get_current_config(env: MasonEnvironment, log_level: str = "info") -> Optional[Config]:
    config_id = get_session_config(env)
    configs = get_all(env)
    config: Optional[Config] = get(configs, config_id)
    if config:
        banner(f"Current Configuration {config_id}", log_level)
        tabulate_configs(configs, env, log_level)
        return config
    else:
        return None


