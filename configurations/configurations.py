from configurations.config import Config
from configurations.invalid_config import InvalidConfig
from configurations.valid_config import ValidConfig
from util.printer import banner

from util.yaml import parse_yaml
from util.session import set_session_config, get_session_config

from util.logger import logger
from util.list import get
from util.environment import MasonEnvironment
from typing import List, Union, Optional
from os import walk
from tabulate import tabulate
from clients.response import Response

def get_all(env: MasonEnvironment) -> List[ValidConfig]:
    logger.debug(f"Reading configurations at {env.config_home}")

    configs: List[ValidConfig] = []
    for subdir, dirs, files in walk(env.config_home):
        for file in files:
            if '.yaml' in file:
                config = get_config(env, file)
                if isinstance(config, ValidConfig):
                    configs.append(config)
                else:
                    logger.error(f"Invalid configuration at {file}. Reason: {config.reason}")

    return configs

def get_config(env: MasonEnvironment, file: str) -> Union[ValidConfig, InvalidConfig]:
    yaml_config_doc: dict = parse_yaml(env.config_home + file) or {}
    return Config(yaml_config_doc).validate(env)

def tabulate_configs(configs: List[ValidConfig], env: MasonEnvironment, log_level: str = "info") -> Optional[ValidConfig]:
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
    config: Optional[ValidConfig] = get(configs, config_id)
    if config:
        response.add_info(f"Setting current config to {config_id}")
        set_session_config(env, config_id)
        tabulate_configs(configs, env)
    else:
        response.add_error(f"Config {config_id} not found")

def get_current_config(env: MasonEnvironment, log_level: str = "info") -> Optional[ValidConfig]:
    config_id = get_session_config(env)
    configs = get_all(env)
    config: Optional[ValidConfig] = get(configs, config_id)
    if config:
        banner(f"Current Configuration {config_id}", log_level)
        tabulate_configs(configs, env, log_level)
        return config
    else:
        return None


