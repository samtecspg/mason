from configurations.config import Config
from configurations.invalid_config import InvalidConfig
from configurations.valid_config import ValidConfig
from util.printer import banner

from util.yaml import parse_yaml_invalid
from util.session import set_session_config, get_session_config

from util.logger import logger
from util.environment import MasonEnvironment
from typing import List, Union, Optional, Dict, Tuple
from os import walk
from tabulate import tabulate
from clients.response import Response

def get_all(env: MasonEnvironment, config_file: Optional[str] = None) -> Tuple[Dict[str, ValidConfig], List[InvalidConfig]]:
    dir = config_file or env.config_home
    logger.debug(f"Reading configurations at {dir}")

    configs: Dict[str, ValidConfig] = {}
    invalid: List[InvalidConfig] = []

    for subdir, dirs, files in walk(dir):
        for file in files:
            if '.yaml' in file:
                config = get_config(env, subdir + file)
                if isinstance(config, ValidConfig):
                    if not configs.get(config.id):
                        configs[config.id] = config
                    else:
                        invalid.append(InvalidConfig(config.config, f"Duplicate configuration id {config.id}.  Skipping..."))
                else:
                     invalid.append(config)

    return configs, invalid

def get_config(env: MasonEnvironment, file: str) -> Union[ValidConfig, InvalidConfig]:
    yaml_config_doc = parse_yaml_invalid(file)
    if isinstance(yaml_config_doc, dict):
        return Config(yaml_config_doc).validate(env, file)
    else:
        return InvalidConfig({}, yaml_config_doc)

def tabulate_configs(configs: Dict[str, ValidConfig], env: MasonEnvironment, log_level: str = "info") -> Optional[ValidConfig]:
    config_id = get_session_config(env)

    extended_info: List[List[Union[str, dict, int]]] = []
    current_config = None

    for id, c in configs.items():
        if config_id and id == config_id:
            current = True
            current_config = c
        else:
            current = False

        for ei in c.extended_info(id, current):
            extended_info.append(ei)

    banner("Configurations", log_level)
    logger.at_level(log_level, tabulate(extended_info, headers=["Config ID", "Engine", "Client", "Configuration"]))
    logger.at_level(log_level)
    logger.at_level(log_level, "* = Current Configuration")

    return current_config


def set_current_config(env: MasonEnvironment, config: ValidConfig, configs: Dict[str, ValidConfig], response: Response):
    response.add_info(f"Setting current config to {config.id}")
    set_session_config(env, config.id)
    tabulate_configs(configs, env)

def set_current_config_id(env: MasonEnvironment, config_id: str, response: Response):
    configs, invalid = get_all(env)
    config: Optional[ValidConfig] = configs.get(config_id)
    if config:
        set_current_config(env, config, configs, response)
    else:
        response.add_error(f"Config {config_id} not found")


def get_current_config(env: MasonEnvironment, log_level: str = "info") -> Optional[ValidConfig]:
    config_id = get_session_config(env)
    configs, invalid = get_all(env)
    if config_id:
        config: Optional[ValidConfig] = configs.get(config_id)
        if config:
            banner(f"Current Configuration {config_id}", log_level)
            tabulate_configs(configs, env, log_level)
            return config
        else:
            return None
    else:
        return None

def get_config_by_id(env: MasonEnvironment,  config_id: str) -> Optional[ValidConfig]:
    configs, invalid = get_all(env)
    return configs.get(config_id)

