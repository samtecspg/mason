from collections import OrderedDict
from typing import List, Union, Optional, Dict, Tuple
from tabulate import tabulate
from typistry.protos.invalid_object import InvalidObject

from mason.configurations.invalid_config import InvalidConfig
from mason.configurations.config import Config
from mason.util.environment import MasonEnvironment
from mason.util.list import sequence
from mason.util.logger import logger
from mason.clients.response import Response
from mason.util.printer import banner
from mason.util.session import set_session_config, get_session_config
#  from mason.validations.validate import validate_configs

def get_all(env: MasonEnvironment) -> Tuple[Dict[str, Config], List[InvalidConfig]]:
    logger.debug(f"Reading configurations at {env.state_store.config_home}")
    return {}, []

    #  valid_configs, invalid = sequence(validate_configs(env), Config, InvalidObject)
    #  invalid_configs = list(map(lambda i: InvalidConfig(i.reference, i.message),invalid))

    #  configs = {}
    #  for config in valid_configs:
        #  configs[config.id] = config

    #  configs_ordered = OrderedDict(sorted(configs.items()))
    #  return configs_ordered, invalid_configs

def tabulate_configs(configs: Dict[str, Config], env: MasonEnvironment, log_level: str = "info") -> Optional[Config]:
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


def set_current_config(env: MasonEnvironment, config: Config, configs: Dict[str, Config], response: Response):
    response.add_info(f"Setting current config to {config.id}")
    set_session_config(env, config.id)
    tabulate_configs(configs, env)

def set_current_config_id(env: MasonEnvironment, config_id: str, response: Response):
    configs, invalid = get_all(env)
    config: Optional[Config] = configs.get(config_id)
    if config:
        set_current_config(env, config, configs, response)
    else:
        response.add_error(f"Config {config_id} not found")


def get_current_config(env: MasonEnvironment, log_level: str = "info") -> Optional[Config]:
    config_id = get_session_config(env)
    configs, invalid = get_all(env)
    if config_id:
        config: Optional[Config] = configs.get(config_id)
        if config:
            banner(f"Current Configuration {config_id}", log_level)
            tabulate_configs(configs, env, log_level)
            return config
        else:
            return None
    else:
        return None

def get_config_by_id(env: MasonEnvironment,  config_id: str) -> Optional[Config]:
    configs, invalid = get_all(env)
    return configs.get(config_id)


