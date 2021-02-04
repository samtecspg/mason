from typing import Optional, Dict

from mason.configurations.config import Config
from mason.util.environment import MasonEnvironment
from mason.clients.response import Response
from mason.util.session import set_session_config

def set_current_config(env: MasonEnvironment, config: Config, configs: Dict[str, Config], response: Response):
    response.add_info(f"Setting current config to {config.id}")
    set_session_config(env, config.id)
    # tabulate_configs(configs, env)

def set_current_config_id(env: MasonEnvironment, config_id: str, response: Response):
    pass
    # configs, invalid = get_all(env)
    # config: Optional[Config] = configs.get(config_id)
    # if config:
    #     set_current_config(env, config, configs, response)
    # else:
    #     response.add_error(f"Config {config_id} not found")


def get_current_config(env: MasonEnvironment, log_level: str = "info") -> Optional[Config]:
    pass
    #

def get_config_by_id(env: MasonEnvironment) -> Optional[Config]:
    pass
    # configs, invalid = get_all(env)
    # return configs.get(config_id)


