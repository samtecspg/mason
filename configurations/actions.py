from util.environment import MasonEnvironment
from typing import Optional
from clients.response import Response
from util.logger import logger
from os import path
from configurations.configurations import set_current_config, tabulate_configs, get_all, set_current_config_id
import shutil

def run_configuration_actions(env: MasonEnvironment, config_file: Optional[str]=None, set_current: Optional[str]=None,  log_level: Optional[str]=None) -> Response:
    response = Response()
    logger.set_level(log_level)

    if config_file:
        valid, invalid = get_all(env, config_file)

        i = 0
        for id, c in valid.items():
            if i == 0:
                set_current_config(env, c, valid, response)
            i += 1
            sp = c.source_path
            if sp:
                response.add_info(f"Valid Configuration. Saving config {c.id} at {c.source_path} to {env.config_home}")
                shutil.copyfile(sp, env.config_home + path.basename(sp))
            else:
                response.add_error("Config source path not found.   Run get_all with config file specified")

        for inv in invalid:
            response.add_error(f"Invalid Configuration: {inv.reason}")

    elif set_current:
        set_current_config_id(env, str(set_current), response)
    else:
        if path.exists(env.config_home):
            all_configs, invalid = get_all(env)
            current_config = tabulate_configs(all_configs, env)
            response.add_current_config(current_config)
            for id, config in all_configs.items():
                response.add_config(id, config.engines)
        else:
            logger.error()
            logger.error("Configuration not found.")
            logger.error("First pass configuration:  \"mason config <config_file_path>\"")

    return response

