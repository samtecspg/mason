from util.environment import MasonEnvironment
from typing import Optional
from clients.response import Response
from util.logger import logger
from os import path, walk
from configurations import set_current_config, tabulate_configs, get_all
from util.yaml import parse_yaml
import shutil
from configurations import Config

def run_configuration_actions(env: MasonEnvironment, config_file: Optional[str]=None, set_current: Optional[str]=None,  log_level: Optional[str]=None) -> Response:
    response = Response()
    logger.set_level(log_level)

    if config_file:
        configs = []
        if path.isdir(config_file):
            for r, d, f in walk(config_file):
                for file in f:
                    if '.yaml' in file:
                        file_path = path.join(r, file)
                        configs.append(file_path)
        elif path.isfile(config_file):
            if '.yaml' in config_file:
                configs.append(config_file)
        else:
            message = "Invalid configuration file must specify yaml file or directory of yaml files"
            response.add_error(message)
            logger.error(message)

        logger.info()
        for c in configs:
            # TODO: Interactive configuration
            parsed = parse_yaml(c)
            config = Config(env, parsed)

            if config.valid:
                response.add_info(f"Valid Configuration. Saving config {c} to {env.config_home}")
                shutil.copyfile(c, env.config_home + path.basename(c))
        logger.info()
        if len(configs) > 0:
            set_current_config(env, 0, response)
    elif set_current:
        set_current_config(env, int(set_current), response)
    else:
        if path.exists(env.config_home):
            all_configs = get_all(env)
            current_config = tabulate_configs(all_configs, env)
            response.add_current_config(current_config)
            for i, config in enumerate(all_configs):
                response.add_config(i, config.engines)
        else:
            logger.error()
            logger.error("Configuration not found.")
            logger.error("First pass configuration:  \"mason config <config_file_path>\"")

    return response

