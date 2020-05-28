from typing import Optional, List
import urllib.parse

from mason.parameters.input_parameters import InputParameters
from mason.configurations.configurations import get_current_config
from mason.configurations.valid_config import ValidConfig
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger
from mason.clients.response import Response
from mason.operators import operators

def get(namespace: str, command: str, environment: Optional[MasonEnvironment] = None, configuration: Optional[ValidConfig] = None, *args, **kwargs) :

    env: MasonEnvironment = environment or MasonEnvironment()
    config: Optional[ValidConfig] = configuration or get_current_config(env)

    if config:
        param_list: List[str] = []
        for k,v in kwargs.items():
            unq = urllib.parse.unquote(v)
            param_list.append(f"{k}:{unq}")

        parameters = ",".join(param_list)
        params = InputParameters(parameters)

        logger.set_level(params.unsafe_get("log_level"))

        response = operators.run(env, config, params, namespace, command)
    else:
        response = Response()
        response.add_error("Configuration not found")

    return response.formatted(), response.status_code
