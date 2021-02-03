
from typing import Optional, List
import urllib.parse

from mason.configurations.config import Config
from mason.operators.operator_response import OperatorResponse
from mason.parameters.input_parameters import InputParameters
from mason.configurations.configurations import get_current_config, get_config_by_id
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger
from mason.clients.response import Response
from mason.operators import operators
import gc

def get(namespace: str, command: str, environment: Optional[MasonEnvironment] = None, configuration: Optional[Config] = None, *args, **kwargs) :

    env: MasonEnvironment = environment or MasonEnvironment()
    config_id = kwargs.get("config_id")
    
    if config_id:
        config = get_config_by_id(env, config_id) 
    else:
        config = configuration or get_current_config(env)

    if config:
        param_list: List[str] = []
        for k,v in kwargs.items():
            unq = urllib.parse.unquote(v)
            param_list.append(f"{k}:{unq}")

        parameters = ",".join(param_list)
        params = InputParameters(parameters)

        logger.set_level(params.unsafe_get("log_level"))

        operator_response = operators.run(env, config, params, namespace, command)

    else:
        response = Response()
        response.add_error("Configuration not found")
        operator_response = OperatorResponse(response)

    uncollected = gc.collect()
    logger.debug(f"UNCOLLECTED ITEMS {uncollected}")
    return operator_response.formatted(), operator_response.status_code()
