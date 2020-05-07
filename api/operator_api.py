from parameters import Parameters
from configurations import Config, get_current_config
from util.environment import MasonEnvironment
from typing import Optional
import operators as Operators
from typing import List
import urllib.parse
from util.logger import logger
from clients.response import Response

def get(namespace: str, command: str, environment: Optional[MasonEnvironment] = None, configuration: Optional[Config] = None, *args, **kwargs) :

    env: MasonEnvironment = environment or MasonEnvironment()
    config: Optional[Config] = configuration or get_current_config(env)

    if config:
        param_list: List[str] = []
        for k,v in kwargs.items():
            unq = urllib.parse.unquote(v)
            param_list.append(f"{k}:{unq}")

        parameters = ",".join(param_list)
        params = Parameters(parameters)

        logger.set_level(params.unsafe_get("log_level"))
        response = Operators.run(env, config, params, namespace, command)
    else:
        response = Response()
        response.add_error("Configuration not found")

    return response.formatted(), response.status_code
