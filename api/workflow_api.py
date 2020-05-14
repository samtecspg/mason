import urllib
from typing import Optional, List

from clients.response import Response
from configurations.configurations import get_current_config

from configurations.valid_config import ValidConfig
from parameters import InputParameters, WorkflowParameters

from util.environment import MasonEnvironment
from util.logger import logger
import workflows as Workflows

def get(namespace: str, command: str, environment: Optional[MasonEnvironment] = None, configuration: Optional[ValidConfig] = None, *args, **kwargs) :

    env: MasonEnvironment = environment or MasonEnvironment()
    config: Optional[ValidConfig] = configuration or get_current_config(env)

    if config:
        param_list: List[str] = []
        for k,v in kwargs.items():
            unq = urllib.parse.unquote(v)
            param_list.append(f"{k}:{unq}")

        parameters = ",".join(param_list)
        params = WorkflowParameters(parameters)

        logger.set_level(kwargs.get("log_level"))

        # def run(env: MasonEnvironment, config: ValidConfig, parameters: InputParameters, cmd: Optional[str] = None,
        #         subcmd: Optional[str] = None, deploy: bool = False, run: bool = False):

        response = Workflows.run(env, config, params, namespace, command)
    else:
        response = Response()
        response.add_error("Configuration not found")

    return response.formatted(), response.status_code
