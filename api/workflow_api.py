import urllib
from typing import Optional, List

from clients.response import Response
from configurations.configurations import get_current_config

from clients.response import Response
from configurations.valid_config import ValidConfig
from parameters import WorkflowParameters

from util.environment import MasonEnvironment
from util.logger import logger
import workflows as Workflows

def get(namespace: str, command: str, environment: Optional[MasonEnvironment] = None, configuration: Optional[ValidConfig] = None, *args, **kwargs) :

    env: MasonEnvironment = environment or MasonEnvironment()
    config: Optional[ValidConfig] = configuration or get_current_config(env)

    if config:
        parameters = kwargs["parameters"]
        log_level = kwargs["log_level"]
        deploy = kwargs["deploy"]
        run_now = kwargs["run_now"]
        schedule_name = kwargs["schedule_name"]

        params = WorkflowParameters(parameter_dict=parameters)
        logger.set_level(log_level)

        response = Workflows.run(env, config, params, namespace, command, deploy, run_now, schedule_name)
    else:
        response = Response()
        response.add_error("Configuration not found")

    return response.formatted(), response.status_code
