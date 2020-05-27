from typing import Optional

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
        log_level: str = kwargs.get("log_level", "info")
        deploy: bool = kwargs.get("deploy", False)
        run_now: bool = kwargs.get("run_now", False)
        parameters: dict = kwargs.get("parameters") or {}


        schedule_name: Optional[str] = kwargs.get("schedule_name")

        logger.set_level(log_level)

        params = WorkflowParameters(parameter_dict=parameters)

        response = Workflows.run(env, config, params, namespace, command, deploy, run_now, schedule_name)
    else:
        response = Response()
        response.add_error("Configuration not found")

    return response.formatted(), response.status_code
