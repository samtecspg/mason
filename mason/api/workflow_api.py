from typing import Optional

from mason.workflows import workflows
from mason.configurations.configurations import get_current_config
from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger


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

        response = workflows.run(env, config, params, namespace, command, deploy, run_now, schedule_name)
    else:
        response = Response()
        response.add_error("Configuration not found")

    return response.formatted(), response.status_code
