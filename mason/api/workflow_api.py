import gc
from typing import Optional

from mason.workflows import workflows
from mason.configurations.configurations import get_current_config, get_config_by_id
from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger


def get(namespace: str, command: str, environment: Optional[MasonEnvironment] = None, configuration: Optional[ValidConfig] = None, *args, **kwargs) :

    env: MasonEnvironment = environment or MasonEnvironment()
    config_id = kwargs.get("config_id")

    if config_id:
        config = get_config_by_id(env, config_id)
    else:
        config = configuration or get_current_config(env)

    if config:
        log_level: str = kwargs.get("log_level", "info")
        logger.set_level(log_level)

        deploy: bool = kwargs.get("deploy", False)
        run_now: bool = kwargs.get("run_now", False)
        parameters: dict = kwargs.get("parameters") or {}
        schedule_name: Optional[str] = kwargs.get("schedule_name")

        params = WorkflowParameters(parameter_dict=parameters)
        response = workflows.run(env, config, params, namespace, command, deploy, run_now, schedule_name)
    else:
        response = Response()
        response.add_error("Configuration not found")

    uncollected = gc.collect()
    logger.debug(f"UNCOLLECTED ITEMS {uncollected}")

    return response.formatted(), response.status_code
