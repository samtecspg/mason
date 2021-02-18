from typing import Optional, Union
from mason.clients.response import Response
from mason.configurations.config import Config
from mason.parameters.parameters import Parameters
from mason.resources.resource import Resource
from mason.resources.malformed import MalformedResource
from mason.resources.validate import validate_resource
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger
from mason.resources import base

def run(resource_type: str, namespace: str, command: str, parameters: Optional[str] = None, param_file: Optional[str] = None, config_id: Optional[str] = None, log_level: Optional[str] = None, env: Optional[MasonEnvironment] = None, dry_run: bool = False):
    response: Response = Response()
    environment: MasonEnvironment = env or MasonEnvironment().initialize()
    logger.set_level(log_level)
    res = base.Resources(environment)

    resource: Union[Resource, MalformedResource] = res.get_resource(resource_type, namespace, command)
    config: Union[Config, MalformedResource] = res.get_best_config(config_id)
    params: Union[Parameters, MalformedResource] = res.get_parameters(resource_type, parameters, param_file)

    if isinstance(resource, Resource) and isinstance(config, Config) and isinstance(params, Parameters):
        if dry_run:
            response = validate_resource(resource, config, params, environment).dry_run(environment, response).to_response(response)
        else:
            response = validate_resource(resource, config, params, environment).run(environment, response).to_response(response)
    else:
        if isinstance(resource, MalformedResource):
            response.add_error(f"Malformed Resource: {resource.get_message()}")
        elif isinstance(config, MalformedResource):
            response.add_error(f"Bad Config: {config.get_message()}")
        elif isinstance(params, MalformedResource):
            response.add_error(f"Bad Parameters: {params.get_message()}")

    return response.with_status()
