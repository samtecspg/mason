from typing import Union

from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.parameters import Parameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.resources.invalid import InvalidResource, GenericInvalidResource
from mason.resources.malformed import MalformedResource
from mason.resources.resource import Resource
from mason.resources.valid import ValidResource
from mason.util.environment import MasonEnvironment
from mason.workflows.workflow import Workflow

def validate_resource(resource: Resource, config: Config, parameters: Parameters, env: MasonEnvironment) -> Union[ValidResource, InvalidResource]:
    if isinstance(resource, Operator) and isinstance(parameters, OperatorParameters):
        return resource.validate(config, parameters)
    elif isinstance(resource, Workflow) and isinstance(parameters, WorkflowParameters):
        return resource.validate(env, config, parameters)
    elif isinstance(resource, Config):
        return GenericInvalidResource(f"Validate undefined for config")
    elif isinstance(resource, MalformedResource):
        return GenericInvalidResource(f"{resource.get_message()}")
    else:
        return GenericInvalidResource(f"Validate undefined for {resource}")
        
