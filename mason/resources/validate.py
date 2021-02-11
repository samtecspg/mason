from typing import Union

from mason.configurations.config import Config
from mason.operators.invalid_operator import InvalidOperator
from mason.operators.operator import Operator
from mason.operators.valid_operator import ValidOperator
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.parameters import Parameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.resources.invalid import InvalidResource, GenericInvalidResource
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
        
def validate_operator(operator: Operator, config: Config, parameters: OperatorParameters) -> Union[ValidOperator, InvalidOperator]:
    return operator.validate(config, parameters)
    