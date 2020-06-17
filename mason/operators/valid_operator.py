from importlib import import_module
from typing import List, Optional, Union, Any

from mason.engines.execution.models.jobs import Job
from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.operator_response import OperatorResponse
from mason.operators.supported_engines import SupportedEngineSet
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger


class ValidOperator:

    def __init__(self, namespace: str, command: str, supported_configurations: List[SupportedEngineSet], description: str,  params: ValidatedParameters, config: ValidConfig, source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.parameters = params
        self.config = config
        self.supported_configurations = supported_configurations
        self.source_path = source_path

    def type_name(self):
        return str.capitalize(self.namespace) + str.capitalize(self.command)

    def display_name(self):
        return f"{self.namespace}:{self.command}"

    def run(self, env: MasonEnvironment, response: Response) -> OperatorResponse:
        try:
            mod = import_module(f'{env.operator_module}.{self.namespace}.{self.command}')
            operator_response: OperatorResponse = mod.run(env, self.config, self.parameters, response) #type: ignore
        except ModuleNotFoundError as e:
            response.add_error(f"Module Not Found: {e}")
            operator_response = OperatorResponse(response)

        return operator_response

    def to_dict(self):
        return {
            'cmd': self.namespace,
            'subcommand': self.command,
            'description': self.description,
            'parameters': self.parameters,
            'supported_configurations': list(map(lambda x: x.all, self.supported_configurations))
        }