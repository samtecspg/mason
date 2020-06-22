from importlib import import_module
from typing import List, Optional, Union

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.invalid_operator import InvalidOperator
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.operators.supported_engines import SupportedEngineSet
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment


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

    def module(self, env: MasonEnvironment) -> Union[OperatorDefinition, InvalidOperator]:
        try:
            mod = import_module(env.operator_module + f".{self.namespace}.{self.command}")
            try:
                classname = f"{self.namespace.capitalize()}{self.command.capitalize()}"
                operator_definition = getattr(mod, classname)()
                if isinstance(operator_definition, OperatorDefinition):
                    return operator_definition
                else:
                    return InvalidOperator("Invalid Operator definition.  See operators/operator_definition.py")
            except AttributeError as e:
                return InvalidOperator(f"Operator has no attribute {classname}")
        except ModuleNotFoundError as e:
            return InvalidOperator(f"Module not found: {env.operator_module}")

    def run(self, env: MasonEnvironment, response: Response) -> OperatorResponse:
        try:
            module = self.module(env)
            if isinstance(module, OperatorDefinition):
                operator_response: OperatorResponse = module.run(env, self.config, self.parameters, response) 
            else:
                operator_response = module.run(env, response)
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
