from typing import Dict, Optional, List, Union

from returns.result import safe

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.configurations.invalid_config import InvalidConfig
from mason.operators.invalid_operator import InvalidOperator
from mason.operators.supported_engines import from_array, SupportedEngineSet
from mason.operators.valid_operator import ValidOperator
from mason.parameters.input_parameters import InputParameters
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.validated_parameters import ValidatedParameters
from mason.resources.resource import Resource
from mason.resources.saveable import Saveable
from mason.state.base import MasonStateStore
from mason.util.exception import message


class Operator(Saveable, Resource):

    def __init__(self, namespace: str, command: str, parameters: dict, supported_configurations: List[Dict[str, str]], description: str = "", source: Optional[str] = None):
        super().__init__(source)
        self.namespace = namespace
        self.command = command
        self.description = description
        self.parameters = InputParameters(**parameters) 
        self.supported_configurations: List[SupportedEngineSet] = from_array(supported_configurations)
        self.source_path = source

    def save(self, state_store: MasonStateStore, overwrite: bool = False, response: Response = Response()) -> Response:
        try:
            state_store.cp_source(self.source_path, "operator", self.namespace, self.command, overwrite)
        except Exception as e:
            response.add_error(f"Error copying source: {message(e)}")
            
        return response

    def validate(self, config: Config, parameters: OperatorParameters) -> Union[ValidOperator, InvalidOperator]:
        validated_params: ValidatedParameters = self.parameters.validate(parameters)
        validated_config = self.validate_config(config)

        a: Union[ValidOperator, InvalidOperator]
        if validated_params.has_invalid():
            return InvalidOperator(f"Invalid parameters.  {validated_params.messages()}")
        elif isinstance(validated_config, InvalidConfig):
            return InvalidOperator(f"Invalid config: {validated_config.reason}")
        else:
            return ValidOperator(self.namespace, self.command,self.supported_configurations,self.description, validated_params, validated_config)

    def validate_config(self, config: Config) -> Union[Config, InvalidConfig]:
        test = False
        message = ""
        for ses in self.supported_configurations:
            test, message = ses.validate_coverage(config)
            if test:
                break

        if test:
            return config
        else:
            return InvalidConfig(f"Configuration {config.id} not supported by configured engines for operator {self.namespace}:{self.command}.  {message}. Check operator.yaml for supported engine configurations.", config)

    def to_dict(self):
        return {
            'namespace': self.namespace,
            'command': self.command,
            'description': self.description,
            'parameters': self.parameters.to_dict(),
            'supported_configurations': list(map(lambda s: s.all, self.supported_configurations))
        }


def emptyOperator():
    Operator("", "", {}, [])
