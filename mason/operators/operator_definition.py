from abc import abstractmethod

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment


class OperatorDefinition:
    
    @abstractmethod
    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        raise NotImplementedError("Operator Run Definition no implemented")
