from abc import abstractmethod
from typing import Union

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class OperatorDefinition:

    @abstractmethod
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        raise NotImplementedError("Operator run Definition not implemented")

    @abstractmethod
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> Union[ExecutedJob, InvalidJob]:
        raise NotImplementedError("Operator async_run Definition not implemented")
