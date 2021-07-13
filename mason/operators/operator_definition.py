from abc import abstractmethod
from typing import Tuple, Union

from mason_dask.jobs.executed import InvalidJob

from mason.engines.execution.models.jobs import Job

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator_response import OperatorResponse, DelayedOperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class OperatorDefinition:
    
    @abstractmethod
    def job(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> Tuple[Union[Job, InvalidJob], Response]:
        raise NotImplementedError("Operator run Definition not implemented")

    @abstractmethod
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        raise NotImplementedError("Operator run Definition not implemented")

    @abstractmethod
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        raise NotImplementedError("Operator async_run Definition not implemented")
