from abc import abstractmethod

from mason.clients.response import Response
from mason.operators.operator_response import OperatorResponse
from mason.util.environment import MasonEnvironment

class ValidResource:

    @abstractmethod
    def run(self, env: MasonEnvironment, response: Response = Response()) -> OperatorResponse:
        raise Exception("Run not implemented for resource")

    @abstractmethod
    def dry_run(self, env: MasonEnvironment, response: Response = Response()) -> OperatorResponse:
        raise Exception("Dry run not implemented for resource")
