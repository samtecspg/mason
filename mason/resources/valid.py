from abc import abstractmethod

from mason.clients.response import Response
from mason.util.environment import MasonEnvironment

class ValidResource:

    @abstractmethod
    def run(self, env: MasonEnvironment, response: Response = Response()) -> Response:
        raise Exception("Run not implemented for resource")

    @abstractmethod
    def dry_run(self, env: MasonEnvironment, response: Response = Response()) -> Response:
        raise Exception("Dry run not implemented for resource")
