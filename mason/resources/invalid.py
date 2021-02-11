from abc import abstractmethod

from mason.clients.response import Response
from mason.util.environment import MasonEnvironment

class InvalidResource:

    @abstractmethod
    def __init__(self, reason: str):
        self.reason = reason

    def run(self, env: MasonEnvironment, response: Response = Response()) -> Response:
        response.add_error("Invalid Resource: " + self.reason)
        response.set_status(400)
        return response

    def dry_run(self, env: MasonEnvironment, response: Response = Response()) -> Response:
        response.add_error("Invalid Resource: " + self.reason)
        response.set_status(400)
        return response
        
class GenericInvalidResource(InvalidResource):
    
    def __init__(self, reason: str):
        super().__init__(reason)
