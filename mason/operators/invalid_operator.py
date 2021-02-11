from mason.clients.response import Response
from mason.resources.invalid import InvalidResource
from mason.util.environment import MasonEnvironment

class InvalidOperator(InvalidResource):

    def __init__(self, reason: str):
        super().__init__(reason)

    def run(self, env: MasonEnvironment, response: Response = Response()) -> Response:
        response.add_error(f"Invalid Operator.  Reason:  {self.reason}")
        response.set_status(400)
        return response
