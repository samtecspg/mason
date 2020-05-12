from clients.response import Response
from util.environment import MasonEnvironment


class InvalidOperator:

    def __init__(self, reason: str):
        self.reason = reason

    def run(self, env: MasonEnvironment, response: Response) -> Response:
        response.add_error(f"Invalid Operator.  Reason:  {self.reason}")
        response.set_status(400)
        return response
